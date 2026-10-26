"""
Inspect Condor job outputs, identify missing jobs, analyze recent XRootD
failures, and optionally regenerate input filesets with problematic sites
blacklisted before resubmitting only the missing jobs.
"""

import re
import sys
import yaml
import json
import select
import argparse
import subprocess
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from analysis.utils import make_output_directory
from analysis.filesets.xrootd_sites import xroot_to_site
from analysis.filesets.utils import divide_list, get_nano_version

ENDPOINT_PATTERN = r"root://[a-zA-Z0-9._-]+(?::\d+)?"

EXECUTOR_ERROR_PATTERNS = {
    "Authentication": [
        r"Auth failed",
        r"No protocols left to try",
    ],
    "Connection": [
        r"\[FATAL\]\s*Connection error",
        r"Connection reset",
        r"Connection refused",
        r"Network is unreachable",
        r"XRootD error:.*Connection error",
        r"\bConnectionError\b",
    ],
    "TLS": [
        r"\bTLS error\b",
        r"SSL error",
    ],
}

FILE_ERROR_PATTERNS = {
    "File access / permission": [
        r"\[3010\]\s*Unable to open file",
        r"Unable to open file",
        r"Failed to open file",
        r"Error opening file",
        r"Operation not permitted",
        r"Permission denied",
    ],
    "File not found": [
        r"File not found",
        r"No such file or directory",
        r"\bFileNotFoundError\b",
    ],
    "Read error": [
        r"Read error",
        r"Input/output error",
        r"\bIOError\b",
    ],
    "Timeout": [
        r"Operation expired",
        r"timed?\s*out",
        r"\btimeout\b",
        r"\bTimeoutError\b",
    ],
    "Socket / network": [
        r"Socket error",
        r"XrdCl::",
    ],
    "Other I/O error": [
        r"\bOSError\b",
        r"XRootD error",
    ],
}

ERROR_PATTERNS = {
    **EXECUTOR_ERROR_PATTERNS,
    **FILE_ERROR_PATTERNS,
}


def parse_args():
    """Parse command-line arguments."""
    workflows_dir = Path.cwd() / "analysis" / "workflows"

    parser = argparse.ArgumentParser(
        description=(
            "Inspect Condor job outputs, diagnose XRootD failures, "
            "regenerate filesets, and resubmit missing jobs."
        )
    )

    parser.add_argument(
        "-w",
        "--workflow",
        required=True,
        choices=[path.stem for path in workflows_dir.glob("*.yaml")],
        help="Workflow name corresponding to a YAML file in analysis/workflows",
    )
    parser.add_argument(
        "-y",
        "--year",
        required=True,
        choices=[
            "2016preVFP",
            "2016postVFP",
            "2017",
            "2018",
            "2022preEE",
            "2022postEE",
            "2023preBPix",
            "2023postBPix",
            "2024",
        ],
        help="Dataset year",
    )
    parser.add_argument(
        "--output_format",
        default="coffea",
        choices=["coffea", "parquet"],
        help="Output format of the produced histograms",
    )
    parser.add_argument(
        "--hours_ago",
        type=int,
        default=8,
        help="Only consider .err files modified within the last N hours",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing job outputs, logs, and filesets before rerunning",
    )
    parser.add_argument(
        "--single_fetch",
        action="store_true",
        help=(
            "Run fetch.py once by merging all failing sites and datasets "
            "into a single group"
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed XRootD error diagnostics",
    )
    parser.add_argument(
        "--executor",
        choices=["iterative", "futures"],
        default="futures",
        help="Executor to use when updating dataset arguments",
    )
    parser.add_argument(
        "-e",
        "--update_executor",
        action="store_true",
        help="Update the executor in arguments.json for affected datasets",
    )

    return parser.parse_args()


def timed_input(prompt, timeout=10, default="y"):
    """Read user input with a timeout and return the default if necessary."""
    print(
        f"{prompt} [{default.upper()}/n] (auto-selected in {timeout}s): ",
        end="",
        flush=True,
    )

    ready, _, _ = select.select([sys.stdin], [], [], timeout)

    if ready:
        response = sys.stdin.readline().strip()
        return response.lower() if response else default.lower()

    print(f"\n[Timeout reached] Using default option: '{default.upper()}'")
    return default.lower()


def modify_site_list(year: str, site: str, status: str) -> None:
    """Move a site to either the white or black list in the year's YAML file."""
    sites_file = Path.cwd() / "analysis" / "filesets" / f"{year}_sites.yaml"
    data = yaml.safe_load(sites_file.read_text())

    if status == "white":
        if site not in data["white"]:
            data["white"].append(site)
        if site in data["black"]:
            data["black"].remove(site)
    else:
        if site not in data["black"]:
            data["black"].append(site)
        if site in data["white"]:
            data["white"].remove(site)

    sites_file.write_text(yaml.dump(data, default_flow_style=False))


def identify_error_types(content: str) -> set[str]:
    """Identify recognized XRootD, connection, and file-access errors."""
    return {
        error_type
        for error_type, patterns in ERROR_PATTERNS.items()
        if any(re.search(pattern, content, flags=re.IGNORECASE) for pattern in patterns)
    }


def get_missing_job_partitions(job_dir: Path, dataset: str) -> set[str]:
    """Read missing partition identifiers from missing.txt, if available."""
    missing_path = Path(job_dir) / dataset / "missing.txt"

    if not missing_path.is_file():
        return set()

    return {
        line.strip() for line in missing_path.read_text().splitlines() if line.strip()
    }


def get_partition_endpoints(
    partitions_data: dict,
    missing_partitions: set[str],
    endpoint_pattern: re.Pattern,
) -> set[str]:
    """Extract XRootD endpoints from the specified missing partitions."""
    endpoints = set()

    for partition_id in missing_partitions:
        partition_data = partitions_data.get(partition_id, {})

        for sample_data in partition_data.values():
            for url in sample_data.get("files", {}):
                match = endpoint_pattern.match(url)
                if match:
                    endpoints.add(match.group(0))

    return endpoints


def analyze_xrootd_error_logs(
    error_files,
    job_dir: Path,
    dataset: str,
    missing_partitions=None,
    verbose: bool = False,
) -> dict:
    """
    Identify error types and XRootD endpoints for one dataset.

    Endpoints are extracted from recent error logs first. If no endpoint
    is found in the logs, the function falls back to the files belonging
    to the currently missing partitions.
    """
    dataset_dir = Path(job_dir) / dataset
    partitions_path = dataset_dir / "partitions.json"

    if missing_partitions is None:
        missing_partitions = get_missing_job_partitions(job_dir, dataset)

    missing_partitions = {str(partition) for partition in missing_partitions}

    if partitions_path.is_file():
        partitions_data = json.loads(partitions_path.read_text())
    else:
        partitions_data = {}
        if verbose:
            print(f"[{dataset}] Missing partitions.json: {partitions_path}")

    endpoint_pattern = re.compile(
        ENDPOINT_PATTERN,
        flags=re.IGNORECASE,
    )

    endpoints = set()
    error_types = set()

    for error_file in error_files:
        error_file = Path(error_file)

        try:
            content = error_file.read_text(errors="replace")
        except OSError as exc:
            if verbose:
                print(f"[{dataset}] Could not read {error_file}: {exc}")
            continue

        endpoints.update(endpoint_pattern.findall(content))
        error_types.update(identify_error_types(content))

    if not endpoints:
        endpoints = get_partition_endpoints(
            partitions_data,
            missing_partitions,
            endpoint_pattern,
        )

        if verbose:
            print(f"[{dataset}] Fallback endpoints: {sorted(endpoints)}")

    if verbose:
        print(f"[{dataset}] Error types: {sorted(error_types)}")
        print(f"[{dataset}] Endpoints: {sorted(endpoints)}")
        print(f"[{dataset}] Missing partitions: {sorted(missing_partitions)}")

    return {
        "endpoints": endpoints,
        "error_types": error_types,
        "partitions": missing_partitions,
    }


def get_jobs_info(
    job_dir: Path,
    output_dir: Path,
    log_dir: Path,
    output_format: str,
    hours_ago: int = 3,
):
    """
    Collect expected jobs, completed jobs, and recent error logs per dataset.

    Returns
    -------
    jobnum : dict
        Dataset -> list of expected job numbers.
    jobnum_done : dict
        Dataset -> list of job numbers with output files.
    error_files : dict
        Dataset -> list of recent .err file paths.
    """
    jobnum = {}
    jobnum_done = {}
    error_files = {}

    cutoff_time = datetime.now() - timedelta(hours=hours_ago)

    for dataset_dir in job_dir.iterdir():
        if not dataset_dir.is_dir():
            continue

        dataset = dataset_dir.name
        jobnum_path = dataset_dir / "jobnum.txt"

        if not jobnum_path.is_file():
            raise FileNotFoundError(
                f"Missing jobnum.txt for dataset '{dataset}'. "
                f"Expected at: {jobnum_path}"
            )

        jobnum[dataset] = jobnum_path.read_text().splitlines()

        output_files = list((output_dir / dataset).glob(f"*.{output_format}"))
        jobnum_done[dataset] = [
            path.stem.replace(f"{dataset}_", "") for path in output_files
        ]

        dataset_log_dir = log_dir / dataset
        if dataset_log_dir.is_dir():
            recent_errors = [
                path
                for path in dataset_log_dir.glob("*.err")
                if datetime.fromtimestamp(path.stat().st_mtime) > cutoff_time
            ]

            if recent_errors:
                error_files[dataset] = recent_errors

    return jobnum, jobnum_done, error_files


def print_job_status(jobnum, jobnum_done):
    """
    Print job completion totals and return missing jobs per dataset.

    Returns
    -------
    jobnum_missing : dict
        Dataset -> set of missing job numbers.
    datasets_with_missing : list
        Datasets with at least one missing job.
    """
    jobnum_missing = {
        dataset: set(expected) - set(jobnum_done.get(dataset, []))
        for dataset, expected in jobnum.items()
    }

    n_expected = sum(len(jobs) for jobs in jobnum.values())
    n_done = sum(len(jobs) for jobs in jobnum_done.values())
    n_missing = sum(len(jobs) for jobs in jobnum_missing.values())

    print("-" * 60)
    print("JOB STATUS SUMMARY")
    print("-" * 60)
    print(f"Total jobs expected : {n_expected}")
    print(f"Total jobs finished : {n_done}")
    print(f"Total jobs missing  : {n_missing}")

    datasets_with_missing = [
        dataset for dataset, missing in jobnum_missing.items() if missing
    ]

    if n_missing:
        print("-" * 60)
        print(
            f"Datasets with missing jobs "
            f"({len(datasets_with_missing)}/{len(jobnum)}):"
        )
        print("-" * 60)

    return jobnum_missing, datasets_with_missing


def update_executor_for_dataset(
    job_dir: Path,
    dataset: str,
    executor: str = "iterative",
) -> None:
    """Update the executor in a dataset's arguments.json."""
    args_json = job_dir / dataset / "arguments.json"

    if not args_json.is_file():
        print(f"[{dataset}] Missing arguments.json — cannot update executor")
        return

    arguments = json.loads(args_json.read_text())

    if arguments.get("executor") == executor:
        print(f"[{dataset}] Executor is already '{executor}'")
        return

    arguments["executor"] = executor
    args_json.write_text(json.dumps(arguments, indent=4) + "\n")

    print(f"[{dataset}] Updated executor to '{executor}'")


def map_endpoints_to_sites(endpoints: set[str]) -> set[str]:
    """Map known XRootD endpoints to CMS site names."""
    return {
        xroot_to_site[endpoint] for endpoint in endpoints if endpoint in xroot_to_site
    }


def analyze_xrootd_errors_by_dataset(
    error_files,
    job_dir,
    dataset,
    missing_partitions,
    verbose=False,
):
    """
    Diagnose one dataset, identifying endpoints and CMS sites.

    Endpoints from error logs are preferred. If they do not identify a CMS
    site, endpoints from the currently missing partitions are checked.
    """
    errors = analyze_xrootd_error_logs(
        error_files=error_files,
        job_dir=job_dir,
        dataset=dataset,
        missing_partitions=missing_partitions,
        verbose=verbose,
    )

    errors["sites"] = map_endpoints_to_sites(errors["endpoints"])

    # If log endpoints are unmapped, retry using missing-partition endpoints.
    if not errors["sites"]:
        fallback_errors = analyze_xrootd_error_logs(
            error_files=[],
            job_dir=job_dir,
            dataset=dataset,
            missing_partitions=missing_partitions,
            verbose=verbose,
        )

        fallback_endpoints = fallback_errors["endpoints"]
        fallback_sites = map_endpoints_to_sites(fallback_endpoints)

        if fallback_sites:
            errors["endpoints"] = fallback_endpoints
            errors["sites"] = fallback_sites

        if verbose:
            print(f"[{dataset}] Fallback sites: {sorted(fallback_sites)}")

    for endpoint in sorted(errors["endpoints"]):
        if endpoint not in xroot_to_site:
            print(
                f"[{dataset}] Could not map XRootD endpoint "
                f"'{endpoint}' to a site name"
            )

    if not errors["sites"]:
        print(
            f"[{dataset}] No CMS site could be identified "
            "from the error logs or missing partitions."
        )

    return errors


def group_datasets_by_sites(dataset_sites):
    """Group datasets that share exactly the same set of failing sites."""
    groups = defaultdict(list)

    for dataset, sites in dataset_sites.items():
        groups[frozenset(sites)].append(dataset)

    return groups


def regenerate_partitions(
    dataset: str,
    current_fileset: dict,
    job_dir: Path,
) -> None:
    """Regenerate partitions.json for a dataset using its current arguments."""
    args_json = job_dir / dataset / "arguments.json"

    if not args_json.is_file():
        print(
            f"[{dataset}] Missing {args_json} — cannot repartition",
            flush=True,
        )
        return

    fileset_files = current_fileset["files"]
    root_files = list(fileset_files.items())
    nfiles = json.loads(args_json.read_text())["nfiles"]
    fileset_chunks = divide_list(root_files, nfiles)

    partition_dataset = {}

    for index, chunk in enumerate(fileset_chunks, start=1):
        dataset_key = f"{dataset}_{index}" if len(fileset_chunks) > 1 else dataset

        partition_dataset[index] = {
            dataset_key: {
                "files": dict(chunk),
                "metadata": current_fileset.get("metadata", {}),
            }
        }

    partition_file = job_dir / dataset / "partitions.json"
    partition_file.write_text(json.dumps(partition_dataset, indent=4))


def update_input_filesets_for_group(
    sites_to_blacklist,
    year,
    fileset_dir: Path,
    job_dir: Path,
    datasets,
):
    """
    Regenerate filesets and partitions for datasets sharing failing sites.

    The site lists are reset before applying each group's blacklist to avoid
    carrying blacklist entries over from a previous group.
    """
    print("-" * 60)
    print("REGENERATING FILESETS FOR DATASET GROUP")
    print("-" * 60)
    print(f"Datasets           : {datasets}")
    print(f"Blacklisted sites  : {sorted(sites_to_blacklist)}")

    # Reset site lists so that each group uses only its own blacklist.
    for site in set(xroot_to_site.values()):
        modify_site_list(year, site, "white")

    for site in sites_to_blacklist:
        modify_site_list(year, site, "black")

    print("Running fetch.py for this group")

    subprocess.run(
        [
            "python3",
            "fetch.py",
            "--year",
            year,
            "--samples",
            " ".join(datasets),
        ],
        check=False,
    )

    nano_version = get_nano_version(year)
    fileset_path = fileset_dir / f"fileset_{year}_nanov{nano_version}_lxplus.json"

    if not fileset_path.is_file():
        raise FileNotFoundError(f"Regenerated fileset not found: {fileset_path}")

    fileset = json.loads(fileset_path.read_text())

    short_name_map = {
        sample_data["metadata"]["short_name"]: sample
        for sample, sample_data in fileset.items()
    }

    for dataset in datasets:
        sample_name = short_name_map.get(dataset)

        if sample_name is None:
            print(
                f"[{dataset}] Not found in fileset JSON — skipping",
                flush=True,
            )
            continue

        regenerate_partitions(
            dataset,
            fileset[sample_name],
            job_dir,
        )

    print("Fileset regeneration for this group completed.\n", flush=True)


def sort_job_numbers(job_numbers):
    """Sort numeric job identifiers numerically and other identifiers afterward."""
    return sorted(
        job_numbers,
        key=lambda value: (
            not str(value).isdigit(),
            int(value) if str(value).isdigit() else str(value),
        ),
    )


def resubmit_jobs(
    job_dir: Path,
    jobnum_missing,
    datasets_with_missing_jobs,
    workflow: str,
    year: str,
):
    """
    Prepare and resubmit only jobs without output files.

    The original submission file is backed up once as *_all.sub.
    """
    print("-" * 60)
    print("RESUBMITTING MISSING JOBS")
    print("-" * 60)

    submit_files = []

    for dataset in datasets_with_missing_jobs:
        missing_jobs = sort_job_numbers(jobnum_missing[dataset])
        dataset_dir = job_dir / dataset

        missing_file = dataset_dir / "missing.txt"
        missing_file.write_text("".join(f"{job}\n" for job in missing_jobs))

        condor_file = dataset_dir / f"{workflow}_{dataset}.sub"
        condor_backup = condor_file.with_name(f"{condor_file.stem}_all.sub")

        if not condor_backup.exists():
            subprocess.run(
                ["cp", str(condor_file), str(condor_backup)],
                check=True,
            )

        submit_text = condor_file.read_text().replace(
            "jobnum.txt",
            "missing.txt",
        )
        condor_file.write_text(submit_text)

        submit_files.append(condor_file)

    for submit_file in submit_files:
        print(f"Submitting {submit_file}")
        subprocess.run(
            ["condor_submit", str(submit_file)],
            check=True,
        )

    print("Job resubmission completed.\n")


def reset_workflow(args, job_dir, log_dir, sites_file, fileset_file):
    """Remove existing workflow outputs and rerun runner.py."""
    print("-" * 60)
    print("RESET REQUESTED")
    print("-" * 60)

    subprocess.run(["rm", "-rf", str(job_dir)], check=False)
    subprocess.run(["rm", "-rf", str(log_dir)], check=False)

    for path in (sites_file, fileset_file):
        if path.exists():
            path.unlink()

    # Forward only arguments defined by this script.
    reset_cmd = [
        "python3",
        "runner.py",
        "-w",
        args.workflow,
        "-y",
        args.year,
        "--output_format",
        args.output_format,
        "--executor",
        args.executor,
    ]

    print("Rerunning the workflow via runner.py...")
    subprocess.run(reset_cmd, check=True)
    print("Reset complete.\n")


def main():
    args = parse_args()

    base_dir = Path.cwd()
    output_dir = Path(make_output_directory(args))

    condor_dir = base_dir / "condor"
    job_dir = condor_dir / args.workflow / args.year
    log_dir = condor_dir / "logs" / args.workflow / args.year
    fileset_dir = base_dir / "analysis" / "filesets"

    if not job_dir.is_dir() and not args.reset:
        raise FileNotFoundError(f"Condor job directory does not exist: {job_dir}")

    nano_version = get_nano_version(args.year)
    sites_file = fileset_dir / f"{args.year}_sites.yaml"
    fileset_file = fileset_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"

    if args.reset:
        reset_workflow(
            args,
            job_dir,
            log_dir,
            sites_file,
            fileset_file,
        )

    jobnum, jobnum_done, error_files = get_jobs_info(
        job_dir,
        output_dir,
        log_dir,
        args.output_format,
        args.hours_ago,
    )

    jobnum_missing, datasets_with_missing_jobs = print_job_status(
        jobnum,
        jobnum_done,
    )

    if not datasets_with_missing_jobs:
        print("All expected jobs have completed successfully.")
        return

    missing_datasets = sorted(datasets_with_missing_jobs)
    errors_by_dataset = {}

    # Diagnose each dataset using its current missing job identifiers.
    for dataset in missing_datasets:
        err_files = error_files.get(dataset, [])
        missing_partitions = {str(job) for job in jobnum_missing[dataset]}

        if not err_files and args.verbose:
            print(
                f"[{dataset}] No recent .err files found. "
                "Using missing partitions for endpoint discovery."
            )

        errors_by_dataset[dataset] = analyze_xrootd_errors_by_dataset(
            error_files=err_files,
            job_dir=job_dir,
            dataset=dataset,
            missing_partitions=missing_partitions,
            verbose=args.verbose,
        )

    for dataset in missing_datasets:
        errors = errors_by_dataset[dataset]
        missing_jobs = sort_job_numbers(jobnum_missing[dataset])
        sites = sorted(errors.get("sites", set()))

        print(
            f"- [{dataset}] "
            f"(partitions with missing jobs: {', '.join(missing_jobs)}) "
            f"(problematic sites: {', '.join(sites) or 'not identified'})"
        )

    if args.verbose:
        print("\n" + "=" * 60)
        print("DETAILED XROOTD ERROR ANALYSIS")
        print("=" * 60)

        for dataset in missing_datasets:
            errors = errors_by_dataset[dataset]

            print(f"\nDataset: {dataset}")
            print(
                "  Error types: "
                + (
                    ", ".join(sorted(errors.get("error_types", set())))
                    or "unrecognized or no logs"
                )
            )
            print(
                "  Endpoints: "
                + (
                    ", ".join(sorted(errors.get("endpoints", set())))
                    or "none identified"
                )
            )
            print(
                "  Sites: "
                + (", ".join(sorted(errors.get("sites", set()))) or "none identified")
            )
            print(
                "  Missing jobs: "
                + (", ".join(sort_job_numbers(jobnum_missing[dataset])) or "none")
            )

    if args.update_executor:
        for dataset in missing_datasets:
            update_executor_for_dataset(
                job_dir,
                dataset,
                executor=args.executor,
            )

    # Only datasets with identified CMS sites can be regrouped for fetch.py.
    dataset_sites = {
        dataset: errors["sites"]
        for dataset, errors in errors_by_dataset.items()
        if errors.get("sites")
    }

    if not dataset_sites:
        print(
            "\nNo problematic CMS sites could be identified. "
            "Filesets will not be regenerated automatically."
        )
    else:
        if args.single_fetch:
            all_sites = set().union(*dataset_sites.values())
            all_datasets = sorted(dataset_sites)
            groups = {frozenset(all_sites): all_datasets}
        else:
            groups = group_datasets_by_sites(dataset_sites)

        if args.verbose:
            print("\nDatasets grouped by problematic sites:")

            for sites, datasets in groups.items():
                print(f"  Sites: {', '.join(sorted(sites))}")
                print(f"  Datasets: {', '.join(sorted(datasets))}")

        if timed_input("\nBuild input filesets for these groups?") in {
            "y",
            "yes",
        }:
            for sites, datasets in groups.items():
                update_input_filesets_for_group(
                    sites,
                    args.year,
                    fileset_dir,
                    job_dir,
                    datasets,
                )

    if timed_input("Resubmit missing jobs now?") in {"y", "yes"}:
        resubmit_jobs(
            job_dir,
            jobnum_missing,
            missing_datasets,
            args.workflow,
            args.year,
        )


if __name__ == "__main__":
    main()
