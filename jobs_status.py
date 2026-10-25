"""
This script inspects Condor job outputs, identifies missing jobs, analyzes recent xrootd-related failures, and optionally regenerates input filesets with problematic sites blacklisted and resubmits only the missing jobs
"""

import sys
import yaml
import json
import select
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from analysis.utils import make_output_directory
from analysis.filesets.xrootd_sites import xroot_to_site
from analysis.filesets.utils import (
    divide_list,
    modify_site_list,
    analyze_xrootd_error_logs,
    get_missing_job_partitions,
    get_nano_version,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--workflow",
        dest="workflow",
        required=True,
        type=str,
        choices=[
            f.stem for f in (Path.cwd() / "analysis" / "workflows").glob("*.yaml")
        ],
        help="Workflow name (must correspond to a YAML file in analysis/workflows)",
    )
    parser.add_argument(
        "-y",
        "--year",
        type=str,
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
        type=str,
        default="coffea",
        choices=["coffea", "parquet"],
        help="Output file format of the produced histograms",
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
        help="Delete all job outputs, logs, and filesets for this workflow/year and rerun",
    )
    parser.add_argument(
        "--single_fetch",
        action="store_true",
        help="Run fetch.py only once by merging all failing sites and datasets into a single group",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed XRootD error diagnostics.",
    )
    parser.add_argument(
        "--executor",
        dest="executor",
        type=str,
        default="futures",
        choices=["iterative", "futures"],
        help="executor to run the processor",
    )
    parser.add_argument(
        "-e",
        "--update_executor",
        action="store_true",
        help="Update current executor",
    )
    return parser.parse_args()


def timed_input(prompt, timeout=10, default="y"):
    """Request user input with a timeout. Falls back to default if timed out or empty."""
    print(
        f"{prompt} [{default.upper()}/n] (auto-selected in {timeout}s): ",
        end="",
        flush=True,
    )

    # Check for available input on stdin within the timeout limit
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if ready:
        response = sys.stdin.readline().strip()
        return response.lower() if response else default.lower()
    else:
        print(f"\n[Timeout reached] Using default option: '{default.upper()}'")
        return default.lower()


def get_jobs_info(job_dir, output_dir, log_dir, output_format, hours_ago=3):
    """
    Inspect the job directory structure and determine:
      1. Which jobs were expected to run (from jobnum.txt)
      2. Which jobs successfully produced output files
      3. Which jobs produced recent error logs

    error logs are collected *per dataset*, which allows later
    analysis and remediation to remain dataset-local rather than global.

    Parameters
    ----------
    job_dir : Path
        Directory containing one subdirectory per dataset with Condor configs.
    output_dir : Path
        Directory containing produced output files.
    log_dir : Path
        Directory containing Condor log files (.err, .out, .log).
    output_format : str
        File extension of produced outputs (e.g. "coffea", "parquet").
    hours_ago : int
        Only .err files modified within this many hours are considered relevant.

    Returns
    -------
    tuple
        jobnum : dict
            dataset -> list of expected job numbers (strings)
        jobnum_done : dict
            dataset -> list of job numbers that produced output
        error_files : dict
            dataset -> list of Path objects pointing to recent .err files
    """
    jobnum, jobnum_done = {}, {}
    error_files = {}

    for dataset_dir in job_dir.iterdir():
        if not dataset_dir.is_dir():
            continue

        dataset = dataset_dir.name
        jobnum_path = dataset_dir / "jobnum.txt"
        if not jobnum_path.exists():
            raise FileNotFoundError(
                f"Missing jobnum.txt for dataset '{dataset}'. "
                f"Expected at: {jobnum_path}"
            )

        # Read expected job numbers
        jobnum[dataset] = jobnum_path.read_text().splitlines()

        # Discover completed jobs by looking for output files
        output_files = list((output_dir / dataset).glob(f"*.{output_format}"))
        jobnum_done[dataset] = [f.stem.replace(f"{dataset}_", "") for f in output_files]

        # Collect recent error logs for this dataset
        x_hours_ago = datetime.now() - timedelta(hours=hours_ago)
        dataset_errs = []
        for err_file in (log_dir / dataset).glob("*.err"):
            if datetime.fromtimestamp(err_file.stat().st_mtime) > x_hours_ago:
                dataset_errs.append(err_file)

        if dataset_errs:
            error_files[dataset] = dataset_errs

    return jobnum, jobnum_done, error_files


def print_job_status(jobnum, jobnum_done):
    """
    Print a concise but informative summary of the job execution state.

    The function reports:
      - Total expected jobs
      - Total completed jobs
      - Total missing jobs
      - A YAML-formatted list of datasets with missing jobs

    Parameters
    ----------
    jobnum : dict
        dataset -> list of expected job numbers
    jobnum_done : dict
        dataset -> list of completed job numbers

    Returns
    -------
    tuple
        jobnum_missing : dict
            dataset -> set of missing job numbers
        datasets_with_missing : list
            List of dataset names with at least one missing job
    """
    jobnum_missing = {d: set(jobnum[d]) - set(jobnum_done.get(d, [])) for d in jobnum}
    n_expected = sum(len(v) for v in jobnum.values())
    n_done = sum(len(v) for v in jobnum_done.values())
    n_missing = sum(len(v) for v in jobnum_missing.values())

    print("------------------------------------------------------------")
    print("JOB STATUS SUMMARY")
    print("------------------------------------------------------------")
    print(f"Total jobs expected : {n_expected}")
    print(f"Total jobs finished : {n_done}")
    print(f"Total jobs missing  : {n_missing}")

    datasets_with_missing = [d for d in jobnum_missing if jobnum_missing[d]]
    if n_missing:
        print("------------------------------------------------------------")
        print(
            f"Datasets with missing jobs ({len(datasets_with_missing)}/{len(jobnum)}):"
        )
        print("------------------------------------------------------------")

    return jobnum_missing, datasets_with_missing


def update_executor_for_dataset(job_dir, dataset, executor="iterative"):
    """Update the executor in a dataset's arguments.json."""
    args_json = job_dir / dataset / "arguments.json"

    if not args_json.exists():
        print(f"[{dataset}] Missing arguments.json — cannot update executor")
        return

    arguments = json.loads(args_json.read_text())

    if arguments.get("executor") == executor:
        print(f"[{dataset}] Executor is already '{executor}'")
        return

    arguments["executor"] = executor
    args_json.write_text(json.dumps(arguments, indent=4) + "\n")

    print(f"[{dataset}] Updated executor to '{executor}'")


def analyze_xrootd_errors_by_dataset(
    error_files,
    job_dir,
    dataset,
    missing_partitions,
    verbose=False,
):
    errors = analyze_xrootd_error_logs(
        error_files=error_files,
        job_dir=job_dir,
        dataset=dataset,
        missing_partitions=missing_partitions,
        verbose=verbose,
    )

    def map_sites(endpoints):
        return {
            xroot_to_site[endpoint]
            for endpoint in endpoints
            if endpoint in xroot_to_site
        }

    # First, map endpoints found directly in the error logs.
    errors["sites"] = map_sites(errors["endpoints"])

    # If no site is identified, retry with endpoints from missing partitions.
    if not errors["sites"]:
        fallback_errors = analyze_xrootd_error_logs(
            error_files=[],
            job_dir=job_dir,
            dataset=dataset,
            missing_partitions=missing_partitions,
            verbose=verbose,
        )

        fallback_endpoints = fallback_errors["endpoints"]
        fallback_sites = map_sites(fallback_endpoints)

        if fallback_sites:
            errors["endpoints"] = fallback_endpoints
            errors["sites"] = fallback_sites

        if verbose:
            print(f"[{dataset}] Fallback sites: " f"{sorted(fallback_sites)}")

    # Report endpoints that could not be mapped.
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
    """
    Group datasets by identical sets of failing sites.

    Many datasets often fail on exactly the same sites.
    Rather than regenerating input filesets separately
    for each dataset, we group datasets by their failing-site
    signature and regenerate filesets once per group.

    Example:
        Input:
            {
              "A": {"T2_US_Florida", "T2_FR_GRIF"},
              "B": {"T2_US_Florida", "T2_FR_GRIF"},
              "C": {"T2_IT_Pisa"},
            }

        Output:
            {
              frozenset({"T2_US_Florida", "T2_FR_GRIF"}): ["A", "B"],
              frozenset({"T2_IT_Pisa"}): ["C"],
            }

    Parameters
    ----------
    dataset_sites : dict
        dataset -> set of failing sites

    Returns
    -------
    dict
        frozenset(failing sites) -> list of datasets
    """
    groups = defaultdict(list)
    for dataset, sites in dataset_sites.items():
        groups[frozenset(sites)].append(dataset)
    return groups


def update_input_filesets_for_group(
    sites_to_blacklist, year, fileset_dir, job_dir, datasets
):
    """
    Regenerate input filesets for a group of datasets that share the same
    failing sites.

    The procedure is:
      1. Reset all sites to "white" (allowed)
      2. Blacklist only the sites known to be problematic for this group
      3. Run fetch.py once for all datasets in the group
      4. Regenerate partition files (partitions.json) for each dataset

    This minimizes expensive calls to fetch.py while still preserving
    dataset-level correctness.

    Parameters
    ----------
    sites_to_blacklist : iterable
        Collection of site names to blacklist for this group
    year : str
        Dataset year
    fileset_dir : Path
        Directory containing JSON filesets
    job_dir : Path
        Directory containing Condor job files
    datasets : list
        List of dataset names to regenerate
    """
    print("------------------------------------------------------------")
    print("REGENERATING FILESETS FOR DATASET GROUP")
    print("------------------------------------------------------------")
    print(f"Datasets           : {datasets}")
    print(f"Blacklisted sites  : {sorted(sites_to_blacklist)}")

    # First, reset all sites to "white" to ensure no cross-group contamination.
    # This guarantees that each group is regenerated with exactly its own
    # blacklist, and nothing else.
    for site in xroot_to_site.values():
        modify_site_list(year, site, "white")

    # Apply the blacklist for this specific group.
    for site in sites_to_blacklist:
        modify_site_list(year, site, "black")

    # Run fetch.py once for the entire dataset group.
    samples_str = " ".join(datasets)
    print("Running fetch.py for this group")
    subprocess.run(
        ["python3", "fetch.py", "--year", year, "--samples", samples_str],
        check=False,
    )
    # Load the regenerated filesets.
    nano_version = get_nano_version(year)
    fileset_path = fileset_dir / f"fileset_{year}_nanov{nano_version}_lxplus.json"

    if not fileset_path.exists():
        raise FileNotFoundError(f"Regenerated fileset not found: {fileset_path}")

    fileset = json.loads(fileset_path.read_text())

    short_name_map = {}
    for sample in fileset:
        short_name = fileset[sample]["metadata"]["short_name"]
        short_name_map[short_name] = sample

    # Regenerate partitions.json for each dataset in the group.

    for dataset in datasets:
        if dataset not in short_name_map:
            print(
                f"[{dataset}] Not found in fileset JSON — skipping",
                flush=True,
            )
            continue

        args_json = job_dir / dataset / "arguments.json"
        if not args_json.exists():
            print(
                f"[{dataset}] Missing {args_json} — cannot repartition",
                flush=True,
            )
            continue

        current_fileset = fileset[short_name_map[dataset]]
        fileset_files = current_fileset["files"]

        root_files = list(fileset_files.items())
        nfiles = json.loads(args_json.read_text())["nfiles"]
        fileset_chunks = divide_list(root_files, nfiles)

        partition_dataset = {}
        for i, chunk in enumerate(fileset_chunks, start=1):
            dataset_key = f"{dataset}_{i}" if len(fileset_chunks) > 1 else dataset
            partition_dataset[i] = {
                dataset_key: {
                    "files": dict(chunk),
                    "metadata": current_fileset.get("metadata", {}),
                }
            }

        partition_file = job_dir / dataset / "partitions.json"
        with open(partition_file, "w") as json_file:
            json.dump(partition_dataset, json_file, indent=4)

    print("Fileset regeneration for this group completed.\n", flush=True)


def resubmit_jobs(job_dir, jobnum_missing, datasets_with_missing_jobs, workflow, year):
    """
    Prepare and resubmit only the jobs that are missing output files.

    For each affected dataset:
      1. Write a missing.txt file listing the missing job numbers
      2. Patch the Condor submission file to use missing.txt instead of jobnum.txt
      3. Submit the modified job description to Condor

    A backup of the original submission file (*_all.sub) is created once
    per dataset to avoid destructive overwrites.

    Parameters
    ----------
    job_dir : Path
        Directory containing Condor job files
    jobnum_missing : dict
        dataset -> set of missing job numbers
    datasets_with_missing_jobs : list
        List of dataset names with missing jobs
    workflow : str
        Workflow name
    year : str
        Dataset year
    """
    print("------------------------------------------------------------")
    print("RESUBMITTING MISSING JOBS")
    print("------------------------------------------------------------")

    to_resubmit = []
    for dataset in datasets_with_missing_jobs:
        missing_jobs = sorted(jobnum_missing[dataset])
        missing_file = job_dir / dataset / "missing.txt"
        with open(missing_file, "w") as f:
            print(*missing_jobs, sep="\n", file=f)

        condor_file = job_dir / dataset / f"{workflow}_{dataset}.sub"
        condor_backup = condor_file.with_name(condor_file.stem + "_all.sub")
        if not condor_backup.exists():
            subprocess.run(["cp", str(condor_file), str(condor_backup)], check=True)

        submit_text = condor_file.read_text().replace("jobnum.txt", "missing.txt")
        condor_file.write_text(submit_text)
        to_resubmit.append(str(condor_file))

    for submit_file in to_resubmit:
        print(f"Submitting {submit_file}")
        subprocess.run(["condor_submit", submit_file], check=True)

    print("Job resubmission completed.\n")


def main():
    args = parse_args()

    base_dir = Path.cwd()
    output_dir = Path(make_output_directory(args))

    condor_dir = base_dir / "condor"
    workflow_dir = args.workflow

    job_dir = condor_dir / workflow_dir / args.year
    # Inspect expected jobs, completed outputs, and recent error logs.
    if not job_dir.is_dir():
        raise FileNotFoundError(f"Condor job directory does not exist: {job_dir}")

    log_dir = condor_dir / "logs" / workflow_dir / args.year
    fileset_dir = base_dir / "analysis" / "filesets"

    nano_version = get_nano_version(args.year)
    sites_file = fileset_dir / f"{args.year}_sites.yaml"
    fileset_file = fileset_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"

    # Optionally reset the workflow and rerun all jobs.
    if args.reset:
        print("-" * 60)
        print("RESET REQUESTED")
        print("-" * 60)

        subprocess.run(["rm", "-rf", str(job_dir)], check=False)
        subprocess.run(["rm", "-rf", str(log_dir)], check=False)

        for path in (sites_file, fileset_file):
            if path.exists():
                path.unlink()

        reset_cmd = [
            "python3",
            "runner.py",
            "-w",
            args.workflow,
            "-y",
            args.year,
            "--output_format",
            args.output_format,
            "-m",
            args.memory,
            "--executor",
            args.executor,
        ]

        if args.eos:
            reset_cmd.append("--eos")

        print("Rerunning the workflow via runner.py...")
        subprocess.run(reset_cmd, check=True)
        print("Reset complete.\n")

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

    # Diagnose each dataset independently.
    for dataset in missing_datasets:
        err_files = error_files.get(dataset, [])

        # Use the current missing jobs, not a potentially stale
        # missing.txt left by a previous resubmission.
        missing_partitions = {str(job) for job in jobnum_missing[dataset]}

        if not err_files and args.verbose:
            print(
                f"[{dataset}] No recent .err files found. "
                "Using missing partitions for endpoint discovery."
            )

        errors = analyze_xrootd_errors_by_dataset(
            error_files=err_files,
            job_dir=job_dir,
            dataset=dataset,
            missing_partitions=missing_partitions,
            verbose=args.verbose,
        )

        errors_by_dataset[dataset] = errors

    for dataset in missing_datasets:
        errors = errors_by_dataset.get(dataset, {})

        missing_jobs = sorted(
            jobnum_missing[dataset],
            key=lambda value: (
                not str(value).isdigit(),
                int(value) if str(value).isdigit() else str(value),
            ),
        )

        sites = sorted(errors.get("sites", set()))

        print(
            f"- [{dataset}] "
            f"(partitions with missing jobs: {', '.join(missing_jobs)}) "
            f"(problematic sites: {', '.join(sites) or 'not identified'})"
        )

    # Print detailed diagnostics when requested.
    if args.verbose:
        print("\n" + "=" * 60)
        print("DETAILED XROOTD ERROR ANALYSIS")
        print("=" * 60)

        for dataset in missing_datasets:
            errors = errors_by_dataset.get(dataset, {})

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
                + (
                    ", ".join(
                        sorted(
                            jobnum_missing[dataset],
                            key=lambda value: (
                                not str(value).isdigit(),
                                (int(value) if str(value).isdigit() else str(value)),
                            ),
                        )
                    )
                    or "none"
                )
            )

    # Offer to change the executor only for datasets with
    # Change the executor only when explicitly requested.
    if args.update_executor:
        for dataset in sorted(errors_by_dataset):
            update_executor_for_dataset(
                job_dir,
                dataset,
                executor=args.executor,
            )

    # Only datasets with successfully mapped sites can be grouped
    # for site blacklisting and fileset regeneration.
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

        if timed_input("\nBuild input filesets for these groups?") in {"y", "yes"}:
            for sites, datasets in groups.items():
                update_input_filesets_for_group(
                    sites,
                    args.year,
                    fileset_dir,
                    job_dir,
                    datasets,
                )

    # Ask before resubmitting jobs.
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
