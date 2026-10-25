import re
import glob
import yaml
import json
import subprocess
import numpy as np
from pathlib import Path
from collections import defaultdict
from analysis.workflows.config import WorkflowConfigBuilder


def get_rootfiles(year: str, dataset: str):
    main_dir = Path.cwd()
    fileset_path = Path(f"{main_dir}/analysis/filesets")
    with open(f"{fileset_path}/{year}_fileset.yaml", "r") as f:
        dataset_config = yaml.safe_load(f)[dataset]

    # check for .root files in the specified path
    root_files = glob.glob(f"{dataset_config['path']}/*.root")
    if not root_files:
        # if no files found, check in the subdirectories
        root_files = glob.glob(f"{dataset_config['path']}/*/*.root")
    elif not root_files:
        raise FileNotFoundError(
            f"No .root files found in {dataset_config['path']} or its subdirectories."
        )
    return root_files


def divide_list(lst: list, nfiles: int = 20) -> list:
    """Divide a list into sublists such that each sublist has at least 20 elements."""
    if len(lst) < nfiles:
        return [lst]

    # Dynamically calculate the number of sublists such that each has at least 20 elements
    n = len(lst) // nfiles  # This gives the number of groups with at least 20 elements
    if len(lst) % nfiles != 0:
        n += 1  # Increase n by 1 if there is a remainder, to accommodate extra elements

    # Divide the list into 'n' sublists
    size = len(lst) // n
    remainder = len(lst) % n
    result = []
    start = 0

    for i in range(n):
        if i < remainder:
            end = start + size + 1
        else:
            end = start + size
        result.append(lst[start:end])
        start = end
    return result


def get_nano_version(year: str):
    if year.startswith("201"):
        nano_version = "9"
    elif year == "2024":
        nano_version = "15"
    else:
        nano_version = "12"
    return nano_version


def get_dataset_config(year: str):
    aux_year_map = {
        "2016": "2016preVFP",
        "2022": "2022preEE",
        "2023": "2023preBPix",
    }
    aux_year = aux_year_map.get(year, year)
    fileset_path = Path.cwd() / "analysis" / "filesets"
    nano_version = get_nano_version(year)
    fileset_file = f"{fileset_path}/{aux_year}_nanov{nano_version}.yaml"
    with open(fileset_file, "r") as f:
        dataset_config = yaml.safe_load(f)
    return dataset_config


def get_dataset_name(dataset):
    datasets = ["MuonEG", "Muon", "EGamma", "SingleMuon", "DoubleMuon"]
    for dataset_key in datasets:
        if dataset.startswith(dataset_key):
            return dataset_key
    return "mc"


def get_dataset_era(dataset: str, year: str):
    dataset_config = get_dataset_config(year)
    for dataset_key in dataset_config:
        if dataset.startswith(dataset_key):
            return dataset_config[dataset_key]["era"]


def modify_site_list(year: str, site: str, status: str) -> None:
    """
    Move a given site to the specified status list ("white" or "black")

    Parameters:
    -----------
        site: The site identifier to modify.
        status: Desired list to move the site to ("white" or "black").
    """
    yaml_file = Path.cwd() / "analysis" / "filesets" / f"{year}_sites.yaml"
    with open(yaml_file, "r") as f:
        data = yaml.safe_load(f)

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

    with open(yaml_file, "w") as f:
        yaml.dump(data, f, default_flow_style=False)


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


def identify_error_types(content: str) -> set[str]:
    """Identify recognized XRootD, connection, and file-access errors."""
    patterns = {
        **EXECUTOR_ERROR_PATTERNS,
        **FILE_ERROR_PATTERNS,
    }

    return {
        error_type
        for error_type, error_patterns in patterns.items()
        if any(
            re.search(pattern, content, flags=re.IGNORECASE)
            for pattern in error_patterns
        )
    }


def get_missing_job_partitions(job_dir: Path, dataset: str) -> set[str]:
    """Read the missing job/partition identifiers from missing.txt."""
    missing_path = Path(job_dir) / dataset / "missing.txt"

    if not missing_path.is_file():
        return set()

    return {
        line.strip() for line in missing_path.read_text().splitlines() if line.strip()
    }


def analyze_xrootd_error_logs(
    error_files,
    job_dir: Path,
    dataset: str,
    missing_partitions=None,
    verbose: bool = False,
) -> dict:
    """
    Identify error types and XRootD endpoints for one dataset.

    Endpoint discovery first examines the error logs. If no endpoints
    are found, it falls back to the files in the missing partitions.

    missing_partitions can be supplied directly by jobs_status.py.
    Otherwise, the function reads them from missing.txt.
    """
    dataset_dir = Path(job_dir) / dataset
    partitions_path = dataset_dir / "partitions.json"

    if missing_partitions is None:
        missing_partitions = get_missing_job_partitions(Path(job_dir), dataset)

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

    # First, inspect the error logs directly.
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

    # Fallback: inspect only the currently missing partitions.
    if not endpoints:
        for partition_id in missing_partitions:
            partition_data = partitions_data.get(partition_id, {})

            for sample_data in partition_data.values():
                for url in sample_data.get("files", {}):
                    match = endpoint_pattern.match(url)
                    if match:
                        endpoints.add(match.group(0))

        if verbose:
            print(f"[{dataset}] Fallback endpoints: " f"{sorted(endpoints)}")

    if verbose:
        print(f"[{dataset}] Error types: {sorted(error_types)}")
        print(f"[{dataset}] Endpoints: {sorted(endpoints)}")
        print(f"[{dataset}] Missing partitions: " f"{sorted(missing_partitions)}")

    return {
        "endpoints": endpoints,
        "error_types": error_types,
        "partitions": missing_partitions,
    }


def fileset_checker(samples: list, year: str):
    """check if the fileset for the given year exists, generate it otherwise"""
    nano_version = get_nano_version(year)
    filesets_path = Path.cwd() / "analysis" / "filesets"
    fileset_file = filesets_path / f"fileset_{year}_nanov{nano_version}_lxplus.json"

    build_input_fileset = False
    if not fileset_file.exists():
        build_input_fileset = True
    else:
        with open(fileset_file, "r") as f:
            fileset = json.load(f)
        samples_in_fileset = [
            fileset[sample]["metadata"]["short_name"] for sample in fileset
        ]

        build_input_fileset = any(ds not in samples_in_fileset for ds in samples)

    if build_input_fileset:
        print("\nBuilding input filesets for:")
        print(yaml.dump(samples, default_flow_style=False, sort_keys=False, indent=2))
        cmd = f"python3 fetch.py --year {year} --samples {' '.join(samples)}"
        subprocess.run(cmd, shell=True)


def get_datasets_map(year: str):
    dataset_config = get_dataset_config(year)
    datasets_map = {}
    for sample in dataset_config:
        keys = dataset_config[sample]["key"]
        if isinstance(keys, str):
            if keys in datasets_map:
                datasets_map[keys] += [sample]
            else:
                datasets_map[keys] = [sample]
        elif isinstance(keys, list):
            for key in keys:
                if key in datasets_map:
                    datasets_map[key] += [sample]
                else:
                    datasets_map[key] = [sample]
    return datasets_map


def get_datasets_to_run_over(workflow: str, year: str):
    config_builder = WorkflowConfigBuilder(workflow=workflow)
    workflow_config = config_builder.build_workflow_config()

    samples_keys_to_run_over = []
    if "data" in workflow_config.datasets:
        data_samples = workflow_config.datasets["data"]
        samples_keys_to_run_over += data_samples
    if "mc" in workflow_config.datasets:
        mc_samples = workflow_config.datasets["mc"]
        samples_keys_to_run_over += mc_samples
    if "signal" in workflow_config.datasets:
        signal_samples = workflow_config.datasets["signal"]
        samples_keys_to_run_over += signal_samples

    datasets_to_run_over = []
    datasets_map = get_datasets_map(year)
    for key in samples_keys_to_run_over:
        if key not in datasets_map:
            print(f"\n{key} not availabe for {year}!")
            continue
        datasets_to_run_over += datasets_map[key]
    return datasets_to_run_over


def get_workflow_key_process_map(workflow_config, year: str):
    datasets = workflow_config.datasets
    dataset_configs = get_dataset_config(year)
    sample_keys = np.concatenate(list(datasets.values())).tolist()

    processes = []
    key_process_map = {}

    for sample in dataset_configs:
        sample_process = dataset_configs[sample]["process"]
        if sample_process == "Data":
            continue
        sample_key = dataset_configs[sample]["key"]
        if isinstance(sample_key, str):
            if sample_key in sample_keys:
                if sample_process not in processes:
                    processes.append(sample_process)
                    key_process_map[sample_key] = sample_process
        elif isinstance(sample_key, list):
            for sk in sample_key:
                if sk in sample_keys:
                    if sample_process not in processes:
                        processes.append(sample_process)
                        key_process_map[sk] = sample_process

    return key_process_map


def get_process_era_map(year: str):
    dataset_configs = get_dataset_config(year)
    process_era_map = {}
    for sample in dataset_configs:
        sample_process = dataset_configs[sample]["process"]
        if sample_process not in process_era_map:
            process_era_map[sample_process] = dataset_configs[sample]["era"]

    return process_era_map


def get_process_sample_map(datasets: list[str], year: str) -> dict[str, list[str]]:
    """map processes to their corresponding samples based on dataset config"""
    dataset_configs = get_dataset_config(year)
    process_sample_map = defaultdict(list)
    for sample in datasets:
        config = dataset_configs[sample]
        process_sample_map[config["process"]].append(sample)
    return dict(process_sample_map)
