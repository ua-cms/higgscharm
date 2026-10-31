import os
import yaml
import json
import argparse
from pathlib import Path
from analysis.filesets.utils import get_nano_version, is_private_query
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI

if __name__ == "__main__":
    years = [
        "2016preVFP",
        "2016postVFP",
        "2017",
        "2018",
        "2022preEE",
        "2022postEE",
        "2023preBPix",
        "2023postBPix",
        "2024",
        "2025",
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=str,
        choices=years,
    )
    parser.add_argument(
        "--samples",
        nargs="*",
        type=str,
        help="(Optional) List of samples to use. If omitted, all available samples will be used",
    )
    args = parser.parse_args()

    # open dataset configs
    filesets_dir = Path.cwd() / "analysis" / "filesets"
    nano_version = get_nano_version(args.year)
    datasets_dir = filesets_dir / f"{args.year}_nanov{nano_version}.yaml"
    with open(datasets_dir, "r") as f:
        dataset_configs = yaml.safe_load(f)

    # read dataset queries
    if args.samples:
        samples_to_use = args.samples
    else:
        samples_to_use = list(dataset_configs.keys())
    das_queries = {}
    for sample in samples_to_use:
        if sample not in dataset_configs:
            print(f"Sample '{sample}' not found in dataset config, skipping")
            continue
        query = dataset_configs[sample]["query"]
        if not query:
            print(f"No available query for: {sample}")
        elif is_private_query(query):
            # USER (phys03) and local signal datasets are added by make_signal_filesets.py
            print(f"Skipping private dataset (handled by make_signal_filesets.py): {sample}")
        else:
            das_queries[sample] = query

    fileset_file = filesets_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"
    if not das_queries:
        # only private samples requested: do_save below would overwrite the fileset with {}
        print("No DAS samples to query")
        raise SystemExit(0)

    # create a dataset_definition dict for each year
    dataset_definition = {}
    for dataset_key, query in das_queries.items():
        dataset_definition[f"/{query}"] = {"short_name": dataset_key}
    # the dataset definition is passed to a DataDiscoveryCLI
    ddc = DataDiscoveryCLI()
    # set the allow sites to look for replicas
    sites_file = filesets_dir / f"{args.year}_sites.yaml"
    with open(sites_file, "r") as f:
        sites = yaml.safe_load(f)["white"]
    ddc.do_allowlist_sites(sites)
    # query rucio and get replicas
    ddc.load_dataset_definition(
        dataset_definition,
        query_results_strategy="all",
        replicas_strategy="round-robin",
    )
    # do_save rewrites the whole file: keep the entries of the samples not queried now
    # (e.g. signal samples added by make_signal_filesets.py)
    if fileset_file.exists():
        with open(fileset_file, "r") as f:
            old_fileset = json.load(f)
        for key, entry in old_fileset.items():
            if not isinstance(entry, dict) or key in ddc.final_output:
                continue
            if entry.get("metadata", {}).get("short_name") not in das_queries:
                ddc.final_output[key] = entry
    ddc.do_save(fileset_file)
