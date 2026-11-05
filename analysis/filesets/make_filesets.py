import os
import yaml
import json
import argparse
from pathlib import Path
from analysis.filesets.utils import get_nano_version
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
        query = dataset_configs[sample]["query"]
        if query:
            das_queries[sample] = query
        else:
            print(f"No available query for: {sample}")

    # create a dataset_definition dict for each yeare
    dataset_definition = {}
    for dataset_key, query in das_queries.items():
        dataset_definition[f"/{query}"] = {"short_name": dataset_key}

    # set the allow sites to look for replicas
    sites_file = filesets_dir / f"{args.year}_sites.yaml"
    with open(sites_file, "r") as f:
        sites = yaml.safe_load(f)
    white_sites = sites["white"]
    black_sites = sites["black"]

    # the dataset definition is passed to a DataDiscoveryCLI
    ddc = DataDiscoveryCLI()
    ddc.do_allowlist_sites(white_sites)
    ddc.do_blocklist_sites(black_sites)
    # query rucio and get replicas
    ddc.load_dataset_definition(
        dataset_definition,
        query_results_strategy="all",
        replicas_strategy="round-robin",
    )
    fileset_file = filesets_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"
    ddc.do_save(fileset_file)

    # Remove known files with zero events.
    # https://cms-talk.web.cern.ch/t/empty-files-in-muoneg-datasets-for-2022/145956
    files_to_remove = {
        "2022postEE": {
            "/MuonEG/Run2022F-22Sep2023-v1/NANOAOD": [
                "store/data/Run2022F/MuonEG/NANOAOD/22Sep2023-v1/50000/4fb72196-3b02-4499-8f6c-a54e15692b32.root",
                "store/data/Run2022F/MuonEG/NANOAOD/22Sep2023-v1/50000/4d76213a-ef14-411a-9558-559a6df3f978.root",
            ],
            "/MuonEG/Run2022G-22Sep2023-v1/NANOAOD": [
                "store/data/Run2022G/MuonEG/NANOAOD/22Sep2023-v1/2520000/cd404eb6-8218-4787-b5ed-af6cd9fe3750.root",
            ],
        }
    }
    if args.year in files_to_remove:
        with open(fileset_file, "r") as f:
            fileset = json.load(f)

        for dataset_name, files_to_remove_for_dataset in files_to_remove[
            args.year
        ].items():
            if dataset_name not in fileset:
                continue

            original_files = fileset[dataset_name]["files"]

            filtered_files = {
                file: metadata
                for file, metadata in original_files.items()
                if not any(
                    file.endswith(file_to_remove)
                    for file_to_remove in files_to_remove_for_dataset
                )
            }

            removed_count = len(original_files) - len(filtered_files)

            fileset[dataset_name]["files"] = filtered_files

            print(f"{dataset_name}: removed {removed_count} files with zero events")

        with open(fileset_file, "w") as json_file:
            json.dump(fileset, json_file, indent=4, sort_keys=True)
