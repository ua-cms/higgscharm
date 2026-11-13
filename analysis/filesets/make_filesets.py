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
        "2025",
        "2026",
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
        elif str(query).endswith("/USER"):
            # USER (phys03) datasets are fetched outside singularity via make_signal_filesets.py
            print(f"Skipping USER dataset (handled by make_signal_filesets.py): {sample}")
        else:
            das_queries[sample] = query

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
    ddc.do_save(f"dataset_discovery_{args.year}.json")

    # load and reformat generated fileset
    with open(f"dataset_discovery_{args.year}.json", "r") as f:
        dataset_discovery = json.load(f)
    if not dataset_discovery:
        print("WARNING: Rucio returned no results. Check grid proxy and site availability.")
        dataset_discovery = {}

    # load existing fileset to preserve already-fetched entries (e.g. phys03 samples)
    fileset_file = filesets_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"
    if fileset_file.exists():
        with open(fileset_file, "r") as f:
            new_dataset = json.load(f)
    else:
        new_dataset = {key: [] for key in das_queries}

    for dataset in dataset_discovery:
        root_files = list(dataset_discovery[dataset]["files"].keys())
        dataset_key = dataset_discovery[dataset]["metadata"]["short_name"]
        if dataset_key.startswith("Single"):
            new_dataset[dataset_key.split("_")[0]] += root_files
        else:
            new_dataset[dataset_key] = root_files

    # normalize redirectors: replace IIHE-local xrootd endpoints (unreachable from CERN Condor)
    # with the global redirector so XRootD can route to any available whitelisted site.
    # Note: T2_BE_IIHE is blacklisted in the sites file, so Rucio should not return IIHE replicas,
    # but we sanitize here in case any slip through.
    for key in new_dataset:
        new_dataset[key] = [
            f.replace("root://maite.iihe.ac.be:1094/", "root://cms-xrd-global.cern.ch//")
             .replace("root://maite.iihe.ac.be:1095/", "root://cms-xrd-global.cern.ch//")
            for f in new_dataset[key]
        ]

    with open(fileset_file, "w") as json_file:
        json.dump(new_dataset, json_file, indent=4, sort_keys=True)
