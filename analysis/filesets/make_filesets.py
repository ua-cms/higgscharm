import os
import yaml
import json
import argparse
from pathlib import Path
from coffea.dataset_tools.dataset_query import DataDiscoveryCLI


SITES = [
    "T1_US_FNAL_Disk",
    "T1_DE_KIT_Disk",
    "T1_UK_RAL_Disk",
    "T1_FR_CCIN2P3_Disk",
    "T1_FR_CCIN2P3_Disk",
    "T2_US_Vanderbilt",
    "T2_US_Purdue",
    "T2_US_Nebraska",
    "T2_DE_DESY",
    "T2_BE_IIHE",
    "T2_CH_CERN",
    "T2_DE_RWTH",
    "T2_BE_UCL",
    "T2_HU_Budapest",
    "T2_RU_JINR",
    "T2_UK_London_IC",
    "T2_ES_CIEMAT",
    "T3_US_FNALLPC",
    "T3_CH_PSI",
]


def main(args):
    # open dataset configs
    with open(f"{Path.cwd()}/{args.year}_nanov12.yaml", "r") as f:
        dataset_configs = yaml.safe_load(f)
    # read dataset queries
    das_queries = {}
    for sample in dataset_configs:
        das_queries[sample] = dataset_configs[sample]["query"]

    for year in ["2022preEE", "2022postEE"]:
        if args.year != "all":
            if year != args.year:
                continue
        # create a dataset_definition dict for each yeare
        dataset_definition = {}
        for dataset_key, query in das_queries.items():
            dataset_definition[f"/{query}"] = {
                "short_name": dataset_key,
                "metadata": {
                    "isMC": (
                        True if dataset_configs[dataset_key]["era"] == "MC" else False
                    )
                },
            }
        # the dataset definition is passed to a DataDiscoveryCLI
        ddc = DataDiscoveryCLI()
        # set the allow sites to look for replicas
        ddc.do_allowlist_sites(SITES)
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
        new_dataset = {key: [] for key in das_queries}
        for dataset in dataset_discovery:
            root_files = list(dataset_discovery[dataset]["files"].keys())
            dataset_key = dataset_discovery[dataset]["metadata"]["short_name"]
            if dataset_key.startswith("Single"):
                new_dataset[dataset_key.split("_")[0]] += root_files
            else:
                new_dataset[dataset_key] = root_files
        # save new fileset and drop 'dataset_discovery' fileset
        os.remove(f"dataset_discovery_{args.year}.json")
        with open(f"fileset_{args.year}_NANO_lxplus.json", "w") as json_file:
            json.dump(new_dataset, json_file, indent=4, sort_keys=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="all",
        help="year of the data {2022preEE, 2022postEE, all}",
    )
    args = parser.parse_args()
    main(args)
