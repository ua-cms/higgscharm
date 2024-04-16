import json
import glob
import tqdm
import argparse
from pathlib import Path
from copy import deepcopy
from coffea.dataset_tools import preprocess
from analysis.configs.load_config import load_config
from analysis.filesets.utils import build_single_fileset, divide_list


def build_dataset_runnable(dataset_name:str , year:str) -> None:
    """
    build and save dataset runnables partitions
    
    Parameters:
        name:
            name of the dataset
        year:
            year of the dataset {2022EE, 2022, 2023}
    """
    dataset_config = load_config(
        config_type="dataset", config_name=dataset_name, year=year
    )
    print("-" * 70)
    print(
        f"Start {dataset_config.name} runnables building with {dataset_config.partitions} partitions"
    )
    print("-" * 70)

    # make output directories
    main_directory = Path().cwd()
    output_directory = Path(
        f"{main_directory}/analysis/filesets/dataset_runnables/{dataset_config.year}"
    )
    if not output_directory.exists():
        output_directory.mkdir(parents=True)
        
    # build dataset fileset 
    fileset = build_single_fileset(name=dataset_config.name, year=dataset_config.year)
    # get partition lists from root files
    root_files = list(fileset[dataset_config.name]["files"].keys())
    root_files_list = divide_list(root_files, dataset_config.partitions)
    # make dataset runnables
    dataset_runnables = []
    for i, partition in tqdm.tqdm(
        enumerate(root_files_list, start=1), total=len(root_files_list)
    ):
        partition_fileset = deepcopy(fileset)
        if dataset_config.partitions > 1:
            partition_fileset[f"{dataset_config.name}_{i}"] = partition_fileset[
                dataset_config.name
            ]
            del partition_fileset[dataset_config.name]
            partition_fileset[f"{dataset_config.name}_{i}"]["files"] = {
                p: "Events" for p in partition
            }
        dataset_runnable_key = [key for key in partition_fileset][0]

        dataset_runnable, dataset_updated = preprocess(
            partition_fileset,
            step_size=dataset_config.stepsize,
            align_clusters=False,
            files_per_batch=1,
            save_form=False,
        )
        with open(f"{output_directory}/{dataset_runnable_key}.json", "w") as json_file:
            json.dump(dataset_runnable, json_file, indent=4, sort_keys=True)
            

def main(args):
    main_dir = Path.cwd()
    if args.dataset_name == "all":
        dataset_path = f"{main_dir}/analysis/configs/dataset/{args.year}/"
        dataset_names = [
            f.split("/")[-1].replace(".py", "")
            for f in glob.glob(f"{dataset_path}*.py", recursive=True)
        ]
        dataset_names.remove("__init__")

        for dataset_name in dataset_names:
            build_dataset_runnable(dataset_name=dataset_name, year=args.year)
    else:
        build_dataset_runnable(dataset_name=args.dataset_name, year=args.year)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="all",
        help="dataset name. Set 'all' to build all datasets",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year",
    )
    args = parser.parse_args()
    main(args)