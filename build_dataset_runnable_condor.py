import os
import glob
import argparse
import subprocess
from pathlib import Path
from collections import OrderedDict
from condor.utils import submit_condor
from analysis.configs import load_config


def main(args):
    args = vars(args)
    main_dir = Path.cwd()
    if args["dataset_name"] == "all":
        dataset_path = f"{main_dir}/analysis/configs/dataset/{args['year']}/"
        dataset_names = [
            f.split("/")[-1].replace(".py", "")
            for f in glob.glob(f"{dataset_path}*.py", recursive=True)
        ]
        dataset_names.remove("__init__")

        for dataset_name in dataset_names:
            args["dataset_name"] = dataset_name
            args["cmd"] = (
                f"python3 build_dataset_runnable.py --dataset_name {dataset_name}"
            )
            submit_condor(args, is_dataset=True)
    else:
        args["cmd"] = (
            f"python3 build_dataset_runnable.py --dataset_name {args['dataset_name']}"
        )
        submit_condor(args, is_dataset=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="all",
        help="dataset_name",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022EE, 2022, 2023}",
    )
    args = parser.parse_args()
    main(args)