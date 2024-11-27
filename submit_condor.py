import json
import yaml
import argparse
from copy import deepcopy
from pathlib import Path
from checker import run_checker
from analysis.utils import paths
from condor.utils import submit_condor
from analysis.filesets.utils import get_rootfiles, divide_list


def main(args):
    run_checker(args)
    args = vars(args)

    # set output path
    processor_output_path = paths.processor_path(
        processor=args["processor"],
        year=args["year"],
    )
    args["output_path"] = str(processor_output_path)

    # split dataset into batches
    fileset_path = Path(f"{Path.cwd()}/analysis/filesets")
    with open(f"analysis/filesets/{args['year']}_lxplus.yaml", "r") as f:
        root_files = yaml.safe_load(f)[args["dataset"]]
    root_files_list = divide_list(root_files, args["nfiles"])
    
    # submit job for each partition
    for i, partition in enumerate(root_files_list, start=1):
        if len(root_files_list) == 1:
            dataset_key = args["dataset"]
        else:
            dataset_key = f'{args["dataset"]}_{i}'
        
        partition_fileset = {dataset_key: partition}
        # set condor and submit args
        args["dataset_key"] = dataset_key
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--year {args['year']} "
            f"--output_path {args['output_path']} "
            f"--dataset {dataset_key} "
            # dictionaries must be passed as a string enclosed in single quotes,
            # with strings within the dictionary enclosed in double quotes.
            # we use json.dumps() to switch from single to double quotes within the dictionary
            f"--partition_fileset '{json.dumps(partition_fileset)}' "
        )
        submit_condor(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztomumu",
        help="processor to be used {ztomumu, ztoee} (default ztomumu)",
    )
    parser.add_argument(
        "--dataset",
        dest="dataset",
        type=str,
        default="",
        help="dataset name",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022postEE",
        help="dataset year {2022preEE, 2022postEE} (default 2022postEE)",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Enable Condor job submission. If not provided, it just builds datasets and condor files",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=20,
        help="number of root files to include in each dataset partition (default 20)",
    )
    args = parser.parse_args()
    main(args)
