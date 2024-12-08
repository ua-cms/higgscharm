import json
import yaml
import argparse
from copy import deepcopy
from pathlib import Path
from checker import run_checker
from condor.utils import submit_condor
from analysis.filesets.utils import divide_list
from analysis.utils import make_output_directory


def main(args):
    run_checker(args)
    args = vars(args)

    # add output path to args
    args["output_path"] = make_output_directory(args)

    # split dataset into batches
    fileset_path = Path.cwd() / "analysis" / "filesets"
    with open(f"{fileset_path}/fileset_{args['year']}_NANO_lxplus.json", "r") as f:
        root_files = json.load(f)[args["dataset"]]
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
        help="processor to be used {ztomumu, ztoee, zto4l} (default ztomumu)",
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
        "--nfiles",
        dest="nfiles",
        type=int,
        default=20,
        help="number of root files to include in each dataset partition (default 20)",
    )
    parser.add_argument(
        "--eos",
        action="store_true",
        help="Enable saving outputs to /eos",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Enable Condor job submission. If not provided, it just builds condor files",
    )
    args = parser.parse_args()
    main(args)