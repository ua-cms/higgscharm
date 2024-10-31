import json
import argparse
from copy import deepcopy
from condor.utils import submit_condor
from analysis.utils import paths
from analysis.filesets.utils import build_single_fileset, divide_list
from checker import run_checker

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
    dataset = args["dataset"]
    fileset = build_single_fileset(name=dataset, year=args["year"])
    root_files = list(fileset[dataset]["files"].keys())
    root_files_list = divide_list(root_files)
    # run over batches
    for i, partition in enumerate(root_files_list, start=1):
        partition_fileset = deepcopy(fileset)
        if len(root_files_list) > 1:
            partition_fileset[f"{dataset}_{i}"] = partition_fileset[dataset]
            del partition_fileset[dataset]
            partition_fileset[f"{dataset}_{i}"]["files"] = {
                p: "Events" for p in partition
            }
        dataset_runnable_key = [key for key in partition_fileset][0]
        # set condor and submit args
        args["dataset"] = dataset_runnable_key
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--year {args['year']} "
            f"--output_path {args['output_path']} "
            f"--dataset {dataset_runnable_key} "
            # dictionaries must be passed as a string enclosed in single quotes,
            # with strings within the dictionary enclosed in double quotes.
            # we use json.dumps() to switch from single to double quotes within the dictionary
            f"--partition_fileset '{json.dumps(partition_fileset)}' "
        )
        args["cmd"] += f"--stepsize {args['stepsize']} "
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
        "--stepsize",
        dest="stepsize",
        type=int,
        default=50_000,
        help="stepsize param for coffea.dataset_tools.preprocess function",
    )
    args = parser.parse_args()
    main(args)