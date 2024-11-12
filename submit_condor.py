import json
import argparse
from copy import deepcopy
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
    # get root files for sample
    dataset = args["dataset"]
    root_files = get_rootfiles(args["year"], dataset)
    # split dataset into batches
    root_files_list = divide_list(root_files)
    # run over batches
    for i, partition in enumerate(root_files_list, start=1):
        dataset_key = dataset
        if len(root_files_list) > 1:
            dataset_key += f"_{i}"
        partition_fileset = {dataset_key: partition}
        # set condor and submit args
        args["dataset"] = dataset_key
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
    args = parser.parse_args()
    main(args)
