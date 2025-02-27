import json
import argparse
from pathlib import Path
from condor.utils import submit_condor
from analysis.filesets.utils import divide_list
from analysis.utils import make_output_directory


def main(args):
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
        args["dataset_key"] = dataset_key
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--year {args['year']} "
            f"--output_path {args['output_path']} "
            f"--dataset {dataset_key} "
            f"--output_format {args['output_format']} "
            f"--partition_fileset '{json.dumps(partition_fileset)}' "
        )
        submit_condor(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        choices=["ztomumu", "ztoee", "zzto4l", "hww"],
        help="processor to be used",
    )
    parser.add_argument(
        "--dataset",
        dest="dataset",
        type=str,
        help="dataset name",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        choices=["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"],
        help="dataset year",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=10,
        help="number of root files to include in each dataset partition (default 10)",
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
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="format of output histogram",
    )
    args = parser.parse_args()
    main(args)