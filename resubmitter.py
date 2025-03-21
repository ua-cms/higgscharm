import glob
import yaml
import argparse
import subprocess
from pathlib import Path
from analysis.utils import make_output_directory


def main(args):
    args.output_path = make_output_directory(vars(args))

    """Helper function to resubmit condor jobs"""
    main_dir = Path.cwd()
    print(f"Reading outputs from: {args.output_path}")

    # get jobs to be run
    condor_path = f"{main_dir}/condor/{args.processor}/{args.year}"
    condor_files = glob.glob(f"{condor_path}/*/*.sub", recursive=True)
    total_files = len(condor_files)

    # get jobs already run
    dataset_path = f"{main_dir}/analysis/filesets/{args.year}_nanov12.yaml"
    with open(dataset_path, "r") as f:
        dataset_config = yaml.safe_load(f)
    datasets = dataset_config.keys()

    run_done = []
    for sample in datasets:
        output_list = glob.glob(f"{args.output_path}/*/{sample}*.{args.output_format}")
        for f in output_list:
            run_done.append(f.split("/")[-1].replace(f".{args.output_format}", ""))
    total_run = len(run_done)

    # show (and optionally resubmit) missing jobs
    condor_files_keys = [
        f.split("/")[-1].replace(f"{args.processor}_", "").replace(".sub", "")
        for f in condor_files
    ]
    for f, sub in zip(condor_files_keys, condor_files):
        if f not in run_done:
            print(f)
            if args.resubmit:
                subprocess.run(["condor_submit", sub])

    print(f"Jobs to be run: {total_files}")
    print(f"Jobs already run: {total_run}")
    print(f"Missing jobs {total_files - total_run}:")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        choices=["ztomumu", "ztoee", "zzto4l", "hww", "zplusl", "zplusll"],
        help="processor to be used",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        choices=["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"],
        help="year of the data",
    )
    parser.add_argument(
        "--eos",
        action="store_true",
        help="Enable reading outputs from /eos",
    )
    parser.add_argument(
        "--resubmit",
        action="store_true",
        help="if True resubmit the jobs. if False only print the missing jobs",
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="format of output histograms",
    )
    args = parser.parse_args()
    main(args)