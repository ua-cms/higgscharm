import glob
import yaml
import argparse
import subprocess
from pathlib import Path


def main(args):
    """Helper function to resubmit condor jobs"""
    main_dir = Path.cwd()
    outputs_path = f"{args.output_path}/{args.processor}/{args.year}"
    print(f"Reading outputs from: {outputs_path}")

    # get jobs to be run
    condor_path = f"{main_dir}/condor/{args.processor}/{args.year}"
    condor_files = glob.glob(f"{condor_path}/*.sub", recursive=True)
    total_files = len(condor_files)

    # get jobs already run
    dataset_path = f"{main_dir}/analysis/filesets/{args.year}_fileset.yaml"
    with open(dataset_path, "r") as f:
        dataset_config = yaml.safe_load(f)
    datasets = dataset_config.keys()

    run_done = []
    for sample in datasets:
        output_list = glob.glob(f"{outputs_path}/*{sample}*.pkl")
        for f in output_list:
            run_done.append(
                f.split("/")[-1].replace(".pkl", "").replace(f"{args.year}_", "")
            )
    total_run = len(run_done)

    # show (and optionally resubmit) missing jobs
    condor_files_keys = [
        f.split("/")[-1].replace(f"{args.processor}_", "").replace(".sub", "")
        for f in condor_files
    ]
    for f, sub in zip(condor_files_keys, condor_files):
        if f not in run_done:
            print(f)
            if args.resubmit == "True":
                subprocess.run(["condor_submit", sub])

    print(f"Jobs to be run: {total_files}")
    print(f"Jobs already run: {total_run}")
    print(f"Missing jobs {total_files - total_run}:")


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--resubmit",
        dest="resubmit",
        type=str,
        default="False",
        help="if True resubmit the jobs. if False only print the missing jobs",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        # default="/pnfs/iihe/cms/store/user/<your_username>/higgscharm_outputs",
        help="path to the outputs folder",
    )
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztoee",
        help="processor to be used {ztomumu, ztoee}",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022postEE",
        help="year of the data {2022preEE, 2022postEE}",
    )
    args = parser.parse_args()
    main(args)
