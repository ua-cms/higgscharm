import argparse
import subprocess
from pathlib import Path
from analysis.filesets.utils import (
    fileset_checker,
    get_datasets_to_run_over,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--workflow",
        dest="workflow",
        required=True,
        type=str,
        choices=[
            f.stem for f in (Path.cwd() / "analysis" / "workflows").glob("*.yaml")
        ],
        help="workflow to run",
    )
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=str,
        choices=[
            "2016preVFP",
            "2016postVFP",
            "2017",
            "2018",
            "2022preEE",
            "2022postEE",
            "2023preBPix",
            "2023postBPix",
            "2024",
        ],
        help="dataset year",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=10,
        help="number of root files to include in each dataset partition (default 15)",
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
        choices=["coffea", "parquet"],
        help="format of output file",
    )
    parser.add_argument(
        "-m",
        "--memory",
        type=str,
        default="4000",
        help="Requested memory (in MB) for the condor job",
    )
    parser.add_argument(
        "-e",
        "--executor",
        dest="executor",
        type=str,
        default="futures",
        help="executor to run the processor",
    )

    args = parser.parse_args()

    # get datasets to run over for selected workflow and year
    datasets_to_run_over = get_datasets_to_run_over(args.workflow, args.year)

    # check if the input fileset for the given year exists, generate it otherwise
    fileset_checker(samples=datasets_to_run_over, year=args.year, update_sites=True)

    # submit (or prepare) a job for each dataset using the given arguments
    cmd = ["python3", "submit_condor.py"]
    for dataset in datasets_to_run_over:
        cmd_args = [
            "--workflow",
            args.workflow,
            "--year",
            args.year,
            "--dataset",
            dataset,
            "--nfiles",
            str(args.nfiles),
            "--output_format",
            args.output_format,
            "--memory",
            args.memory,
            "--executor",
            args.executor,
        ]
        if args.submit:
            cmd_args.append("--submit")

        subprocess.run(cmd + cmd_args)
