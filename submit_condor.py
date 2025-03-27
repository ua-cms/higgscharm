import os
import json
import argparse
import subprocess
from pathlib import Path
from analysis.filesets.utils import divide_list
from analysis.utils import make_output_directory


def move_X509() -> str:
    """move x509 proxy file from /tmp to /afs/private. Returns the afs path"""
    try:
        x509_localpath = (
            [
                line
                for line in os.popen("voms-proxy-info").read().split("\n")
                if line.startswith("path")
            ][0]
            .split(":")[-1]
            .strip()
        )
    except Exception as err:
        raise RuntimeError(
            "x509 proxy could not be parsed, try creating it with 'voms-proxy-init --voms cms'"
        ) from err
    user = os.environ["USER"]
    x509_path = (
        f"/afs/cern.ch/user/{user[0]}/{user}/private/{x509_localpath.split('/')[-1]}"
    )
    subprocess.run(["cp", x509_localpath, x509_path])
    return x509_path


def submit_condor(args):
    """Build condor and executable files. Submit condor job"""
    print(f"Creating {args.processor}/{args.dataset}/{args.year} condor files")
    jobname = f"{args.processor}_{args.dataset}"

    # make condor and log directories
    condor_dir = Path.cwd() / "condor"
    job_dir = condor_dir / args.processor / args.year / args.dataset
    if not job_dir.exists():
        job_dir.mkdir(parents=True, exist_ok=True)

    log_dir = condor_dir / "logs" / args.processor / args.year / args.dataset
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)

    # save partitions json and jobnums to job directory
    jobnum_list = []
    partition_dataset = {}
    fileset_path = Path.cwd() / "analysis" / "filesets"
    with open(f"{fileset_path}/fileset_{args.year}_NANO_lxplus.json", "r") as f:
        root_files = json.load(f)[args.dataset]
    root_files_list = divide_list(root_files, args.nfiles)
    for i in range(len(root_files_list)):
        dataset_key = (
            f"{args.dataset}_{i+1}" if len(root_files_list) > 1 else args.dataset
        )
        partition_dataset[i + 1] = {dataset_key: root_files_list[i]}
        jobnum_list.append(i + 1)

    partition_file = job_dir / "partitions.json"
    with open(f"{partition_file}", "w") as json_file:
        json.dump(partition_dataset, json_file, indent=4)

    jobnum_file = job_dir / "jobnum.txt"
    with open(f"{jobnum_file}", "w") as f:
        print(*jobnum_list, sep="\n", file=f)

    # build and save arguments json
    args.output_path = make_output_directory(args)
    args_file = job_dir / "arguments.json"
    with open(args_file, "w") as json_file:
        json.dump(vars(args), json_file, indent=4)

    # make condor file
    local_condor = f"{job_dir}/{jobname}.sub"
    with open(f"{condor_dir}/submit.sub") as condor_template_file, open(
        local_condor, "w"
    ) as condor_file:
        for line in condor_template_file:
            line = line.replace("CONDORDIR", str(condor_dir))
            line = line.replace("MAINDIRECTORY", str(Path.cwd()))
            line = line.replace("X509PATH", move_X509())
            line = line.replace("LOGDIR", str(log_dir))
            line = line.replace("JOBNAME", jobname)
            line = line.replace(
                "INPUTFILES", f"{partition_file},{jobnum_file},{args_file}"
            )
            line = line.replace("JOBNUM_FILE", str(jobnum_file))
            condor_file.write(line)

    if args.submit:
        subprocess.run(["condor_submit", local_condor])


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
        default=15,
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
    submit_condor(args)
