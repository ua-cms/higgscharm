import os
import json
import argparse
import subprocess
from pathlib import Path
from analysis.utils import make_output_directory
from analysis.filesets.utils import divide_list, get_nano_version, fileset_checker


def move_proxy() -> str:
    """move x509 proxy file from /tmp to /afs/private. Returns the afs path"""
    try:
        subprocess.run("voms-proxy-info -exists -valid 0:20", shell=True, check=True)
    except subprocess.CalledProcessError:
        raise Exception(
            "VOMS proxy expired or non-existing: please run 'voms-proxy-init --voms cms'"
        )
    user = os.environ["USER"]
    x509_localpath = subprocess.check_output(
        "voms-proxy-info -path", shell=True, text=True
    ).strip()
    x509_path = (
        f"/afs/cern.ch/user/{user[0]}/{user}/private/{x509_localpath.split('/')[-1]}"
    )
    subprocess.run(["cp", x509_localpath, x509_path])
    return x509_path


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
        "-d",
        "--dataset",
        dest="dataset",
        type=str,
        help="dataset",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=10,
        help="number of root files to include in each dataset partition (default 10)",
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
        help="format of output histogram",
    )
    parser.add_argument("--jobflavor", dest="jobflavor", type=str, default="longlunch")
    parser.add_argument(
        "-m",
        "--memory",
        type=str,
        default="2000",
        help="Requested memory (in MB) for the condor job",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=4,
        help="change the number of workers to process the analysis",
    )
    args = parser.parse_args()

    # check if the fileset for the given year exists, generate it otherwise
    fileset_checker(samples=[args.dataset], year=args.year)

    print(f"Creating {args.workflow}-{args.year}-{args.dataset} condor file")
    jobname = f"{args.workflow}_{args.dataset}"

    # make condor and log directories
    condor_dir = Path.cwd() / "condor"
    job_dir = condor_dir / args.workflow / args.year / args.dataset
    if not job_dir.exists():
        job_dir.mkdir(parents=True, exist_ok=True)

    log_dir = condor_dir / "logs" / args.workflow / args.year / args.dataset
    if not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)

    # save partitions json and jobnums to job directory
    jobnum_list = []
    partition_dataset = {}
    nano_version = get_nano_version(args.year)
    fileset_dir = Path.cwd() / "analysis" / "filesets"
    fileset_file = fileset_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"
    with open(fileset_file) as f:
        fileset = json.load(f)

    short_name_map = {}
    for sample in fileset:
        short_name = fileset[sample]["metadata"]["short_name"]
        short_name_map[short_name] = sample

    # extract the dictionary of files {"path.root": "Events"} and the metadata
    dataset = fileset[short_name_map[args.dataset]]
    files_dict = dataset["files"]
    metadata = dataset.get("metadata", {})

    # convert the items to a list so we can divide them with divide_list
    file_items = list(files_dict.items())
    file_chunks = divide_list(file_items, args.nfiles)

    for i, chunk in enumerate(file_chunks, start=1):
        dataset_key = f"{args.dataset}_{i}" if len(file_chunks) > 1 else args.dataset
        # reconstruct the structure for each individual job.
        partition_dataset[i] = {
            dataset_key: {"files": dict(chunk), "metadata": metadata}
        }
        jobnum_list.append(i)
    partition_file = job_dir / "partitions.json"
    with open(f"{partition_file}", "w") as json_file:
        json.dump(partition_dataset, json_file, indent=4)

    jobnum_file = job_dir / "jobnum.txt"
    with open(f"{jobnum_file}", "w") as f:
        print(*jobnum_list, sep="\n", file=f)

    # build and save arguments json
    args.output_path = make_output_directory(args)
    args.user = os.environ["USER"]
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
            line = line.replace("BASEDIR", str(Path.cwd()))
            line = line.replace("X509PATH", move_proxy())
            line = line.replace("LOGDIR", str(log_dir))
            line = line.replace("JOBNAME", jobname)
            line = line.replace(
                "INPUTFILES", f"{partition_file},{jobnum_file},{args_file}"
            )
            line = line.replace("JOBNUM_FILE", str(jobnum_file))
            line = line.replace("JOBFLAVOR", f'"{args.jobflavor}"')
            line = line.replace("MEMORY", args.memory)
            condor_file.write(line)

    if args.submit:
        subprocess.run(["condor_submit", local_condor])
