import os
import argparse
import subprocess
from pathlib import Path 
from analysis.configs import load_config
from analysis.filesets.build_fileset import build_full_dataset


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
    x509_path = f"{Path.home()}/private/{x509_localpath.split('/')[-1]}"
    subprocess.run(["cp", x509_localpath, x509_path])
    return x509_path


def submit_condor(args: dict) -> None:
    """build condor and executable files, and submit condor job"""
    main_dir = Path.cwd()
    condor_dir = Path(f"{main_dir}/condor")
    
    # set jobname
    jobname = f'{args["processor"]}_{args["dataset_name"]}_{args["nfile"]}'
    
    # create logs and condor directories
    log_dir = Path(f"{str(condor_dir)}/logs/{args['processor']}/{args['year']}")
    if not log_dir.exists():
        log_dir.mkdir(parents=True)
    local_condor_path = Path(f"{condor_dir}/{args['processor']}/{args['year']}")
    if not local_condor_path.exists():
        local_condor_path.mkdir(parents=True)                        
    local_condor = f"{local_condor_path}/{jobname}.sub"
    
    # make condor file
    condor_template_file = open(f"{condor_dir}/submit.sub")
    condor_file = open(local_condor, "w")
    for line in condor_template_file:
        line = line.replace("DIRECTORY", str(condor_dir))
        line = line.replace("JOBNAME", jobname)
        line = line.replace("PROCESSOR", args["processor"])
        line = line.replace("YEAR", args["year"])
        line = line.replace("JOBFLAVOR", f'"longlunch"')
        condor_file.write(line)
    condor_file.close()
    condor_template_file.close()

    # make executable file
    x509_path = move_X509()
    sh_template_file = open(f"{condor_dir}/submit.sh")
    local_sh = f"{local_condor_path}/{jobname}.sh"
    sh_file = open(local_sh, "w")
    for line in sh_template_file:
        line = line.replace("MAINDIRECTORY", str(main_dir))
        line = line.replace("COMMAND", args["cmd"])
        line = line.replace("X509PATH", x509_path)
        sh_file.write(line)
    sh_file.close()
    sh_template_file.close()

    # submit jobs
    print(f"submitting {jobname}")
    subprocess.run(["condor_submit", local_condor])
    
    
def main(args):
    args = vars(args)
    
    output_path = Path(Path.cwd() / "outputs" / args["processor"] / args["year"])
    if not output_path.exists():
        output_path.mkdir(parents=True)
    args["output_path"] = str(output_path)
    
    filesets = build_full_dataset(args["year"])

    for dataset_name in filesets:
        if args["dataset_name"]:
            if dataset_name != args["dataset_name"]:
                continue
        for i, root_file in enumerate(filesets[dataset_name]["files"]):
            args["dataset_name"] = dataset_name
            args["nfile"] = i
            
            args["cmd"] = (
                "python3 submit.py "
                f"--processor {args['processor']} "
                f"--year {args['year']} "
                f"--output_path {args['output_path']} "
                f"--dataset_name {dataset_name} "
                f"--sample {root_file} "
                f"--nfile {str(i)}"
            )
            submit_condor(args)
            
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ctag_eff",
        help="processor to be used {ctag_eff}",
    )
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="",
        help="dataset_name",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022EE, 2022, 2023}",
    )
    args = parser.parse_args()
    main(args)