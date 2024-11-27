import os
import pathlib
import subprocess
from pathlib import Path


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
    """build condor and executable files. Submit condor job"""
    main_dir = Path.cwd()
    condor_dir = Path(f"{main_dir}/condor")

    # set jobname and jobpath
    jobpath = f'{args["processor"]}/{args["year"]}/{args["dataset_key"]}'
    jobname = f'{args["processor"]}_{args["dataset_key"]}'
    print(f"creating condor files {jobname}")

    # create logs and condor directories
    log_dir = condor_dir / "logs" / jobpath
    local_condor_dir = condor_dir / jobpath
    if not log_dir.exists():
        log_dir.mkdir(parents=True)
    if not local_condor_dir.exists():
        local_condor_dir.mkdir(parents=True)
    local_condor = f"{local_condor_dir}/{jobname}.sub"
    
    # make condor file
    condor_template_file = open(f"{condor_dir}/submit.sub")
    condor_file = open(local_condor, "w")
    for line in condor_template_file:
        line = line.replace("DIRECTORY", str(condor_dir))
        line = line.replace("JOBPATH", jobpath)
        line = line.replace("JOBNAME", jobname)
        condor_file.write(line)
    condor_file.close()
    condor_template_file.close()

    # make executable file
    x509_path = move_X509()
    sh_template_file = open(f"{condor_dir}/submit.sh")
    local_sh = f"{local_condor_dir}/{jobname}.sh"
    sh_file = open(local_sh, "w")
    for line in sh_template_file:
        line = line.replace("X509PATH", x509_path)
        line = line.replace("MAINDIRECTORY", str(main_dir))
        line = line.replace("COMMAND", args["cmd"])
        sh_file.write(line)
    sh_file.close()
    sh_template_file.close()

    if args["submit"]:
        print(f"submitting condor job")
        subprocess.run(["condor_submit", local_condor])