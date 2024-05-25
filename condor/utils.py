import os
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
    
    # set jobname
    jobname = f'{args["processor"]}_{args["dataset_name"]}'
    if "nfile" in args:
        jobname += f'_{args["nfile"]}'

    # create logs directory
    log_dir = condor_dir / "logs" / args["processor"] / args["year"] / args["dataset_name"]
    if not log_dir.exists():
        log_dir.mkdir(parents=True)
        
    # creal local condor submit file
    exe_dir = condor_dir / args["processor"] / args["year"]
    if not exe_dir.exists():
        exe_dir.mkdir(parents=True)
    local_condor = f"{exe_dir}/{jobname}.sub"
    
    # make condor file
    condor_template_file = open(f"{condor_dir}/submit.sub")
    condor_file = open(local_condor, "w")
    for line in condor_template_file:
        line = line.replace("EXECUTABLEPATH", str(exe_dir))
        line = line.replace("LOGPATH", str(log_dir))
        line = line.replace("JOBNAME", jobname)
        line = line.replace("JOBFLAVOR", f'"longlunch"')
        condor_file.write(line)
    condor_file.close()
    condor_template_file.close()

    # make executable file
    x509_path = move_X509() 
    sh_template_file = open(f"{condor_dir}/submit.sh")
    local_sh = f"{exe_dir}/{jobname}.sh"
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