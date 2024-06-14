import os
import pathlib
import subprocess

def submit_condor(args: dict) -> None:
    """build condor and executable files. Submit condor job"""
    main_dir = pathlib.Path.cwd()
    condor_dir = pathlib.Path(f"{main_dir}/condor")
    
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
        condor_file.write(line)
    condor_file.close()
    condor_template_file.close()

    # make executable file
    sh_template_file = open(f"{condor_dir}/to_submit.sh")
    local_sh = f"{exe_dir}/to_run_{jobname}.sh"
    sh_file = open(local_sh, "w")
    for line in sh_template_file:
        line = line.replace("MAINDIRECTORY", str(main_dir))
        line = line.replace("COMMAND", args["cmd"])
        sh_file.write(line)
    sh_file.close()
    sh_template_file.close()
    
    # make executable file runner
    sh_runner_template_file = open(f"{condor_dir}/submit.sh")
    local_sh = f"{exe_dir}/{jobname}.sh"
    sh_file = open(local_sh, "w")
    for line in sh_runner_template_file:
        line = line.replace("EXECUTABLEPATH", str(exe_dir))
        line = line.replace("JOBNAME", jobname)
        sh_file.write(line)
    sh_file.close()
    sh_template_file.close()
    
    # submit jobs
    print(f"submitting {jobname}")
    #subprocess.run(["condor_submit", local_condor])