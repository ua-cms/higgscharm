import yaml
import json
import argparse
import subprocess
from glob import glob
from pathlib import Path
from analysis.utils import make_output_directory
from analysis.filesets.xrootd_sites import xroot_to_site
from analysis.filesets.utils import divide_list, modify_site_list, extract_xrootd_errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--workflow",
        dest="workflow",
        type=str,
        choices=["ztomumu", "ztoee", "zzto4l", "hww", "zplusl", "zplusll"],
        help="workflow config to run",
    )
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=str,
        choices=["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"],
        help="dataset year",
    )
    parser.add_argument(
        "--eos", action="store_true", help="Enable reading outputs from /eos"
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="Format of output histograms",
    )
    args = parser.parse_args()

    output_dir = make_output_directory(args)
    print(f"Reading outputs from: {output_dir}")

    base_dir = Path.cwd()
    condor_dir = base_dir / "condor"
    job_dir = condor_dir / args.workflow / args.year
    log_dir = condor_dir / "logs" / args.workflow / args.year
    fileset_path = base_dir / "analysis" / "filesets"

    datasets = [item.name for item in job_dir.iterdir() if item.is_dir()]
    jobnum, jobnum_done = {}, {}
    error_file = []
    for dataset in datasets:
        jobnum[dataset] = [
            line.strip() for line in open(job_dir / dataset / "jobnum.txt")
        ]
        output_files = glob(f"{output_dir}/{dataset}/*.{args.output_format}")
        jobnum_done[dataset] = [
            Path(f).stem.replace(f"{dataset}_", "") for f in output_files
        ]
        error_file.extend(glob(f"{log_dir}/{dataset}/*.err"))

    xrootd_errs = extract_xrootd_errors(error_file)
    jobnum_missing = {
        d: sorted(set(jobnum[d]) - set(jobnum_done.get(d, []))) for d in jobnum
    }
    total = sum(len(v) for v in jobnum.values())
    done = sum(len(jobnum_done[d]) for d in jobnum_done)
    missing = sum(len(v) for v in jobnum_missing.values())
    datasets_with_missing_jobs = [d for d in jobnum_missing if jobnum_missing[d]]

    print(
        f"Jobs to be run: {total}\nJobs already run: {done}\nMissing jobs: {missing}\n"
    )

    if missing:
        print(
            f"Datasets with missing jobs ({len(datasets_with_missing_jobs)}/{len(jobnum.keys())}): {datasets_with_missing_jobs}"
        )
        if len(xrootd_errs) > 0:
            site_errs = [
                xroot_to_site[err] for err in xrootd_errs if err in xroot_to_site
            ]
            for err in xrootd_errs:
                if err not in xroot_to_site:
                    print(f"Could not identify the site for xrootd error {err}")
            print(f"Sites with xrootd errors: {site_errs}")

            if input("Update datasets? (y/n): ").lower() in ["y", "yes"]:
                for site in site_errs:
                    modify_site_list(site, "black")

                cmd = f"python3 fetch.py --year {args.year}"
                subprocess.run(cmd, shell=True)

                for dataset in datasets_with_missing_jobs:
                    # get root files to split
                    fileset_file = (
                        fileset_path / f"fileset_{args.year}_NANO_lxplus.json"
                    )
                    with open(fileset_file, "r") as f:
                        root_files = json.load(f)[dataset]

                    # get number of partitions
                    args_json = job_dir / dataset / "arguments.json"
                    with open(args_json, "r") as f:
                        nfiles = json.load(f)["nfiles"]

                    # get new dataset partitions
                    root_files_list = divide_list(root_files, nfiles)
                    partition_dataset = {
                        i
                        + 1: {
                            (
                                f"{dataset}_{i+1}"
                                if len(root_files_list) > 1
                                else dataset
                            ): chunk
                        }
                        for i, chunk in enumerate(root_files_list)
                    }
                    partition_file = job_dir / dataset / "partitions.json"
                    with open(partition_file, "w") as json_file:
                        json.dump(partition_dataset, json_file, indent=4)

        if input("Update and resubmit condor files? (y/n): ").lower() in ["y", "yes"]:
            to_resubmit = []
            for dataset in datasets_with_missing_jobs:
                missing_file = job_dir / "missing.txt"
                with open(missing_file, "w") as f:
                    print(*jobnum_missing[dataset], sep="\n", file=f)

                condor_file = job_dir / dataset / f"{args.workflow}_{dataset}.sub"
                subprocess.run(
                    [
                        "cp",
                        str(condor_file),
                        str(condor_file).replace(".sub", "_all.sub"),
                    ]
                )
                with open(condor_file, "r") as file:
                    submit_file = file.read().replace("jobnum.txt", "missing.txt")
                with open(condor_file, "w") as file:
                    file.write(submit_file)
                print(f"condor file {args.workflow}/{args.year}/{dataset} updated")
                to_resubmit.append(str(condor_file))

            for submit_file in to_resubmit:
                subprocess.run(["condor_submit", submit_file], shell=True)
