import argparse
import subprocess
from pathlib import Path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        choices=["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"],
    )
    parser.add_argument(
        "--image",
        dest="image",
        type=str,
        default="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest-py3.10",
    )
    args = parser.parse_args()

    try:
        subprocess.run("voms-proxy-info -exists", shell=True, check=True)
    except subprocess.CalledProcessError:
        raise Exception(
            "VOMS proxy expired or non-existing: please run 'voms-proxy-init --voms cms'"
        )

    sites_file = Path.cwd() / "analysis" / "filesets" / f"{args.year}_sites.yaml"
    if not sites_file.exists():
        cmd = f"python3 analysis/filesets/build_sites.py --year {args.year}"
        subprocess.run(cmd, shell=True)

    cmd = f"singularity exec -B /afs -B /cvmfs {args.image} python3 analysis/filesets/make_filesets.py --year {args.year}"
    subprocess.run(cmd, shell=True)
