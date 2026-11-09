import argparse
import subprocess
from pathlib import Path
from analysis.filesets.utils import get_nano_version

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
    )
    parser.add_argument(
        "--samples",
        nargs="*",
        type=str,
        help="(Optional) List of samples to use. If omitted, all available samples will be used",
    )
    parser.add_argument("--update_sites", action="store_true", help="Update sites file")
    args = parser.parse_args()

    try:
        subprocess.run("voms-proxy-info -exists", shell=True, check=True)
    except subprocess.CalledProcessError:
        raise Exception(
            "VOMS proxy expired or non-existing: please run 'voms-proxy-init --voms cms'"
        )

    sites_file = Path.cwd() / "analysis" / "filesets" / f"{args.year}_sites.yaml"
    if not sites_file.exists() or args.update_sites:
        cmd = f"python3 analysis/filesets/build_sites.py --year {args.year}"
        subprocess.run(cmd, shell=True)

    samples_str = " ".join(args.samples) if args.samples else ""
    cmd = f"python3 -m analysis.filesets.make_filesets --year {args.year} --samples {samples_str}"
    subprocess.run(cmd, shell=True)
