import argparse
import subprocess
from pathlib import Path


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
    years = [
        "2016preVFP",
        "2016postVFP",
        "2016",
        "2017",
        "2018",
        "2022preEE",
        "2022postEE",
        "2022",
        "2023preBPix",
        "2023postBPix",
        "2023",
        "2024",
        "2025",
    ]
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=str,
        choices=years + ["all"],
        default="all",
    )
    parser.add_argument(
        "--nopostprocess", action="store_true", help="Skip postprocessing step"
    )
    parser.add_argument("--noplot", action="store_true", help="Skip ploting step")
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "parquet"],
        help="Format of output files",
    )
    args = parser.parse_args()

    for year in years:
        if args.year != "all":
            if args.year != year:
                continue

        if args.workflow in ["ztoee", "ztomumu"]:
            if not args.nopostprocess:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--postprocess",
                        "--output_format",
                        args.output_format,
                    ]
                )
            if not args.noplot:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                    ]
                )
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                        "--group_by",
                        '{"name": "leadingjet_flavour", "label": {"usdg": 0, "c": 4, "b": 5}}',
                    ]
                )

        elif args.workflow.startswith("zplusl_"):
            if not args.nopostprocess:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--postprocess",
                        "--output_format",
                        args.output_format,
                    ]
                )
            if not args.noplot:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                    ]
                )
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                        "--pass_axis",
                        "is_passing_lepton",
                        "--yratio_limits",
                        "0",
                        "2",
                    ]
                )

        elif args.workflow in ["zplusll_os", "zplusll_ss"]:
            if not args.nopostprocess:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--postprocess",
                        "--output_format",
                        args.output_format,
                    ]
                )
            if not args.noplot:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--yratio_limits",
                        "0",
                        "2",
                    ]
                )
        elif args.workflow in ["zzto4l"]:
            if not args.nopostprocess:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--postprocess",
                        "--output_format",
                        args.output_format,
                    ]
                )
            if not args.noplot:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--yratio_limits",
                        "0",
                        "2",
                    ]
                )
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                        "--group_by",
                        '{"name": "leadingjet_flavour", "label": {"usdg": 0, "c": 4, "b": 5}}',
                        "--yratio_limits",
                        "0",
                        "2",
                    ]
                )
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                        "--group_by",
                        '{"name": "subleadingjet_flavour", "label": {"usdg": 0, "c": 4, "b": 5}}',
                        "--yratio_limits",
                        "0",
                        "2",
                    ]
                )
        else:
            if not args.nopostprocess:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--postprocess",
                        "--output_format",
                        args.output_format,
                    ]
                )
            if not args.noplot:
                subprocess.run(
                    [
                        "python3",
                        "run_postprocess.py",
                        "-w",
                        args.workflow,
                        "-y",
                        year,
                        "--plot",
                        "--log",
                    ]
                )
