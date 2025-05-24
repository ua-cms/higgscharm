import json
import argparse
from coffea import processor
from coffea.util import save
from coffea.nanoevents import NanoAODSchema
from analysis.utils import write_root
from analysis.processors.base import BaseProcessor


def main(args):
    with open(args.partition_json) as f:
        partition_fileset = json.load(f)
    out = processor.run_uproot_job(
        partition_fileset,
        treename="Events",
        processor_instance=BaseProcessor(workflow=args.workflow, year=args.year),
        executor=processor.futures_executor,
        executor_args={"schema": NanoAODSchema, "workers": 4},
    )
    savepath = f"{args.output_path}/{args.dataset}"
    if args.output_format == "coffea":
        save(out, f"{savepath}.coffea")
    elif args.output_format == "root":
        write_root(out, savepath, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--workflow",
        dest="workflow",
        type=str,
        choices=["ztomumu", "ztoee", "zzto4l", "hww", "zplusl_os", "zplusl_ss", "zplusll_os", "zplusll_ss"],
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
        "-d",
        "--dataset",
        dest="dataset",
        type=str,
        help="dataset",
    )
    parser.add_argument(
        "--partition_json",
        dest="partition_json",
        type=str,
        help="json with partition dataset",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        help="output path",
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="format of output histogram",
    )
    args = parser.parse_args()
    main(args)
