import json
import argparse
from coffea import processor
from coffea.util import save
from coffea.nanoevents import NanoAODSchema
from analysis.utils import write_root
from analysis.processors.hww import HWWProcessor
from analysis.processors.ztoee import ZToEEProcessor
from analysis.processors.ztomumu import ZToMuMuProcessor
from analysis.processors.zzto4l import ZZTo4LProcessor


def main(args):
    # execute processor
    processors = {
        "ztomumu": ZToMuMuProcessor(year=args.year),
        "ztoee": ZToEEProcessor(year=args.year),
        "zzto4l": ZZTo4LProcessor(year=args.year),
        "hww": HWWProcessor(year=args.year),
    }
    out = processor.run_uproot_job(
        args.partition_fileset,
        treename="Events",
        processor_instance=processors[args.processor],
        executor=processor.futures_executor,
        executor_args={"schema": NanoAODSchema, "workers": 4},
    )
    savepath = f"{args.output_path}/{args.dataset}"
    if args.output_format == "coffea":
        save(out, f"{asavepath}.coffea")
    elif args.output_format == "root":
        write_root(out, savepath, args)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        help="processor to be used {ztomumu, ztoee, zzto4l, hww}",
    )
    parser.add_argument(
        "--dataset",
        dest="dataset",
        type=str,
        help="dataset",
    )
    parser.add_argument(
        "--partition_fileset",
        dest="partition_fileset",
        type=json.loads,
        help="partition_fileset needed to preprocess a fileset",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        help="year of the data {2022preEE, 2022postEE}",
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
        help="format of output histograms {root, coffea}",
    )
    args = parser.parse_args()
    main(args)