import json
import time
import pickle
import argparse
from coffea import processor
from humanfriendly import format_timespan
from coffea.nanoevents import NanoAODSchema
from analysis.processors.ztoee import ZToEEProcessor
from analysis.processors.ztomumu import ZToMuMuProcessor
from analysis.processors.zzto4l import ZZTo4LProcessor
from analysis.processors.hww import HWWProcessor


def main(args):
    processors = {
        "ztomumu": ZToMuMuProcessor(year=args.year),
        "ztoee": ZToEEProcessor(year=args.year),
        "zzto4l": ZZTo4LProcessor(year=args.year),
        "hww": HWWProcessor(year=args.year),
    }
    t0 = time.monotonic()
    out = processor.run_uproot_job(
        args.partition_fileset,
        treename="Events",
        processor_instance=processors[args.processor],
        executor=processor.futures_executor,
        executor_args={"schema": NanoAODSchema, "workers": 4},
    )
    exec_time = format_timespan(time.monotonic() - t0)
    print(f"Execution time: {exec_time}")
    save_path = f"{args.output_path}/{args.year}_{args.dataset}"
    with open(f"{save_path}.pkl", "wb") as handle:
        pickle.dump(out, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztomumu",
        help="processor to be used {ztomumu, ztoee}",
    )
    parser.add_argument(
        "--dataset",
        dest="dataset",
        type=str,
        default="",
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
        default="",
        help="year of the data {2022preEE, 2022postEE}",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        help="output path",
    )
    args = parser.parse_args()
    main(args)
