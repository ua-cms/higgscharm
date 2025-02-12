import json
import time
import uproot
import pickle
import argparse
from coffea import processor
from humanfriendly import format_timespan
from coffea.nanoevents import NanoAODSchema
from analysis.configs import ProcessorConfigBuilder
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
    # save metadata (sumw, cutflow, ...)
    save_path = f"{args.output_path}/{args.dataset}"
    with open(f"{save_path}.pkl", "wb") as handle:
        pickle.dump(out["metadata"], handle, protocol=pickle.HIGHEST_PROTOCOL)
    # save histograms
    config_builder = ProcessorConfigBuilder(processor=args.processor, year=args.year)
    processor_config = config_builder.build_processor_config()
    categories = processor_config.event_selection["categories"]
    histograms = out["histograms"]
    with uproot.recreate(f"{save_path}.root") as f:
        for category in categories:
            for histogram in histograms.values():
                category_histogram = histogram[{"category": category}]
                variables = [
                    v for v in category_histogram.axes.name if v != "variation"
                ]
                for variable in variables:
                    for syst_var in category_histogram.axes["variation"]:
                        variation_histogram = category_histogram[
                            {"variation": syst_var}
                        ]
                        f[f"{category}_{variable}_{syst_var}"] = (
                            variation_histogram.project(variable)
                        )


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
    args = parser.parse_args()
    main(args)