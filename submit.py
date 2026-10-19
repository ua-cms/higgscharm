import json
import argparse
from pathlib import Path
from coffea import processor
from coffea.util import save
from analysis.processors.base import BaseProcessor
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

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
        choices=["coffea", "parquet"],
        help="format of output histogram",
    )
    parser.add_argument(
        "--user",
        type=str,
        help="User name",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="change the number of workers to process the analysis",
    )

    args = parser.parse_args()

    # set output location and partition key (used when --output_format parquet)
    output_location = f"root://eosuser.cern.ch//eos/user/{args.user[0]}/{args.user}/higgscharm_outputs/"
    if args.output_format == "parquet":
        factory = NanoEventsFactory.from_root(
            partition_fileset,
            schemaclass=NanoAODSchema,
            entry_stop=1,
            mode="virtual",
        )
        partition_key = factory._partition_key

    # load partition_fileset
    with open(args.partition_json) as f:
        partition_fileset = json.load(f)

    # Configure the Runner using coffea 2024+ API
    futures_run = processor.Runner(
        executor=processor.FuturesExecutor(
            workers=args.workers, compression=None, retries=6
        ),
        schema=NanoAODSchema,
        # chunksize=100000,
        savemetrics=False,
        xrootdtimeout=120,
        align_clusters=True,
        skipbadfiles=False,
    )
    # Execute processing job using the Runner instance
    out = futures_run(
        partition_fileset,
        treename="Events",
        processor_instance=BaseProcessor(
            workflow=args.workflow,
            year=args.year,
            output_format=args.output_format,
            output_location=output_location,
            partition_key=partition_key,
            mode="virtual",
        ),
    )
    savepath = f"{args.output_path}/{args.dataset}"
    if args.output_format in ["coffea", "parquet"]:
        save(out, f"{savepath}.coffea")
