import json
import dask
import pickle
import argparse
from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema
from analysis.processors.tag_eff import TaggingEfficiencyProcessor
from analysis.processors.signal import SignalProcessor


def main(args):
    # load fileset and get PFNano events array
    with open(args.fileset_path) as f:
        fileset = json.load(f)[args.dataset_name]
    events = NanoEventsFactory.from_root(
        fileset["files"],
        schemaclass=PFNanoAODSchema,
        metadata={"dataset": args.dataset_name},
    ).events()

    # set processor
    processors = {
        "signal": SignalProcessor(),
        "tag_eff": TaggingEfficiencyProcessor(
            tagger=args.tagger, 
            flavor=args.flavor,
            wp=args.wp, 
        ),
    }
    p = processors[args.processor]

    # compute and save output
    to_compute = p.process(events)
    (computed,) = dask.compute(to_compute)
    with open(
        f"{args.output_path}/{args.dataset_name}.pkl", "wb"
    ) as handle:
        pickle.dump(computed, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="tag_eff",
        help="processor to be used {tag_eff}",
    )
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="",
        help="dataset name",
    )
    parser.add_argument(
        "--fileset_path",
        dest="fileset_path",
        type=str,
        default="",
        help="fileset path",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="",
        help="year of the data {2022EE, 2022, 2023}",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        default="",
        help="output path",
    )
    parser.add_argument(
        "--tagger",
        dest="tagger",
        type=str,
        default="pnet",
        help="tagger {pnet, part, deepjet}",
    )
    parser.add_argument(
        "--wp",
        dest="wp",
        type=str,
        default="tight",
        help="working point {loose, medium, tight}",
    )
    parser.add_argument(
        "--flavor",
        dest="flavor",
        type=str,
        default="c",
        help="Hadron flavor {c, b}",
    )
    args = parser.parse_args()
    main(args)