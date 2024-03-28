import dask
import pickle
import argparse
from analysis.processors.ctag_eff import CTaggingEfficiencyProcessor
from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema


def main(args):
    events = NanoEventsFactory.from_root(
        {args.sample: "Events"},
        schemaclass=PFNanoAODSchema,
        metadata={"dataset": args.dataset_name},
    ).events()

    processors = {"ctag_eff": CTaggingEfficiencyProcessor}
    p = processors[args.processor](tagger=args.tagger, wp=args.wp)
    out = p.process(events)
    (computed,) = dask.compute(out)

    with open(
        f"{args.output_path}/{args.dataset_name}_{args.nfile}.pkl", "wb"
    ) as handle:
        pickle.dump(computed, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ctag_eff",
        help="processor to be used {ctag_eff}",
    )
    parser.add_argument(
        "--sample",
        dest="sample",
        type=str,
        default="",
        help="sample key to be processed",
    )
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="",
        help="dataset_name",
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
        "--nfile",
        dest="nfile",
        type=str,
        default="",
        help="nfile",
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
    args = parser.parse_args()
    main(args)