import json
import dask
import pickle
import argparse
from coffea.nanoevents import PFNanoAODSchema
from analysis.processors.tag_eff import TaggingEfficiencyProcessor
from coffea.dataset_tools import apply_to_fileset, max_chunks

def main(args):
    processors = {"tag_eff": TaggingEfficiencyProcessor}
    p = processors[args.processor](tagger=args.tagger, wp=args.wp, flavor=args.flavor)

    with open(args.dataset_runnable) as f:
        dataset_runnable = json.load(f)

    to_compute = apply_to_fileset(
        p,
        max_chunks(dataset_runnable, None),
        schemaclass=PFNanoAODSchema,
        #uproot_options={"allow_read_errors_with_report": True}
    )
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
        default="ctag_eff",
        help="processor to be used {ctag_eff}",
    )
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="",
        help="dataset name",
    )
    parser.add_argument(
        "--dataset_runnable",
        dest="dataset_runnable",
        type=str,
        default="",
        help="dataset runnable",
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