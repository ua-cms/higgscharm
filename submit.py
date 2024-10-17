import json
import dask
import pickle
import argparse
from coffea.nanoevents import PFNanoAODSchema
from analysis.processors.ztomumu import ZToMuMuProcessor
from coffea.dataset_tools import preprocess, apply_to_fileset, max_chunks


def main(args):
    # build dataset runnable (preprocessed fileset)
    dataset_runnable, _ = preprocess(
        args.partition_fileset,
        step_size=args.stepsize,
        align_clusters=False,
        files_per_batch=1,
        save_form=False,
    )
    dataset_runnable[args.dataset]["metadata"] = {
        "metadata": {
            "era": args.partition_fileset[args.dataset]["metadata"]["metadata"]["era"]
        }
    }
    # process dataset runnable and save output to a pickle file
    processors = {
        "ztomumu": ZToMuMuProcessor(year=args.year),
    }
    to_compute = apply_to_fileset(
        processors[args.processor],
        max_chunks(dataset_runnable),
        schemaclass=PFNanoAODSchema,
    )
    (computed,) = dask.compute(to_compute)

    save_path = f"{args.output_path}/{args.year}_{args.dataset}"
    with open(f"{save_path}.pkl", "wb") as handle:
        pickle.dump(computed, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztoll",
        help="processor to be used {ztoll}",
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
        help="year of the data {2022, 2022EE}",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        help="output path",
    )
    parser.add_argument(
        "--stepsize",
        dest="stepsize",
        type=str,
        default="50000",
        help="stepsize",
    )
    args = parser.parse_args()
    main(args)
