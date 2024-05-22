import json
import dask
import gzip
import pickle
import argparse
import awkward as ak
import dask_awkward as dak
from coffea.nanoevents import PFNanoAODSchema
from analysis.processors.tag_eff import TaggingEfficiencyProcessor
from analysis.processors.signal import SignalProcessor
from analysis.processors.taggers import JetTaggersPlots
from analysis.processors.zplusjet import ZPlusJetProcessor
from coffea.dataset_tools import preprocess, apply_to_fileset, max_chunks


def main(args):
    processors = {
        "signal": SignalProcessor(),
        "tag_eff": TaggingEfficiencyProcessor(
            tagger=args.tagger,
            flavor=args.flavor,
            wp=args.wp,
        ),
        "taggers": JetTaggersPlots(),
        "zplusjet": ZPlusJetProcessor(year=args.year, config=args.config),
    }
    # check if preprocess file exist, otherwise preprocees fileset
    if args.dataset_runnable:
        dataset_runnable = args.dataset_runnable
    else:
        dataset_runnable, _ = preprocess(
            args.partition_fileset,
            step_size=args.stepsize,
            align_clusters=False,
            files_per_batch=1,
            save_form=False,
        )
        dataset_runnable[args.dataset_name]["metadata"] = {
            "metadata": {
                "era": args.partition_fileset[args.dataset_name]["metadata"]["metadata"]["era"]
            }
        # save preprocess output
        with gzip.open(
            f"{args.preprocess_file_path}/{args.dataset_name}.json.gz", "wt"
        ) as f:
            f.write(json.dumps(dataset_runnable))
            
    # process fileset
    to_compute = apply_to_fileset(
        processors[args.processor],
        max_chunks(dataset_runnable),
        schemaclass=PFNanoAODSchema,
    )
    (computed,) = dask.compute(to_compute)
    # save output to a pickle file
    save_path = f"{args.output_path}/{args.dataset_name}"
    with open(f"{save_path}.pkl", "wb") as handle:
        pickle.dump(computed, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="tag_eff",
        help="processor to be used {signal, tag_eff, taggers, zplusjet}",
    )
    parser.add_argument(
        "--dataset_name",
        dest="dataset_name",
        type=str,
        default="",
        help="dataset name",
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
        help="year of the data {2022EE}",
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
    parser.add_argument(
        "--stepsize",
        dest="stepsize",
        type=int,
        default=None,
        help="stepsize",
    )
    parser.add_argument(
        "--config",
        dest="config",
        type=json.loads,
        help="config file with processor parameters",
    )
    parser.add_argument(
        "--dataset_runnable",
        dest="dataset_runnable",
        type=json.loads,
        help="dataset runnable",
    )
    parser.add_argument(
        "--preprocess_file_path",
        dest="preprocess_file_path",
        type=str,
        help="preprocess_file_path",
    )
    args = parser.parse_args()
    main(args)