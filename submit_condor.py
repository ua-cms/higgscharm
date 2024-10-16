import os
import glob
import json
import gzip
import pathlib
import argparse
import subprocess
from copy import deepcopy
from analysis.utils import paths
from condor.utils import submit_condor
from analysis.configs import load_config
from analysis.filesets.utils import build_single_fileset, divide_list


def main(args):
    args = vars(args)
    # set output path
    processor_output_path = paths.processor_path(
        processor=args["processor"],
        tagger=args["tagger"],
        flavor=args["flavor"],
        wp=args["wp"],
        year=args["year"],
        lepton_flavor=args["lepton_flavor"],
    )
    args["output_path"] = str(processor_output_path)

    # split dataset into batches
    dataset_name = args["dataset_name"]
    fileset = build_single_fileset(name=dataset_name, year=args["year"])
    root_files = list(fileset[dataset_name]["files"].keys())
    root_files_list = divide_list(root_files)
    print(fileset)
    # run over batches
    for i, partition in enumerate(root_files_list, start=1):
        partition_fileset = deepcopy(fileset)
        if len(root_files_list) > 1:
            partition_fileset[f"{dataset_name}_{i}"] = partition_fileset[dataset_name]
            del partition_fileset[dataset_name]
            partition_fileset[f"{dataset_name}_{i}"]["files"] = {
                p: "Events" for p in partition
            }
        dataset_runnable_key = [key for key in partition_fileset][0]

        # set condor and submit args
        args["dataset_name"] = dataset_runnable_key
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--year {args['year']} "
            f"--lepton_flavor {args['lepton_flavor']} "
            f"--output_path {args['output_path']} "
            f"--dataset_name {dataset_runnable_key} "
            f"--tagger {args['tagger']} "
            f"--wp {args['wp']} "
            f"--flavor {args['flavor']} "
            # dictionaries must be passed as a string enclosed in single quotes,
            # with strings within the dictionary enclosed in double quotes.
            # we use json.dumps() to switch from single to double quotes within the dictionary
            f"--partition_fileset '{json.dumps(partition_fileset)}' "
        )
        args["cmd"] += f"--stepsize {args['stepsize']} "
        submit_condor(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="",
        help="processor to be used {signal, tag_eff, taggers, zplusjet}",
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
        default="2022EE",
        help="year of the data {2022EE}",
    )
    parser.add_argument(
        "--lepton_flavor",
        dest="lepton_flavor",
        type=str,
        default="muon",
        help="lepton flavor {muon, electron}",
    )
    parser.add_argument(
        "--tagger",
        dest="tagger",
        type=str,
        default=None,
        help="tagger {pnet, part, deepjet}",
    )
    parser.add_argument(
        "--wp",
        dest="wp",
        type=str,
        default=None,
        help="working point {loose, medium, tight}",
    )
    parser.add_argument(
        "--flavor",
        dest="flavor",
        type=str,
        default=None,
        help="Hadron flavor {c, b}",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        type=str,
        default=None,
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
