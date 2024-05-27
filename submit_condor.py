import os
import glob
import json
import gzip
import argparse
import subprocess
from pathlib import Path
from copy import deepcopy
from condor.utils import submit_condor
from analysis.configs import load_config
from analysis.filesets.utils import build_single_fileset, divide_list


def main(args):
    args = vars(args)
    
    # set output path
    if args["output_path"]:
        output_path = Path(args["output_path"])
    else:
        cwd = Path.cwd()
        output_path = cwd 
    output_path = output_path / "outputs" / args["processor"] / args["year"]
    if args["processor"] == "tag_eff":
        output_path = output_path / args["tagger"] / args["flavor"] / args["wp"]
    if not output_path.exists():
        output_path.mkdir(parents=True)
    args["output_path"] = str(output_path)
        
    # split dataset into batches
    dataset_config = load_config(
        config_type="dataset", config_name=args["dataset_name"], year=args["year"]
    )
    fileset = build_single_fileset(name=dataset_config.name, year=dataset_config.year)
    root_files = list(fileset[dataset_config.name]["files"].keys())
    root_files_list = divide_list(root_files, dataset_config.partitions)

    # run over batches
    for i, partition in enumerate(root_files_list, start=1):
        partition_fileset = deepcopy(fileset)
        if dataset_config.partitions > 1:
            partition_fileset[f"{dataset_config.name}_{i}"] = partition_fileset[
                dataset_config.name
            ]
            del partition_fileset[dataset_config.name]
            partition_fileset[f"{dataset_config.name}_{i}"]["files"] = {
                p: "Events" for p in partition
            }
        dataset_runnable_key = [key for key in partition_fileset][0]
                
        # set condor and submit args
        args["dataset_name"] = dataset_runnable_key
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--year {args['year']} "
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
        if dataset_config.stepsize:
            args["cmd"] += f"--stepsize {dataset_config.stepsize} "
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
        "--output_path",
        dest="output_path",
        type=str,
        default=None,
        help="output path",
    )
    args = parser.parse_args()
    main(args)