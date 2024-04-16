import os
import argparse
import subprocess
from pathlib import Path 
from condor.utils import submit_condor
from analysis.configs import load_config
from analysis.filesets.utils import build_full_dataset


def main(args):
    args = vars(args)
    
    output_path = Path(Path.cwd() / "outputs" / args["processor"] / args["year"])
    if not output_path.exists():
        output_path.mkdir(parents=True)
    args["output_path"] = str(output_path)
    
    filesets = build_full_dataset(args["year"])

    for dataset_name in filesets:
        if args["dataset_name"]:
            if dataset_name != args["dataset_name"]:
                continue
        for i, root_file in enumerate(filesets[dataset_name]["files"], start=1):
            args["dataset_name"] = dataset_name
            args["nfile"] = i
            
            args["cmd"] = (
                "python3 submit.py "
                f"--processor {args['processor']} "
                f"--year {args['year']} "
                f"--output_path {args['output_path']} "
                f"--dataset_name {dataset_name} "
                f"--sample {root_file} "
                f"--nfile {str(i)} "
                f"--tagger {args['tagger']} "
                f"--wp {args['wp']} "
                f"--flavor {args['flavor']}"
            )
            submit_condor(args)
            
    
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
        help="dataset_name",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022EE, 2022, 2023}",
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