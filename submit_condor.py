import os
import argparse
import subprocess
from pathlib import Path
from condor.utils import submit_condor
from analysis.configs import load_config
from analysis.filesets.utils import build_full_dataset


def get_filesets(dataset_name: str, year: str) -> dict:
    """
    returns dataset runnables paths in a dictionary of the form {<dataset partition name>: <dataset partition path>}
    
    Parameters:
        dataset_name:
            name of the dataset
        year:
            era year
    """
    main_dir = Path.cwd()
    fileset_path = Path(f"{main_dir}/analysis/filesets/dataset_runnables/{year}")
    file_list = glob.glob(f"{fileset_path}/{dataset_name}*.json")
    filesets = {}
    for file in file_list:
        file_name = file.split("/")[-1].replace(".json", "")
        if file_name.startswith(dataset_name):
            filesets[file_name] = file
    if len(filesets) != 1:
        # sort the dictionary keys based on the number after the "_" in ascending order
        sorted_keys = sorted(filesets.keys(), key=lambda x: int(x.split("_")[-1]))
        # create an ordered dictionary using the sorted keys
        ordered_filesets = OrderedDict((key, filesets[key]) for key in sorted_keys)
        return ordered_filesets
    return filesets


def main(args):
    args = vars(args)

    output_path = Path(Path.cwd() / "outputs" / args["processor"] / args["year"])
    if args["processor"] == "tag_eff":
        output_path = Path(output_path / args["tagger"] / args["flavor"] / args["wp"])
    if not output_path.exists():
        output_path.mkdir(parents=True)
    args["output_path"] = str(output_path)

    # get dataset runnable

    filesets = get_filesets(args["dataset_name"], args["year"])

    for dataset_name, fileset_path in filesets.items():
        args["dataset_name"] = dataset_name
        args["cmd"] = (
            "python3 submit.py "
            f"--processor {args['processor']} "
            f"--year {args['year']} "
            f"--output_path {args['output_path']} "
            f"--fileset_path {fileset_path} "
            f"--dataset_name {dataset_name} "
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