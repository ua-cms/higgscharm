import json
import glob
import argparse
from pathlib import Path
from analysis.configs.load_config import load_config


def build_single_fileset(name: str, year: str) -> dict:
    """
    builds a fileset for a single dataset

    Arguments:
        name:
            name of the dataset
        year:
            year of the dataset
    """
    dataset_config = load_config(config_type="dataset", config_name=name, year=year)
    return {
        dataset_config.name: {
            "files": {
                dataset_config.path + root_file: dataset_config.key
                for root_file in dataset_config.filenames
            },
            "metadata": {
                "short_name": dataset_config.name,
                "metadata": {"isMC": dataset_config.is_mc, "xsec": dataset_config.xsec},
            },
        },
    }


def build_full_dataset(year: str) -> None:
    """
    builds and save a full fileset for a year
    """
    main_dir = Path.home()
    dataset_path = f"{main_dir}/higgscharm/analysis/configs/dataset/{year}/"
    dataset_names = [
        f.split("/")[-1].replace(".py", "")
        for f in glob.glob(f"{dataset_path}*.py", recursive=True)
    ]    
    dataset_names.remove("__init__")
    
    full_fileset = {}
    for dataset_name in dataset_names:
        single_fileset = build_single_fileset(name=dataset_name, year=year)
        if not full_fileset:
            full_fileset = single_fileset
        else:
            full_fileset.update(single_fileset)
            
    return full_fileset
            
    # save fileset
    #output_directory = Path(f"{main_dir}/higgscharm/analysis/filesets/")
    #with open(f"{output_directory}/PFNano_fileset_{year}.json", "w") as json_file:
    #    json.dump(full_fileset, json_file, indent=4, sort_keys=True)