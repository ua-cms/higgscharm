import glob
import argparse
from pathlib import Path
from copy import deepcopy
from analysis.configs.load_config import load_config


def build_single_fileset(name: str, year: str) -> dict:
    """
    builds a fileset for a single dataset

    Parameters:
        name:
            name of the dataset
        year:
            year of the dataset {2022EE, 2022, 2023}
    """
    dataset_config = load_config(config_type="dataset", config_name=name, year=year)
    root_files = glob.glob(f"{dataset_config.path}*.root")
    return {
        dataset_config.name: {
            "files": {
                root_file: dataset_config.key
                for root_file in root_files
            },
            "metadata": {
                "short_name": dataset_config.name,
                "metadata": {"era": dataset_config.era, "xsec": dataset_config.xsec},
            },
        },
    }


def build_full_dataset(year: str) -> None:
    """
    builds and save a full fileset for a year
    """
    main_dir = Path.cwd()
    dataset_path = f"{main_dir}/analysis/configs/dataset/{year}/"
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


def divide_list(lst: list) -> list:
    """Divide a list into sublists such that each sublist has at least 20 elements."""
    if len(lst) < 20:
        return 1, [lst]
    
    # Dynamically calculate the number of sublists such that each has at least 20 elements
    n = len(lst) // 20  # This gives the number of groups with at least 20 elements
    if len(lst) % 20 != 0:
        n += 1  # Increase n by 1 if there is a remainder, to accommodate extra elements
    
    # Divide the list into 'n' sublists
    size = len(lst) // n
    remainder = len(lst) % n
    result = []
    start = 0
    
    for i in range(n):
        if i < remainder:
            end = start + size + 1
        else:
            end = start + size
        result.append(lst[start:end])
        start = end
    
    return n, result