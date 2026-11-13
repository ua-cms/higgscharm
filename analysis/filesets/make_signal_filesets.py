import json
import argparse
import subprocess
from pathlib import Path
from analysis.filesets.utils import get_nano_version, get_dataset_config


def get_signal_fileset(query, instance="prod/phys03"):
    """Query DAS for files in a dataset.

    Args:
        query: DAS dataset path (without leading /)
        instance: DAS instance (default: prod/phys03 for user datasets)

    Returns:
        List of file paths
    """
    das_query = f"file dataset=/{query} instance={instance}"
    farray = subprocess.run(
        ["dasgoclient", f"-query={das_query}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if farray.returncode != 0:
        print(f"DAS query failed: {farray.stderr}")
        return []
    stdout = farray.stdout
    stdout_array = stdout.split("\n")
    stdout_array = [f for f in stdout_array if f.strip()]  # Remove empty lines
    return stdout_array


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=str,
        choices=[
            "2016preVFP",
            "2016postVFP",
            "2017",
            "2018",
            "2022preEE",
            "2022postEE",
            "2023preBPix",
            "2023postBPix",
            "2024",
            "2025",
            "2026",
        ],
    )
    parser.add_argument(
        "--site",
        dest="site",
        default="root://maite.iihe.ac.be:1094",
        type=str,
        help="xrootd site/redirector to read the signal samples from",
    )
    parser.add_argument(
        "--xrd-global",
        dest="xrd_global",
        default="root://cms-xrd-global.cern.ch/",
        type=str,
        help="xrootd global redirector for user datasets (phys03)",
    )
    parser.add_argument(
        "--samples",
        nargs="*",
        type=str,
        help="(Optional) List of specific samples to process",
    )
    args = parser.parse_args()

    # open dataset configs
    dataset_configs = get_dataset_config(args.year)

    # load already generated fileset (or create empty if it doesn't exist)
    nano_version = get_nano_version(args.year)
    filesets_dir = Path.cwd() / "analysis" / "filesets"
    fileset_file = filesets_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"
    if fileset_file.exists():
        with open(fileset_file, "r") as f:
            built_dataset = json.load(f)
    else:
        built_dataset = {}

    # Filter samples if specified
    samples_to_process = args.samples if args.samples else list(dataset_configs.keys())

    for sample in samples_to_process:
        if sample not in dataset_configs:
            print(f"Sample '{sample}' not found in dataset config")
            continue
        query = dataset_configs[sample]["query"]
        if len(query.split("/")) != 1:
            # from DAS queries - check if it's a USER dataset (phys03) or global
            is_user_dataset = query.endswith("/USER")
            if is_user_dataset:
                instance = "prod/phys03"
                site = args.xrd_global
                print(f"Querying DAS (phys03) for sample {sample}...")
            else:
                instance = "prod/global"
                site = args.site
                print(f"Querying DAS (global) for sample {sample}...")

            files = get_signal_fileset(query, instance=instance)
            if files:
                files = [f"{site}{f}" for f in files]
                print(f"  Found {len(files)} files, adding sample {sample}")
                built_dataset[sample] = files
            else:
                print(f"  No files found for {sample}")
        else:
            # from already built samples (JSON files)
            signal_file = filesets_dir / f"{query}.json"
            print(f"Looking for file {signal_file}")
            if signal_file.exists():
                with open(signal_file, "r") as json_file:
                    signal_json = json.load(json_file)
                if sample not in signal_json:
                    # Try using the query as key (for HB files)
                    if query in signal_json:
                        print(f"Adding sample '{sample}' from key '{query}'")
                        built_dataset[sample] = signal_json[query]
                    else:
                        print(f"sample key '{sample}' was not found. Available keys are {list(signal_json.keys())}")
                else:
                    print(f"Adding sample '{sample}'")
                    built_dataset[sample] = signal_json[sample]
            else:
                print(f"File '{signal_file}' was not found!")

    with open(fileset_file, "w") as json_file:
        json.dump(built_dataset, json_file, indent=4, sort_keys=True)
    print(f"\nSaved fileset to {fileset_file}")