import os
import json
import argparse
import subprocess
from pathlib import Path
from analysis.filesets.utils import get_nano_version, get_dataset_config, is_private_query


def get_signal_fileset(query):
    query = f"file dataset=/{query}"

    env = os.environ.copy()
    env.pop("PYTHONHOME", None)

    result = subprocess.run(
        ["dasgoclient", f"-query={query} instance=prod/phys03"],
        stdout=subprocess.PIPE,
        universal_newlines=True,
        check=True,
        env=env,
    )
    stdout_array = result.stdout.splitlines()

    if stdout_array:
        stdout_array[-1] = stdout_array[-1].replace(",", "")

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
        ],
    )
    parser.add_argument(
        "--site",
        dest="site",
        default="root://maite.iihe.ac.be:1094",
        type=str,
        help="site to read the signal samples from",
    )
    parser.add_argument(
        "--samples",
        nargs="*",
        type=str,
        help="(Optional) signal samples to add. If omitted, all signal samples are added",
    )
    args = parser.parse_args()

    # open dataset configs
    dataset_configs = get_dataset_config(args.year)

    # load already generated fileset
    nano_version = get_nano_version(args.year)
    filesets_dir = Path.cwd() / "analysis" / "filesets"
    fileset_file = filesets_dir / f"fileset_{args.year}_nanov{nano_version}_lxplus.json"
    built_dataset = {}
    if fileset_file.exists():
        with open(fileset_file, "r") as f:
            built_dataset = json.load(f)

    def add_sample(key, sample, files):
        # same entry format as DataDiscoveryCLI.do_save (read by fileset_checker / submit_condor.py)
        for old_key in [
            k for k, v in built_dataset.items()
            if k == sample or (isinstance(v, dict) and v.get("metadata", {}).get("short_name") == sample)
        ]:
            del built_dataset[old_key]
        built_dataset[key] = {
            "files": {f: "Events" for f in files},
            "metadata": {"short_name": sample},
        }

    for sample in list(dataset_configs.keys()):
        if args.samples and sample not in args.samples:
            continue
        query = dataset_configs[sample]["query"]
        if is_private_query(query):
            if len(query.split("/")) != 1:
                # from DAS queries
                files = get_signal_fileset(query)
                if files:
                    files = [f"{args.site}/{f}" for f in files]
                    print(f"Adding sample {sample}")
                    add_sample(f"/{query}", sample, files)
                else:
                    print(f"query for '{sample}' sample did not work!")
            else:
                # from already built signal samples
                signal_file = filesets_dir / f"{query}.json"
                print(f"Looking for file {signal_file}")
                if signal_file.exists():
                    with open(signal_file, "r") as json_file:
                        signal_json = json.load(json_file)
                    if sample not in signal_json:
                        print(
                            f"sample key '{sample}' was not found. Available keys are {list(signal_json.keys())}"
                        )
                    else:
                        print(f"Adding signal sample '{sample}'")
                        local_samples = []
                        for root_file in signal_json[sample]:
                            # the *_signal.json files already store xrootd urls
                            if not root_file.startswith("root://"):
                                root_file = f"root://eosuser.cern.ch/{root_file}"
                            local_samples.append(root_file)
                        add_sample(sample, sample, local_samples)
                else:
                    print(f"File '{signal_file}_signal.json' was not found!")

    with open(fileset_file, "w") as json_file:
        json.dump(built_dataset, json_file, indent=4, sort_keys=True)
