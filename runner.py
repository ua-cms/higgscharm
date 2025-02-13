import os
import argparse
from analysis.configs import ProcessorConfigBuilder


def get_datasets(processor: str, year: str):
    eras = {"2022preEE": ["C", "D"], "2022postEE": ["E", "F", "G"]}
    config_builder = ProcessorConfigBuilder(processor=processor, year=year)
    processor_config = config_builder.build_processor_config()
    mc_datasets = processor_config.mc_datasets
    primary_datasets = list(processor_config.hlt_paths.keys())
    data_datasets = []
    for primary_dataset in primary_datasets:
        for era in eras[year]:
            data_datasets.append(f"{primary_dataset}{era}")

    return data_datasets + mc_datasets


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        help="processor to be used {ztomumu, ztoee, zzto4l, hww}",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        help="dataset year {2022preEE, 2022postEE}",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=10,
        help="number of root files to include in each dataset partition (default 10)",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Enable Condor job submission. If not provided, it just builds condor files",
    )
    parser.add_argument(
        "--eos",
        action="store_true",
        help="Enable saving outputs to /eos",
    )
    args = parser.parse_args()

    for dataset in get_datasets(processor=args.processor, year=args.year):
        cmd = f"python3 submit_condor.py --processor {args.processor} --year {args.year} --dataset {dataset} --nfiles {args.nfiles}"
        if args.submit:
            cmd += " --submit"
        if args.eos:
            cmd += " --eos"
        os.system(cmd)
