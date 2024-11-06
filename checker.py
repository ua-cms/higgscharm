import yaml

def run_checker(args):
    # check processor
    available_processors = ["ztomumu", "ztoee"]
    if args.processor not in available_processors:
        raise ValueError(
            f"Incorrect processor. Available processors are: {available_processors}"
        )
    # check years
    available_years = ["2022preEE", "2022postEE"]
    if args.year not in available_years:
        raise ValueError(f"Incorrect year. Available years are: {available_years}")
    # check sample
    dataset_path = f"analysis/filesets/{args.year}_fileset.yaml"
    with open(dataset_path, "r") as f:
        dataset_config = yaml.safe_load(f)
    available_datasets = list(dataset_config.keys())
    if args.dataset not in available_datasets:
        raise ValueError(
            f"Incorrect dataset. Available datasets are: {available_datasets}"
        )
    # check processor/year/dataset combinations
    if args.processor == "ztomumu":
        if args.dataset.startswith("EGamma"):
            raise ValueError("ztomumu processor should be run with 'MuonX'")
        if args.year == "2022postEE":
            if args.dataset in ["MuonC", "MuonD"]:
                raise ValueError(
                    "ztomumu processor for 2022postEE should be run with 'MuonE', 'MuonF' or 'MuonG'"
                )
        if args.year == "2022preEE":
            if args.dataset in ["MuonE", "MuonF", "MuonG"]:
                raise ValueError(
                    "ztomumu processor for 2022preEE should be run with 'MuonC' or 'MuonD'"
                )
    if args.processor == "ztoee":
        if args.dataset.startswith("Muon"):
            raise ValueError("ztoee processor should be run with 'EGammaX'")
        if args.year == "2022postEE":
            if args.dataset in ["EGammaC", "EGammaD"]:
                raise ValueError(
                    "ztoee processor for 2022postEE should be run with 'EGammaE', 'EGammaF' or 'EGammaG'"
                )
        if args.year == "2022preEE":
            if args.dataset in ["EGammaE", "EGammaF", "EGammaG"]:
                raise ValueError(
                    "ztoee processor for 2022preEE should be run with 'EGammaC' or 'EGammaD'"
                )
