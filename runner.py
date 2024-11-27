import os
import argparse

data_samples = {
    "ztoee": {
        "2022preEE": ["EGammaC", "EGammaD"],
        "2022postEE": ["EGammaE", "EGammaF", "EGammaG"],
    },
    "ztomumu": {
        "2022preEE": ["MuonC", "MuonD"],
        "2022postEE": ["MuonE", "MuonF", "MuonG"],
    },
}
background_samples = [
    # DY+jets
    "DYto2L_2Jets_50",
    "DYto2L_2Jets_10to50",
    # Diboson
    "WW",
    "WZ",
    "ZZ",
    # Ttbar
    "TTto4Q",
    "TTto2L2Nu",
    "TTtoLNu2Q",
    # SingleTop
    "TbarWplusto2L2Nu",
    "TWminusto2L2Nu",
    "TWminustoLNu2Q",
    "TbarWplusto4Q",
    "TbarWplustoLNu2Q",
    "TWminusto4Q",
    "TbarBQ",
    "TBbarQ",
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztomumu",
        help="processor to be used {ztomumu, ztoee} (default ztomumu)",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022postEE",
        help="dataset year {2022preEE, 2022postEE} (default 2022postEE)",
    )
    parser.add_argument(
        "--nfiles",
        dest="nfiles",
        type=int,
        default=20,
        help="number of root files to include in each dataset partition (default 20)",
    )
    args = parser.parse_args()

    datasets = background_samples + data_samples[args.processor][args.year]
    for dataset in datasets:
        os.system(
            f"python3 submit_condor.py --processor {args.processor} --year {args.year} --dataset {dataset} --nfiles {args.nfiles} --submit"
        )
