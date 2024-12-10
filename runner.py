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
    "zzto4l": {
        "2022preEE": ["MuonC", "MuonD", "MuonEGC", "MuonEGD", "EGammaC", "EGammaD"],
        "2022postEE": [
            "MuonE",
            "MuonF",
            "MuonG",
            "MuonEGE",
            "MuonEGF",
            "MuonEGG",
            "EGammaE",
            "EGammaF",
            "EGammaG",
        ],
    },
}
mc_samples = {
    "ztoee": [
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
    ],
    "ztomumu": [
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
    ],
    "zzto4l": [
        # SIGNAL
        "bbH_Hto2Zto4L",
        "GluGluHtoZZto4L",
        "TTH_Hto2Z",
        "VBFHto2Zto4L",
        "WminusH_Hto2Zto4L",
        "WplusH_Hto2Zto4L",
        "ZHto2Zto4L"
        # BACKGROUND
        "GluGluToContinto2Zto2E2Mu",
        "GluGluToContinto2Zto2E2Tau",
        "GluGluToContinto2Zto2Mu2Tau",
        "GluGlutoContinto2Zto4E",
        "GluGlutoContinto2Zto4Mu",
        "GluGlutoContinto2Zto4Tau",
        "ZZto4L",
    ],
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztomumu",
        help="processor to be used {ztomumu, ztoee, zto4l} (default ztomumu)",
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

    datasets = (
        background_samples[args.processor] + data_samples[args.processor][args.year]
    )
    for dataset in datasets:
        cmd = f"python3 submit_condor.py --processor {args.processor} --year {args.year} --dataset {dataset} --nfiles {args.nfiles}"
        if args.submit:
            cmd += " --submit"
        if args.eos:
            cmd += " --eos"
        os.system(cmd)
