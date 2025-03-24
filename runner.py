import os
import argparse


MC_DATASETS = {
    "semilep_ttbar": ["TTto2L2Nu"],
    "ttbar": ["TTto2L2Nu", "TTto4Q", "TTtoLNu2Q"],
    "singletop": [
        "TWminusto2L2Nu",
        "TbarWplusto2L2Nu",
        "TWminustoLNu2Q",
        "TbarWplusto4Q",
        "TbarWplustoLNu2Q",
        "TWminusto4Q",
        "TbarBQ",
        "TBbarQ",
    ],
    "diboson": ["WW", "WZ", "ZZ"],
    "dyjets_50": ["DYto2L_2Jets_50"]
    "dyjets_10to50": ["DYto2L_2Jets_10to50"],
    "dyjets": ["DYto2L_2Jets_50", "DYto2L_2Jets_10to50"]
    "higgs": [
        "bbH_Hto2Zto4L",
        "GluGluHtoZZto4L",
        "TTH_Hto2Z",
        "VBFHto2Zto4L",
        "WminusH_Hto2Zto4L",
        "WplusH_Hto2Zto4L",
        "ZHto2Zto4L",
    ],
    "ggtozz": [
        "GluGluToContinto2Zto2E2Mu",
        "GluGluToContinto2Zto2E2Tau",
        "GluGluToContinto2Zto2Mu2Tau",
        "GluGlutoContinto2Zto4E",
        "GluGlutoContinto2Zto4Mu",
        "GluGlutoContinto2Zto4Tau",
    ],
    "qqtozz": ["ZZto4L"],
    "wz": ["WZto3LNu"],
    "triboson": ["ZZZ", "WZZ", "WWZ"],
    "tt_bosons": ["TTWW", "TTZZ", "TTZ"],
}
PD_DATASETS = {
    "Muon": {
        "2022preEE": ["MuonC", "MuonD"],
        "2022postEE": ["MuonE", "MuonF", "MuonG"],
        "2023preBPix": [
            "Muon0v1C",
            "Muon0v2C",
            "Muon0v3C",
            "Muon0v4C",
            "Muon1v1C",
            "Muon1v2C",
            "Muon1v3C",
            "Muon1v4C",
        ],
        "2023postBPix": ["Muon0v1D", "Muon0v2D", "Muon1v1D", "Muon1v2D"],
    },
    "SingleMuon": {
        "2022preEE": ["SingleMuonC"],
        "2022postEE": [],
        "2023preBPix": [],
        "2023postBPix": [],
    },
    "DoubleMuon": {
        "2022preEE": ["DoubleMuonC"],
        "2022postEE": [],
        "2023preBPix": [],
        "2023postBPix": [],
    },
    "EGamma": {
        "2022preEE": ["EGammaC", "EGammaD"],
        "2022postEE": ["EGammaE", "EGammaF", "EGammaG"],
        "2023preBPix": [
            "EGamma0v1C",
            "EGamma0v2C",
            "EGamma0v3C",
            "EGamma0v4C",
            "EGamma1v1C",
            "EGamma1v2C",
            "EGamma1v3C",
            "EGamma1v4C",
        ],
        "2023postBPix": ["EGamma0v1D", "EGamma0v2D", "EGamma1v1D", "EGamma1v2D"],
    },
    "MuonEG": {
        "2022preEE": ["MuonEGC", "MuonEGD"],
        "2022postEE": ["MuonEGE", "MuonEGF", "MuonEGG"],
        "2023preBPix": ["MuonEGv1C", "MuonEGv2C", "MuonEGv3C", "MuonEGv4C"],
        "2023postBPix": ["MuonEGv1D", "MuonEGv2D"],
    },
}
DATASETS = {
    "ztoee": {"mc": ["dyjets", "ttbar", "singletop", "diboson"], "data": ["EGamma"]},
    "ztomumu": {"mc": ["dyjets", "ttbar", "singletop", "diboson"], "data": ["Muon"]},
    "hww": {
        "mc": ["ttbar", "singletop", "diboson"],
        "data": ["SingleMuon", "DoubleMuon", "Muon", "MuonEG", "EGamma"],
    },
    "zzto4l": {
        "mc": ["higgs", "ggtozz", "qqtozz"],
        "data": ["SingleMuon", "DoubleMuon", "Muon", "MuonEG", "EGamma"],
    },
    "zplusl": {
        "mc": ["triboson", "wz", "tt_bosons", "dyjets", "semilep_ttbar"],
        "data": ["SingleMuon", "DoubleMuon", "Muon", "MuonEG", "EGamma"],
    },
    "zplusll": {
        "mc": ["triboson", "wz", "tt_bosons", "dyjets", "semilep_ttbar"],
        "data": ["SingleMuon", "DoubleMuon", "Muon", "MuonEG", "EGamma"],
    }
}


def main(args):
    # get datasets to run
    to_run = []
    for kind, datasets in DATASETS[args.processor].items():
        for dataset in datasets:
            if kind == "mc":
                to_run += MC_DATASETS[dataset]
            if kind == "data":
                to_run += PD_DATASETS[dataset][args.year]
    # submit jobs for each dataset
    for dataset in to_run:
        cmd = f"python3 submit_condor.py --processor {args.processor} --year {args.year} --dataset {dataset} --nfiles {args.nfiles} --output_format {args.output_format}"
        if args.submit:
            cmd += " --submit"
        if args.eos:
            cmd += " --eos"
        os.system(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        choices=["ztomumu", "ztoee", "zzto4l", "hww", "zplusl", "zplusll"],
        help="processor to be used",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        choices=["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"],
        help="dataset year",
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
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="format of output histogram",
    )
    args = parser.parse_args()
    main(args)
