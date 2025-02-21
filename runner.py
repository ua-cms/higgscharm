import os
import argparse


MC_DATASETS = {
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
    "dyjets": ["DYto2L_2Jets_50", "DYto2L_2Jets_10to50"],
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
    "ew": [
        "ZZZ",
        "WZZ",
        "WWZ",
        "TTWW",
        "TTZZ",
        "TTZ",
        "WZto3LNu"
    ]
}

PD_DATASETS = {
    "Muon": {
        "2022preEE": ["MuonC", "MuonD"],
        "2022postEE": ["MuonE", "MuonF", "MuonG"],
    },
    "SingleMuon": {
        "2022preEE": ["SingleMuonC"],
        "2022postEE": [],
    },
    "DoubleMuon": {
        "2022preEE": ["DoubleMuonC"],
        "2022postEE": [],
    },
    "EGamma": {
        "2022preEE": ["EGammaC", "EGammaD"],
        "2022postEE": ["EGammaE", "EGammaF", "EGammaG"],
    },
    "MuonEG": {
        "2022preEE": ["MuonEGC", "MuonEGD"],
        "2022postEE": ["MuonEGE", "MuonEGF", "MuonEGG"],
    }
}

DATASETS = {
    "hww": {
        "2022preEE": {
            "mc": MC_DATASETS["ttbar"] + MC_DATASETS["singletop"] + MC_DATASETS["diboson"],
            "data": PD_DATASETS["Muon"]["2022preEE"] + PD_DATASETS["MuonEG"]["2022preEE"] + PD_DATASETS["EGamma"]["2022preEE"],
        },
        "2022postEE": {
            "mc": MC_DATASETS["ttbar"] + MC_DATASETS["singletop"] + MC_DATASETS["diboson"],
            "data": PD_DATASETS["Muon"]["2022postEE"] + PD_DATASETS["MuonEG"]["2022postEE"] + PD_DATASETS["EGamma"]["2022postEE"],
        },
    },
    "zzto4l": {
        "2022preEE": {
            "mc": MC_DATASETS["higgs"] + MC_DATASETS["ggtozz"] + MC_DATASETS["qqtozz"] + MC_DATASETS["ew"],
            "data": PD_DATASETS["SingleMuon"]["2022preEE"] + PD_DATASETS["DoubleMuon"]["2022preEE"] + PD_DATASETS["Muon"]["2022preEE"] + PD_DATASETS["MuonEG"]["2022preEE"] + PD_DATASETS["EGamma"]["2022preEE"],
        },
        "2022postEE": {
            "mc": MC_DATASETS["higgs"] + MC_DATASETS["ggtozz"] + MC_DATASETS["qqtozz"],
            "data": PD_DATASETS["Muon"]["2022postEE"] + PD_DATASETS["MuonEG"]["2022postEE"] + PD_DATASETS["EGamma"]["2022postEE"],
        }
    },
    "ztoee": {
        "2022preEE": {
            "mc": MC_DATASETS["dyjets"] + MC_DATASETS["ttbar"] + MC_DATASETS["singletop"] + MC_DATASETS["diboson"],
            "data": PD_DATASETS["EGamma"]["2022preEE"]
        },
        "2022postEE": {
            "mc": MC_DATASETS["dyjets"] + MC_DATASETS["ttbar"] + MC_DATASETS["singletop"] + MC_DATASETS["diboson"],
            "data": PD_DATASETS["EGamma"]["2022postEE"]
        } 
    },
    "ztomumu": {
        "2022preEE": {
            "mc": MC_DATASETS["dyjets"] + MC_DATASETS["ttbar"] + MC_DATASETS["singletop"] + MC_DATASETS["diboson"],
            "data": PD_DATASETS["Muon"]["2022preEE"]
        },
        "2022postEE": {
            "mc": MC_DATASETS["dyjets"] + MC_DATASETS["ttbar"] + MC_DATASETS["singletop"] + MC_DATASETS["diboson"],
            "data": PD_DATASETS["Muon"]["2022postEE"]
        } 
    }
}


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
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        help="format of output histograms {root, coffea}",
    )
    args = parser.parse_args()
    
    # get datasets for processor and year
    datasets = DATASETS[args.processor][args.year]["mc"] + DATASETS[args.processor][args.year]["data"]
    # submit job for each dataset
    for dataset in datasets:
        cmd = f"python3 submit_condor.py --processor {args.processor} --year {args.year} --dataset {dataset} --nfiles {args.nfiles} --output_format {args.output_format}"
        if args.submit:
            cmd += " --submit"
        if args.eos:
            cmd += " --eos"
        os.system(cmd)