import os
import argparse


ERAS = {"2022preEE": ["C", "D"], "2022postEE": ["E", "F", "G"]}
PRIMARY_DATASETS = ["Muon", "MuonEG", "EGamma"]
DATA_SAMPLES = {}
for year, eras in ERAS.items():
    DATA_SAMPLES[year] = {}
    for primary_dataset in PRIMARY_DATASETS:
        DATA_SAMPLES[year][primary_dataset] = []
        for era in eras:
            DATA_SAMPLES[year][primary_dataset].append(f"{primary_dataset}{era}")

            
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
}


DATASETS = {
    "hww": {
        "mc": ["ttbar", "singletop", "diboson"],
        "data": ["Muon", "MuonEG", "EGamma"],
    },
    "zzto4l": {
        "mc": ["higgs", "ggtozz", "qqtozz"],
        "data": ["Muon", "MuonEG", "EGamma"],
    },
    "ztoee": {
        "mc": ["ttbar", "singletop", "diboson"], 
        "data": ["EGamma"]
    },
    "ztomumu": {
        "mc": ["ttbar", "singletop", "diboson"], 
        "data": ["EGamma"]
    },
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
        "--root",
        action="store_true",
        help="Enable saving outputs in .root format",
    )
    parser.add_argument(
        "--coffea",
        action="store_true",
        help="Enable saving outputs in .coffea format",
    )
    args = parser.parse_args()
    # get datasets for processor and year
    mc = [
        sample
        for dataset in DATASETS[args.processor]["mc"]
        for sample in MC_DATASETS[dataset]
    ]
    data = [
        sample
        for dataset in DATASETS[args.processor]["data"]
        for sample in DATA_SAMPLES[args.year][dataset]
    ]
    datasets = mc + data
    # submit job for each dataset
    for dataset in datasets:
        cmd = f"python3 submit_condor.py --processor {args.processor} --year {args.year} --dataset {dataset} --nfiles {args.nfiles}"
        if args.submit:
            cmd += " --submit"
        if args.eos:
            cmd += " --eos"
        if args.coffea:
            cmd += " --coffea"
        elif args.root:
            cmd += " --root"
        os.system(cmd)