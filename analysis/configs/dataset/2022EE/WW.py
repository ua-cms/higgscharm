from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="WW",
    process="Diboson",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/WW_TuneCP5_13p6TeV_pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240518_172311/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=80.23,
    stepsize=50_000,
)