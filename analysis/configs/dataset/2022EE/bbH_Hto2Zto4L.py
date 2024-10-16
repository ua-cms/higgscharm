from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="bbH_Hto2Zto4L",
    process="bbH",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/bbH_Hto2Zto4L_M-125_TuneCP5_13p6TeV_JHUGenV752-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v1_BTV_Run3_2022_Comm_MINIAODv4/240321_110756/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=4.880e-1,
    partitions=1,
    stepsize=50_000,
)