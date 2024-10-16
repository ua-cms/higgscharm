from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="ZHto2Zto4L",
    process="ZH",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/ZHto2Zto4L_M125_TuneCP5_13p6TeV_powheg2-minlo-HZJ-JHUGenV752-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240326_080929/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=8.839e-1,
    stepsize=50_000,
)
