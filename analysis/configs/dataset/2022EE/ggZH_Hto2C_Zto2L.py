from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="ggZH_Hto2C_Zto2L",
    process="ZHcc",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/ggZH_Hto2C_Zto2L_M-125_TuneCP5_13p6TeV_powheg-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240604_172337/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=None,
    stepsize=50_000,
)