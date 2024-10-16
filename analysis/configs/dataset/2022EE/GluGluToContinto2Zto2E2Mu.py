from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="GluGluToContinto2Zto2E2Mu",
    process="ggToZZ",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/GluGluToContinto2Zto2E2Mu_TuneCP5_13p6TeV_mcfm701-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240325_122607/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=6.115,
    stepsize=50_000,
)