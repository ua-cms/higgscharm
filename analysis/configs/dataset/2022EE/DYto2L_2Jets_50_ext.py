from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="DYto2L_2Jets_50_ext",
    process="DY+Jets",
    path=(
        "/pnfs/iihe/cms/store/user/tvanlaer/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/DYto2L-2Jets_MLL-50_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6_ext1-v1_BTV_Run3_2022_Comm_MINIAODv4/240827_132427/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=None,
    partitions=30,
    stepsize=50_000,
)
