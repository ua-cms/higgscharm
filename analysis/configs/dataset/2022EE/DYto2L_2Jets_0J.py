from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="DYto2L_2Jets_0J",
    process="DY+Jets",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/DYto2L-2Jets_MLL-50_0J_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240326_081304/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=5378.,
    stepsize=50_000,
)
