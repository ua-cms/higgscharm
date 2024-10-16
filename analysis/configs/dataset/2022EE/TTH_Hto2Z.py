from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="TTH_Hto2Z",
    process="ttH",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/TTH_Hto2Z_M-125_4LFilter_TuneCP5_13p6TeV_powheg2-JHUGenV752-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240322_082449/0000/"
    ),
    key="Events",
    year="2022EE",
    era="MC",
    xsec=0.5806,
    stepsize=50_000,
)
