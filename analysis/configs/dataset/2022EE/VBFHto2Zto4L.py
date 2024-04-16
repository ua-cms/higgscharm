from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="VBFHto2Zto4L",
    path=(
        "root://maite.iihe.ac.be:1094//store/user/daocampo/PFNano_Run3/"
        "mc_summer22EE_MINIAODv4/VBFHto2Zto4L_M125_TuneCP5_13p6TeV_powheg-jhugenv752-pythia8/"
        "Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_BTV_Run3_2022_Comm_MINIAODv4/240325_105444/0000/"
    ),
    key="Events",
    year="2022EE",
    is_mc=True,
    xsec=4.224,
    partitions=1,
    stepsize=10000,
    filenames=(
        "MC_defaultAK4_1.root",
        "MC_defaultAK4_2.root",
        "MC_defaultAK4_3.root",
        "MC_defaultAK4_4.root",
        "MC_defaultAK4_5.root",
        "MC_defaultAK4_6.root",
        "MC_defaultAK4_7.root",
        "MC_defaultAK4_8.root",
        "MC_defaultAK4_9.root",
        "MC_defaultAK4_10.root",
        "MC_defaultAK4_11.root",
        "MC_defaultAK4_12.root",
        "MC_defaultAK4_13.root",
        "MC_defaultAK4_14.root",
    )
)
