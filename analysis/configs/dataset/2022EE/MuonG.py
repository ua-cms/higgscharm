from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="MuonG",
    process="Data",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "data_2022_MINIAODv4/Muon/Run2022G-22Sep2023-v1_BTV_Run3_2022_Comm_MINIAODv4/240429_092127/0000/"
    ),
    key="Events",
    year="2022EE",
    era="G",
    xsec=None,
    stepsize=50_000,
)