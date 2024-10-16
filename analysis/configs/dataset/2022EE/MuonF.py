from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="MuonF",
    process="Data",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "data_2022_MINIAODv4/Muon/Run2022F-22Sep2023-v2_BTV_Run3_2022_Comm_MINIAODv4/240429_092119/"
    ),
    key="Events",
    year="2022EE",
    era="F",
    xsec=None,
    stepsize=50_000,
)
