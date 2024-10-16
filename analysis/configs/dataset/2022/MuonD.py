from analysis.configs.dataset_config import DatasetConfig

dataset_config = DatasetConfig(
    name="MuonD",
    process="Data",
    path=(
        "/pnfs/iihe/cms/store/user/daocampo/PFNano_Run3/"
        "data_2022_MINIAODv4/Muon/Run2022D-22Sep2023-v1_BTV_Run3_2022_Comm_MINIAODv4/240429_092058/0000/"
    ),
    key="Events",
    year="2022",
    era="D",
    xsec=None,
    partitions=15,
    stepsize=50_000,
)