from analysis.configs.processor_config import ProcessorConfig

processor_config = ProcessorConfig(
    lumimask="analysis/data/Cert_Collisions2022_355100_362760_Golden.txt",
    lumidata="analysis/data/lumi2022.csv",
    hlt_paths={
        "muon": ["IsoMu24", "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"],
        "electron": ["Ele30_WPTight_Gsf"],
    },
    selection={
        "muon": {
            "pt": 10,
            "abs_eta": 2.4,
            "dxy": 0.5,
            "dz": 1,
            "sip3d": 4,
            "id_wp": "tight",
            "iso_wp": "tight",
        },
        "electron": {
            "pt": 30,
            "abs_eta": 2.5,
            "id_wp": "wpiso90",
        },
    },
)