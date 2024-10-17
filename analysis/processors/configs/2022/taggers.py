from analysis.processors.configs.processor_config import ProcessorConfig

processor_config = ProcessorConfig(
    lumimask=None,
    lumidata=None,
    hlt_paths=None,
    selection={
        "muon": {
            "pt": 20,
            "abs_eta": 2.4,
            "dxy": 0.5,
            "dz": 1,
            "sip3d": 4,
            "id_wp": "loose",
            "iso_wp": "loose",
        },
        "electron": {
            "pt": 20,
            "abs_eta": 2.5,
            "id_wp": "wpiso80",
            "iso_wp": None,
        },
        "jet": {
            "pt": 25,
            "abs_eta": 2.4,
            "id": 6,
            "delta_r_lepton": True,
            "veto_maps": True,
        },
    },
)
