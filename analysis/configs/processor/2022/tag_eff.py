from analysis.configs.processor_config import ProcessorConfig

processor_config = ProcessorConfig(
    lumimask=None,
    hlt_paths=None,
    selection={
        "jet": {
            "pt": 20,
            "abs_eta": 2.5,
            "veto_maps": True,
        },
    }
)