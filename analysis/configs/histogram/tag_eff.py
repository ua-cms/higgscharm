import numpy as np
from analysis.configs.histogram_config import HistogramConfig


histogram_config = HistogramConfig(
    individual=False,
    add_dataset_axis=True,
    add_syst_axis=False,
    add_weight=False,
    axes={
        "pt": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsL"},
        "cvsb": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsB"},
        "flavor": {
            "type": "IntCategory",
            "categories": [0, 4, 5],
        },
        "tagger": {
            "type": "StrCategory",
            "categories": ["deepjet", "pnet", "part"],
        },
    },
)
