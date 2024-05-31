import numpy as np
from analysis.configs.histogram_config import HistogramConfig


histogram_config = HistogramConfig(
    add_syst_axis=False,
    add_weight=False,
    axes={
        "deepjet_cvsl": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsL"},
        "deepjet_cvsb": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsB"},
        "pnet_cvsl": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsL"},
        "pnet_cvsb": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsB"},
        "part_cvsl": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsL"},
        "part_cvsb": {"type": "Regular", "bins": 50, "start": 0, "stop": 1, "label": "CvsB"},
        "flavor": {
            "type": "IntCategory",
            "categories": [0, 4, 5],
        },
    },
    layout={
        "deepjet": ["deepjet_cvsl", "deepjet_cvsb", "flavor"],
        "pnet": ["pnet_cvsl", "pnet_cvsb", "flavor"],
        "part": ["part_cvsl", "part_cvsb", "flavor"],
    }
)