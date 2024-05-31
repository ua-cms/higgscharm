import numpy as np
from analysis.configs.histogram_config import HistogramConfig

histogram_config = HistogramConfig(
    add_syst_axis=False,
    add_weight=False,
    axes={
        "pt": {
            "type": "Variable",
            "edges": [20, 30, 50, 70, 100, 140, 200, 300, 600, 1000],
            "label": r"Jet $pT$ [GeV]",
        },
        "eta": {
            "type": "Regular",
            "bins": 10,
            "start": -2.5,
            "stop": 2.5,
            "label": r"Jet $\eta$",
        },
        "flavor": {
            "type": "IntCategory",
            "categories": [0, 4, 5],
        },
        "pass_wp": {"type": "IntCategory", "categories": [0, 1]},
    },
    layout={
        "eff": ["pt", "eta", "flavor", "pass_wp"]
    }
)