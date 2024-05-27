import numpy as np
from analysis.configs.histogram_config import HistogramConfig

histogram_config = HistogramConfig(
    individual=True,
    add_syst_axis=True,
    add_weight=True,
    names=[],
    axes={
        "z_mass": {
            "type": "Regular",
            "bins": 100,
            "start": 10,
            "stop": 150,
            "label": r"$m(Z)$ [GeV]",
        },
        "mu1_pt": {
            "type": "Regular",
            "bins": 50,
            "start": 20,
            "stop": 300,
            "label": r"$p_T(\mu_1)$ [GeV]",
        },
        "mu2_pt": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 300,
            "label": r"$p_T(\mu_2)$ [GeV]",
        },
        "npvs": {
            "type": "Regular",
            "bins": 60,
            "start": 0,
            "stop": 60,
            "label": "$N_{pvs}$",
        },
        "njets": {
            "type": "IntCategory",
            "categories": np.arange(0, 16),
            "label": "$N_{jets}$",
        },
    },
)
