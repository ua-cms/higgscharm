import numpy as np
from analysis.histograms.histogram_config import HistogramConfig

histogram_config = HistogramConfig(
    add_syst_axis=True,
    add_weight=True,
    add_cat_axis=None,
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
        "cjet_pt": {
            "type": "Regular",
            "bins": 30,
            "start": 30,
            "stop": 200,
            "label": r"Jet $p_T$ [GeV]",
        },
        "cjet_eta": {
            "type": "Regular",
            "bins": 50,
            "start": -2.5,
            "stop": 2.5,
            "label": "Jet $\eta$",
        },
        "cjet_phi": {
            "type": "Regular",
            "bins": 50,
            "start": -np.pi,
            "stop": np.pi,
            "label": "Jet $\phi$",
        },
        "cjet_z_deltaphi": {
            "type": "Regular",
            "bins": 50,
            "start": -np.pi,
            "stop": np.pi,
            "label": "Jet $\phi$",
        },
        "njets": {
            "type": "IntCategory",
            "categories": np.arange(0, 16),
            "label": "$N_{jets}$",
        },
        "npvs": {
            "type": "Regular",
            "bins": 60,
            "start": 0,
            "stop": 60,
            "label": "$N_{pvs}$",
        },
    },
    layout="individual"
)