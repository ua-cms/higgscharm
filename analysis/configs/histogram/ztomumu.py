import numpy as np
from analysis.configs.histogram_config import HistogramConfig

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
        "muon_pt": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 300,
            "label": r"$p_T(\mu)$ [GeV]",
        },
        "muon_eta": {
            "type": "Regular",
            "bins": 50,
            "start": -2.5,
            "stop": 2.5,
            "label": "$\eta(\mu)$",
        },
        "muon_phi": {
            "type": "Regular",
            "bins": 50,
            "start": -np.pi,
            "stop": np.pi,
            "label": "$\phi(\mu)$",
        },
        "npvs": {
            "type": "Regular",
            "bins": 60,
            "start": 0,
            "stop": 60,
            "label": "$N_{pvs}$",
        },
        "rho": {
            "type": "Regular",
            "bins": 60,
            "start": 0,
            "stop": 60,
            "label": r"$\rho$",
        },
    },
    layout={
        "zcandidate": ["z_mass", "mu1_pt", "mu2_pt"],
        "muon": ["muon_pt", "muon_eta", "muon_phi"],
        # "npvs": ["npvs"],
        # "rho": ["rho"],
    },
)