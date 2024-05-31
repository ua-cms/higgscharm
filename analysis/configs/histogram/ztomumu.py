import numpy as np
from analysis.configs.histogram_config import HistogramConfig


histogram_config = HistogramConfig(
    add_syst_axis=True,
    add_weight=True,
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
        "jet_pt": {
            "type": "Regular",
            "bins": 30,
            "start": 30,
            "stop": 200,
            "label": r"Jet $p_T$ [GeV]",
        },
        "jet_eta": {
            "type": "Regular",
            "bins": 50,
            "start": -2.5,
            "stop": 2.5,
            "label": "Jet $\eta$",
        },
        "jet_phi": {
            "type": "Regular",
            "bins": 50,
            "start": -np.pi,
            "stop": np.pi,
            "label": "Jet $\phi$",
        },
        "jet_flavor": {
            "type": "IntCategory",
            "categories": [0, 4, 5],
        },
        "pnet_cvsl": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 1,
            "label": "CvsL",
        },
        "pnet_cvsb": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 1,
            "label": "CvsB",
        },
        "nljets": {
            "type": "IntCategory",
            "categories": np.arange(0, 16),
            "label": "$N_{jets}$",
        },
        "nbjets": {
            "type": "IntCategory",
            "categories": np.arange(0, 16),
            "label": "$N_{jets}$",
        },
        "ncjets": {
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
    layout={
        "z_mass": ["z_mass"],
        "mu1_pt": ["mu1_pt"],
        "mu2_pt": ["mu2_pt"],
        "jet_kin": ["jet_pt", "jet_eta", "jet_phi"],
        "jet_tag": ["jet_flavor", "pnet_cvsl", "pnet_cvsb"],
        "nljets": ["nljets"],
        "nbjets": ["nbjets"],
        "ncjets": ["ncjets"],
        "npvs": ["npvs"],
    },
)