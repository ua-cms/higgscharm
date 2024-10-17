import numpy as np
from analysis.histograms.histogram_config import HistogramConfig


histogram_config = HistogramConfig(
    add_syst_axis=True,
    add_weight=True,
    add_cat_axis=None,
    axes={
        "higgs_mass": {
            "type": "Regular",
            "bins": 120,
            "start": 10,
            "stop": 150,
            "label": r"$m(H)$ [GeV]",
        },
        "higgs_pt": {
            "type": "Regular",
            "bins": 40,
            "start": 0,
            "stop": 300,
            "label": r"$p_T(H)$ [GeV]",
        },
        "z1_mass": {
            "type": "Regular",
            "bins": 100,
            "start": 10,
            "stop": 150,
            "label": r"$m(Z)$ [GeV]",
        },
        "z2_mass": {
            "type": "Regular",
            "bins": 50,
            "start": 10,
            "stop": 150,
            "label": r"$m(Z^*)$ [GeV]",
        },
        "cjet_pt": {
            "type": "Regular",
            "bins": 30,
            "start": 30,
            "stop": 150,
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
        "cjet_higgs_deltaphi": {
            "type": "Regular",
            "bins": 50,
            "start": -np.pi,
            "stop": np.pi,
            "label": "$\Delta\phi$(Jet, H)",
        },
    },
    layout="individual"
)