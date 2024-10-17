import numpy as np
from analysis.histograms.configs.histogram_config import HistogramConfig

histogram_config = HistogramConfig(
    add_syst_axis=True,
    add_weight=True,
    add_cat_axis=None,
    axes={
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
        "pnet_cvsb": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 1,
            "label": "CvsB",
        },
        "pnet_cvsl": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 1,
            "label": "CvsL",
        },
        "flavor": {
            "type": "IntCategory",
            "categories": [0, 4, 5],
        },
    },
    layout={
        "pt": ["jet_pt", "flavor"],
        "eta":["jet_eta", "flavor"], 
        "phi": ["jet_phi", "flavor"],
        "cvsb": ["pnet_cvsb", "flavor"],
        "cvsl": ["pnet_cvsl", "flavor"],
    }
)
