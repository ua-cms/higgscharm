import numpy as np
from analysis.histograms.configs.histogram_config import HistogramConfig

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
        "leading_lepton_pt": {
            "type": "Regular",
            "bins": 50,
            "start": 20,
            "stop": 300,
            "label": r"$p_T(\ell_1)$ [GeV]",
        },
        "subleading_lepton_pt": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 300,
            "label": r"$p_T(\ell_2)$ [GeV]",
        },
        "lepton_pt": {
            "type": "Regular",
            "bins": 50,
            "start": 0,
            "stop": 300,
            "label": r"$p_T(\ell)$ [GeV]",
        },
        "lepton_eta": {
            "type": "Regular",
            "bins": 50,
            "start": -2.5,
            "stop": 2.5,
            "label": "$\eta(\ell)$",
        },
        "lepton_phi": {
            "type": "Regular",
            "bins": 50,
            "start": -np.pi,
            "stop": np.pi,
            "label": "$\phi(\ell)$",
        }
    },
    layout={
        "zcandidate": ["z_mass", "leading_lepton_pt", "subleading_lepton_pt"],
        "lepton": ["lepton_pt", "lepton_eta", "lepton_phi"]
    }
)