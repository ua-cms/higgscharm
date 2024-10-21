import numpy as np
from analysis.configs.processor_config import ProcessorConfig
from analysis.configs.histogram_config import HistogramConfig


processor_config = ProcessorConfig(
    goldenjson="analysis/data/Cert_Collisions2022_355100_362760_Golden.txt",
    lumidata="analysis/data/lumi2022.csv",
    hlt_paths=["Ele30_WPTight_Gsf"],
    object_selection={
        "electrons": {
            "expression": "events.Electron",
            "cuts": {
                # impose some quality and minimum pT cuts on the electrons
                "pt": "events.Electron.pt > 30",
                "abseta": "np.abs(events.Electron.eta) < 2.4",
                "electron_id": "wp90iso",
            },
        },
        "dielectrons": {
            "expression": "select_dileptons(objects['electrons'])",
            "cuts": {
                # require minimum dilepton mass and minimum dilepton deltaR
                "dr": "((LorentzVector.delta_r(objects['dielectrons'].z.leading_lepton, objects['dielectrons'].z.subleading_lepton) > 0.02))",
                "mass_window": "(objects['dielectrons'].z.p4.mass < 120.0) & (objects['dielectrons'].z.p4.mass > 60.0)",
            },
        },
    },
    event_selection={
        "atleast_one_goodvertex": "events.PV.npvsGood > 0",
        "lumimask": "get_lumi_mask(events, goldenjson)",
        "trigger": "get_trigger_mask(events, hlt_paths)",
        "trigger_matching": "get_trigger_match_mask(events, events.Electron, hlt_paths)",
        "two_leptons": "ak.num(objects['electrons']) == 2",
        "one_z": "ak.num(objects['dielectrons'].z.p4) == 1",
    },
    histogram_config=HistogramConfig(
        add_syst_axis=True,
        add_weight=True,
        add_cat_axis=None,
        axes={
            "z_mass": {
                "type": "Regular",
                "bins": 100,
                "start": 10,
                "stop": 150,
                "label": r"$m(ee)$ [GeV]",
                "expression": "objects['dielectrons'].z.p4.mass",
            },
            "leading_electron_pt": {
                "type": "Regular",
                "bins": 50,
                "start": 30,
                "stop": 300,
                "label": r"$p_T(e_1)$ [GeV]",
                "expression": "objects['dielectrons'].z.leading_lepton.pt",
            },
            "subleading_electron_pt": {
                "type": "Regular",
                "bins": 50,
                "start": 30,
                "stop": 300,
                "label": r"$p_T(e_2)$ [GeV]",
                "expression": "objects['dielectrons'].z.subleading_lepton.pt",
            },
            "electron_pt": {
                "type": "Regular",
                "bins": 50,
                "start": 30,
                "stop": 300,
                "label": r"$p_T(e)$ [GeV]",
                "expression": "objects['electrons'].pt",
            },
            "electron_eta": {
                "type": "Regular",
                "bins": 50,
                "start": -2.5,
                "stop": 2.5,
                "label": "$\eta(e)$",
                "expression": "objects['electrons'].eta",
            },
            "electron_phi": {
                "type": "Regular",
                "bins": 50,
                "start": -np.pi,
                "stop": np.pi,
                "label": "$\phi(e)$",
                "expression": "objects['electrons'].phi",
            },
        },
        layout={
            "zcandidate": ["z_mass", "leading_electron_pt", "subleading_electron_pt"],
            "electron": ["electron_pt", "electron_eta", "electron_phi"],
        },
    ),
)