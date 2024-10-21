import numpy as np
from analysis.configs.processor_config import ProcessorConfig
from analysis.configs.histogram_config import HistogramConfig


processor_config = ProcessorConfig(
    goldenjson="analysis/data/Cert_Collisions2022_355100_362760_Golden.txt",
    lumidata="analysis/data/lumi2022.csv",
    hlt_paths=["IsoMu24"],
    object_selection={
        "muons": {
            "expression": "events.Muon",
            "cuts": {
                # impose some quality and minimum pT cuts on the muons
                "pt": "events.Muon.pt > 24",
                "abseta": "np.abs(events.Muon.eta) < 2.4",
                "dxy": "events.Muon.dxy < 0.5",
                "dz": "events.Muon.dz < 1",
                "sip3d": "events.Muon.sip3d < 4",
                "muon_id": "tight",
                "muon_iso": "tight",
            },
        },
        "dimuons": {
            "expression": "select_dileptons(objects['muons'])",
            "cuts": {
                # require minimum dilepton mass and minimum dilepton deltaR
                "dr": "((LorentzVector.delta_r(objects['dimuons'].z.leading_lepton, objects['dimuons'].z.subleading_lepton) > 0.02))",
                "mass_window": "(objects['dimuons'].z.p4.mass < 120.0) & (objects['dimuons'].z.p4.mass > 60.0)",
            },
        },
    },
    event_selection={
        "atleast_one_goodvertex": "events.PV.npvsGood > 0",
        "lumimask": "get_lumi_mask(events, goldenjson)",
        "trigger": "get_trigger_mask(events, hlt_paths)",
        "trigger_matching": "get_trigger_match_mask(events, events.Muon, hlt_paths)",
        "two_leptons": "ak.num(objects['muons']) == 2",
        "one_z": "ak.num(objects['dimuons'].z.p4) == 1",
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
                "label": r"$m(\mu\mu)$ [GeV]",
                "expression": "objects['dimuons'].z.p4.mass",
            },
            "leading_muon_pt": {
                "type": "Regular",
                "bins": 50,
                "start": 24,
                "stop": 300,
                "label": r"$p_T(\mu_1)$ [GeV]",
                "expression": "objects['dimuons'].z.leading_lepton.pt",
            },
            "subleading_muon_pt": {
                "type": "Regular",
                "bins": 50,
                "start": 24,
                "stop": 300,
                "label": r"$p_T(\mu_2)$ [GeV]",
                "expression": "objects['dimuons'].z.subleading_lepton.pt",
            },
            "muon_pt": {
                "type": "Regular",
                "bins": 50,
                "start": 24,
                "stop": 300,
                "label": r"$p_T(\mu)$ [GeV]",
                "expression": "objects['muons'].pt",
            },
            "muon_eta": {
                "type": "Regular",
                "bins": 50,
                "start": -2.5,
                "stop": 2.5,
                "label": "$\eta(\mu)$",
                "expression": "objects['muons'].eta",
            },
            "muon_phi": {
                "type": "Regular",
                "bins": 50,
                "start": -np.pi,
                "stop": np.pi,
                "label": "$\phi(\mu)$",
                "expression": "objects['muons'].phi",
            },
        },
        layout={
            "zcandidate": ["z_mass", "leading_muon_pt", "subleading_muon_pt"],
            "muon": ["muon_pt", "muon_eta", "muon_phi"],
        },
    ),
)
