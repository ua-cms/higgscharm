import numpy as np
import awkward as ak
from copy import deepcopy
from coffea import processor
from analysis.utils import load_config
from analysis.histograms import HistBuilder
from analysis.working_points import working_points
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.jetvetomaps import jetvetomaps_mask


def normalize(array):
    if array.ndim == 2:
        return ak.fill_none(ak.flatten(array), -99)
    else:
        return ak.fill_none(array, -99)


class JetTaggersPlots(processor.ProcessorABC):
    def __init__(self, year: str):
        self.year = year

        self.config = load_config(
            config_type="processor", config_name="taggers", year=year
        )
        self.histogram_config = load_config(
            config_type="histogram", config_name="taggers"
        )
        self.histograms = HistBuilder(self.histogram_config).build_histogram()

    def process(self, events):
        # apply JEC/JER corrections
        apply_jerc_corrections(
            events,
            era=events.metadata["metadata"]["era"],
            year=self.year,
            apply_jec=True,
            apply_jer=True,
            apply_junc=False,
        )

        # impose some quality and minimum pt cuts on the muons, electrons and jets
        muons = events.Muon
        muons = muons[
            (muons.pt > self.config.selection["muon"]["pt"])
            & (np.abs(muons.eta) < self.config.selection["muon"]["abs_eta"])
            & (muons.dxy < self.config.selection["muon"]["dxy"])
            & (muons.dz < self.config.selection["muon"]["dz"])
            & (
                working_points.muon_id(
                    muons=muons, wp=self.config.selection["muon"]["id_wp"]
                )
            )
            & (
                working_points.muon_iso(
                    muons=muons, wp=self.config.selection["muon"]["iso_wp"]
                )
            )
        ]
        electrons = events.Electron
        electrons = electrons[
            (electrons.pt > self.config.selection["electron"]["pt"])
            & (np.abs(electrons.eta) < self.config.selection["electron"]["abs_eta"])
            & (
                working_points.electron_id(
                    electrons=electrons, wp=self.config.selection["electron"]["id_wp"]
                )
            )
        ]
        if "noiso" in self.config.selection["electron"]["id_wp"]:
            electrons = electrons[
                working_points.electron_iso(
                    muons=muons, wp=self.config.selection["muon"]["id_wp"]
                )
            ]
        jets = events.Jet
        jets = jets[
            (jets.pt > self.config.selection["jet"]["pt"])
            & (np.abs(jets.eta) < self.config.selection["jet"]["abs_eta"])
            & (jets.jetId == self.config.selection["jet"]["id"])
        ]
        if self.config.selection["jet"]["delta_r_lepton"]:
            jets = jets[
                (ak.all(jets.metric_table(muons) > 0.4, axis=-1))
                & (ak.all(jets.metric_table(electrons) > 0.4, axis=-1))
            ]
        if self.config.selection["jet"]["veto_maps"]:
            jets = jets[jetvetomaps_mask(jets, self.year)]
            
        # fill histograms
        feature_map = {
            "deepjet_cvsl": jets.btagDeepFlavCvL,
            "deepjet_cvsb": jets.btagDeepFlavCvB,
            "pnet_cvsl": jets.btagPNetCvL,
            "pnet_cvsb": jets.btagPNetCvB,
            "part_cvsl": jets.btagRobustParTAK4CvL,
            "part_cvsb": jets.btagRobustParTAK4CvB,
            "flavor": ak.values_astype(jets.hadronFlavour, "int32"),
        }
        histograms = deepcopy(self.histograms)
        for key, features in self.histogram_config.layout.items():
            fill_args = {}
            for feature in features:
                fill_args[feature] = normalize(feature_map[feature])
            histograms[key].fill(**fill_args)
            
        return {"histograms": histograms}

    def postprocess(self, accumulator):
        pass