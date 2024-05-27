import numpy as np
import awkward as ak
from copy import deepcopy
from coffea import processor
from analysis.configs import load_config
from analysis.working_points import working_points
from analysis.histograms.utils import build_histogram
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.jetvetomaps import jetvetomaps_mask


def normalize(array):
    return ak.fill_none(ak.flatten(array), -99)


class JetTaggersPlots(processor.ProcessorABC):
    def __init__(self, year: str):
        self.year = year

        self.config = load_config(
            config_type="processor", config_name="taggers", year=year
        )
        self.histograms = build_histogram(
            histogram_config=load_config(config_type="histogram", config_name="taggers")
        )

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
            "deepjet": {
                "cvsl": jets.btagDeepFlavCvL,
                "cvsb": jets.btagDeepFlavCvB,
            },
            "pnet": {
                "cvsl": jets.btagPNetCvL,
                "cvsb": jets.btagPNetCvB,
            },
            "part": {
                "cvsl": jets.btagRobustParTAK4CvL,
                "cvsb": jets.btagRobustParTAK4CvB,
            },
        }
        histograms = deepcopy(self.histograms)
        for tagger in feature_map:
            fill_args = {
                f: normalize(feature_map[tagger][f]) for f in feature_map[tagger]
            }
            fill_args.update(
                {
                    "flavor": normalize(ak.values_astype(jets.hadronFlavour, "int32")),
                }
            )
            histograms[tagger].fill(**fill_args)
            
        return {"histograms": histograms}

    def postprocess(self, accumulator):
        pass