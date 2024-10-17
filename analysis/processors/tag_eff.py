import numpy as np
import awkward as ak
from copy import deepcopy
from coffea import processor
from analysis.utils import load_config
from analysis.histograms import HistBuilder
from analysis.working_points import working_points
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.jetvetomaps import jetvetomaps_mask


class TaggingEfficiencyProcessor(processor.ProcessorABC):
    def __init__(self, year: str, flavor: str, tagger: str, wp: str):
        self.year = year
        self.flavor = flavor
        self.tagger = tagger
        self.wp = wp

        self.config = load_config(
            config_type="processor", config_name="tag_eff", year=year
        )
        self.histogram_config = load_config(
            config_type="histogram", config_name="tag_eff"
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
        # impose some quality and minimum pt cuts on the jets

        jets = events.Jet
        jets = jets[
            (jets.pt >= self.config.selection["jet"]["pt"])
            & (np.abs(jets.eta) < self.config.selection["jet"]["abs_eta"])
        ]
        if self.config.selection["jet"]["veto_maps"]:
            jets = jets[jetvetomaps_mask(jets, self.year)]
        # Histogram filling

        feature_map = {
            "pt": ak.flatten(jets.pt),
            "eta": ak.flatten(jets.eta),
            "flavor": ak.values_astype(ak.flatten(jets.hadronFlavour), "int32"),
            "pass_wp": ak.flatten(
                working_points.jet_tagger(
                    jets=jets,
                    flavor=self.flavor,
                    tagger=self.tagger,
                    wp=self.wp,
                    year=self.year,
                )
            ),
        }
        histogram = deepcopy(self.histograms)
        for key in self.histogram_config.layout:
            histogram[key].fill(**feature_map)
        return {"histograms": histogram}

    def postprocess(self, accumulator):
        pass