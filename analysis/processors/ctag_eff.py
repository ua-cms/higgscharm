import hist
import dask
import numpy as np
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea import processor
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector


class CTaggingEfficiencyProcessor(processor.ProcessorABC):
    def __init__(self, wp: str = "tight"):
        self.wp = wp

    def process(self, events):
        dataset = events.metadata["dataset"]

        eff_histogram = hda.hist.Hist(
            hist.axis.StrCategory([], growth=True, name="dataset"),
            hist.axis.Variable(
                [20, 30, 50, 70, 100, 140, 200, 300, 600, 1000], name="pt"
            ),
            hist.axis.Regular(10, -2.5, 2.5, name="eta"),
            hist.axis.IntCategory([0, 4, 5], name="flavor"),
            hist.axis.IntCategory([0, 1], name="passWP"),
        )

        phasespace_cuts = (abs(events.Jet.eta) < 2.5) & (events.Jet.pt > 20.0)
        jets = events.Jet[phasespace_cuts]

        # https://indico.cern.ch/event/1304360/contributions/5518916/attachments/2692786/4673101/230731_BTV.pdf
        working_points_mask = {
            "loose": (jets.btagPNetCvB > 0.182) & (jets.btagPNetCvL > 0.054),
            "medium": (jets.btagPNetCvB > 0.304) & (jets.btagPNetCvL > 0.160),
            "tight": (jets.btagPNetCvB > 0.258) & (jets.btagPNetCvL > 0.491),
        }
        passctag = working_points_mask[self.wp]

        eff_histogram.fill(
            dataset=dataset,
            pt=ak.flatten(jets.pt),
            eta=ak.flatten(jets.eta),
            flavor=ak.values_astype(ak.flatten(jets.hadronFlavour), "int64"),
            passWP=ak.flatten(passctag),
        )

        return {"histograms": eff_histogram}

    def postprocess(self, accumulator):
        pass