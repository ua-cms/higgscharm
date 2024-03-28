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
    def __init__(self, wp="tight", tagger="pnet"):
        self.wp = wp
        self.tagger = tagger

    def process(self, events):
        dataset = events.metadata["dataset"]

        eff_histogram = hda.hist.Hist(
            hist.axis.StrCategory([], growth=True, name="dataset"),
            hist.axis.Variable(
                [20, 30, 50, 70, 100, 140, 200, 300, 600, 1000], name="pt"
            ),
            hist.axis.Regular(10, -2.5, 2.5, name="eta"),
            hist.axis.IntCategory([0, 4, 5], name="flavor"),
            hist.axis.IntCategory([0, 1], name="pass_wp"),
        )

        phasespace_cuts = (abs(events.Jet.eta) < 2.5) & (events.Jet.pt > 20.0)
        jets = events.Jet[phasespace_cuts]

        # https://indico.cern.ch/event/1304360/contributions/5518916/attachments/2692786/4673101/230731_BTV.pdf
        working_points_mask = {
            "deepjet": {
                "loose": (jets.btagDeepFlavCvB > 0.206)
                & (jets.btagDeepFlavCvL > 0.042),
                "medium": (jets.btagDeepFlavCvB > 0.298)
                & (jets.btagDeepFlavCvL > 0.108),
                "tight": (jets.btagDeepFlavCvB > 0.241)
                & (jets.btagDeepFlavCvL > 0.305),
            },
            "pnet": {
                "loose": (jets.btagPNetCvB > 0.182) & (jets.btagPNetCvL > 0.054),
                "medium": (jets.btagPNetCvB > 0.304) & (jets.btagPNetCvL > 0.160),
                "tight": (jets.btagPNetCvB > 0.258) & (jets.btagPNetCvL > 0.491),
            },
            "part": {
                "loose": (jets.btagRobustParTAK4CvB > 0.067)
                & (jets.btagRobustParTAK4CvL > 0.0390),
                "medium": (jets.btagRobustParTAK4CvB > 0.128)
                & (jets.btagRobustParTAK4CvL > 0.117),
                "tight": (jets.btagRobustParTAK4CvB > 0.095)
                & (jets.btagRobustParTAK4CvL > 0.358),
            },
        }
        passctag = working_points_mask[self.tagger][self.wp]

        eff_histogram.fill(
            dataset=dataset,
            pt=ak.flatten(jets.pt),
            eta=ak.flatten(jets.eta),
            flavor=ak.values_astype(ak.flatten(jets.hadronFlavour), "int32"),
            pass_wp=ak.flatten(passctag),
        )

        return {"histograms": eff_histogram}

    def postprocess(self, accumulator):
        pass