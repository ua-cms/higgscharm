import copy
import hist
import numpy as np
import awkward as ak
import hist.dask as hda
from coffea import processor


def normalize(array):
    return ak.fill_none(ak.flatten(array), -99)

class JetTaggersPlots(processor.ProcessorABC):
    def __init__(self):
        # set histograms
        tagger_axis = hist.axis.StrCategory([], growth=True, name="tagger")
        cvsl_axis = hist.axis.Regular(50, 0, 1, name="cvsl", label="CvsL")
        cvsb_axis = hist.axis.Regular(50, 0, 1, name="cvsb", label="CvsB")
        flavor_axis = hist.axis.IntCategory([0, 4, 5], growth=True, name="flavor")
        self.histograms = {
            "deepjet": hda.hist.Hist(cvsl_axis, cvsb_axis, flavor_axis),
            "pnet": hda.hist.Hist(cvsl_axis, cvsb_axis, flavor_axis),
            "part": hda.hist.Hist(cvsl_axis, cvsb_axis, flavor_axis),
        }

    def process(self, events):
        # impose some quality and minimum pt cuts on the muons, electrons and jets
        muons = events.Muon
        muons = muons[
            (muons.pt > 20)
            & (np.abs(muons.eta) < 2.4)
            & (muons.dxy < 0.05)
            & (muons.dz < 0.2)
            & (muons.pfRelIso04_all < 0.25)
            & (muons.looseId)
        ]
        electrons = events.Electron
        electrons = electrons[
            (electrons.pt > 20)
            & (np.abs(electrons.eta) < 2.5)
            & (electrons.pfRelIso03_all < 0.25)
            & (electrons.mvaIso_WP80)
        ]
        jets = events.Jet
        jets = jets[
            (jets.pt > 25)
            & (np.abs(jets.eta) < 2.4)
            & (jets.jetId == 6)
            & (ak.all(jets.metric_table(muons) > 0.4, axis=-1))
            & (ak.all(jets.metric_table(electrons) > 0.4, axis=-1))
        ]
        
        # fill histograms
        histograms = copy.deepcopy(self.histograms)
        histograms["deepjet"].fill(
            cvsl=normalize(jets.btagDeepFlavCvL),
            cvsb=normalize(jets.btagDeepFlavCvB),
            flavor=normalize(ak.values_astype(jets.hadronFlavour, "int32")),
        )
        histograms["pnet"].fill(
            cvsl=normalize(jets.btagPNetCvL),
            cvsb=normalize(jets.btagPNetCvB),
            flavor=normalize(ak.values_astype(jets.hadronFlavour, "int32")),
        )
        histograms["part"].fill(
            cvsl=normalize(jets.btagRobustParTAK4CvL),
            cvsb=normalize(jets.btagRobustParTAK4CvB),
            flavor=normalize(ak.values_astype(jets.hadronFlavour, "int32")),
        )

        return {"histograms": histograms}

    def postprocess(self, accumulator):
        pass