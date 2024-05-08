import numpy as np
import awkward as ak
from coffea import processor

def normalize(array):
    return ak.fill_none(ak.flatten(array), -99)

class JetTaggersPlots(processor.ProcessorABC):
    def process(self, events):
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
        out_dict = {
            "deepjet_cvsl": jets.btagDeepFlavCvL,
            "deepjet_cvsb": jets.btagDeepFlavCvB,
            "pnet_cvsl": jets.btagPNetCvL,
            "pnet_cvsb": jets.btagPNetCvB,
            "part_cvsl": jets.btagRobustParTAK4CvL,
            "part_cvsb": jets.btagRobustParTAK4CvB,
            "flavor": jets.hadronFlavour,
        }

        out_dict = {f: normalize(out_dict[f]) for f in out_dict}

        return out_dict

    def postprocess(self, accumulator):
        pass