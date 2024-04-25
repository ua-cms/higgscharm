import numba
import hist
import dask
import numpy as np
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea import processor
from collections import defaultdict
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector


@numba.njit
def find_4lep_kernel(events_leptons, builder):
    """Search for valid 4-lepton combinations from an array of events * leptons {charge, ...}

    A valid candidate has two pairs of leptons that each have balanced charge
    Outputs an array of events * candidates {indices 0..3} corresponding to all valid
    permutations of all valid combinations of unique leptons in each event
    (omitting permutations of the pairs)
    """
    for leptons in events_leptons:
        builder.begin_list()
        nlep = len(leptons)
        for i0 in range(nlep):
            for i1 in range(i0 + 1, nlep):
                # A leading muon with pT > 20 GeV
                # A subleading muon with pT > 10 GeV
                # Remaining muons satisfy pT > 5 GeV
                if leptons[i0].pt < 20:
                    continue
                if leptons[i1].pt < 10:
                    continue
                if leptons[i0].charge + leptons[i1].charge != 0:
                    continue
                for i2 in range(nlep):
                    for i3 in range(i2 + 1, nlep):
                        if len({i0, i1, i2, i3}) < 4:
                            continue
                        if leptons[i2].charge + leptons[i3].charge != 0:
                            continue
                        builder.begin_tuple(4)
                        builder.index(0).integer(i0)
                        builder.index(1).integer(i1)
                        builder.index(2).integer(i2)
                        builder.index(3).integer(i3)
                        builder.end_tuple()
        builder.end_list()

    return builder


def find_4lep(events_leptons):
    if ak.backend(events_leptons) == "typetracer":
        # here we fake the output of find_4lep_kernel since
        # operating on length-zero data returns the wrong layout!
        ak.typetracer.length_zero_if_typetracer(events_leptons.charge) # force touching of the necessary data
        return ak.Array(ak.Array([[(0,0,0,0)]]).layout.to_typetracer(forget_length=True))
    return find_4lep_kernel(events_leptons, ak.ArrayBuilder()).snapshot()


class SignalProcessor(processor.ProcessorABC):
    def process(self, events):
        dataset = events.metadata['dataset']
        
        # -----------------------------
        # HIGGS SELECTION
        # -----------------------------
        # impose some quality and minimum pt cuts on the muons
        muons = events.Muon
        muons = muons[
            (muons.pt > 5)
            & (np.abs(muons.eta) < 2.4)
            & (muons.dxy < 0.5)
            & (muons.dz < 1)
            & (muons.pfRelIso04_all < 0.35)
            & (muons.sip3d < 4)
            & (muons.mediumId)
        ]
        jets = events.Jet
        jets = jets[
            (jets.pt >= 30) 
            & (np.abs(jets.eta) < 2.5) 
            & (jets.jetId == 6)
            & ((jets.btagDeepFlavCvB > 0.241) & (jets.btagDeepFlavCvL > 0.305))
            & (ak.all(jets.metric_table(muons) > 0.4, axis=-1))
        ]
        
        # build Lorentz vectors
        muons = ak.zip({
            "pt": muons.pt,
            "eta": muons.eta,
            "phi": muons.phi,
            "mass": muons.mass,
            "charge": muons.charge,
        }, with_name="PtEtaPhiMCandidate", behavior=candidate.behavior)
        
        # make sure they are sorted by transverse momentum
        muons = muons[ak.argsort(muons.pt, axis=1)]

        # find all candidates with helper function
        fourmuon = dak.map_partitions(find_4lep, muons)
        fourmuon = [muons[fourmuon[idx]] for idx in "0123"]
        
        fourmuon = ak.zip({
            "z1": ak.zip({
                "lep1": fourmuon[0],
                "lep2": fourmuon[1],
                "p4": fourmuon[0] + fourmuon[1],
            }),
            "z2": ak.zip({
                "lep1": fourmuon[2],
                "lep2": fourmuon[3],
                "p4": fourmuon[2] + fourmuon[3],
            }),
        })        
        # require minimum deltaR
        fourmuon = fourmuon[LorentzVector.delta_r(fourmuon.z1.p4, fourmuon.z2.p4) > 0.02]

        # require minimum dimuon mass
        z1_mass_window = (fourmuon.z1.p4.mass < 120.) & (fourmuon.z1.p4.mass > 12.)
        z2_mass_window = (fourmuon.z2.p4.mass < 120.) & (fourmuon.z2.p4.mass > 12.)
        fourmuon = fourmuon[z1_mass_window & z2_mass_window]

        # choose permutation with z1 mass closest to nominal Z boson mass
        bestz1 = ak.singletons(ak.argmin(abs(fourmuon.z1.p4.mass - 91.1876), axis=1))
        fourmuon = fourmuon[bestz1]
        
        # select events with 4 muons and one c-tagged jet
        region_selection = (ak.num(jets) == 1) & (ak.num(muons) == 4)
        
        # weights
        weights = events.genWeight
        region_weights = weights[region_selection]
        
        out_dict = {
            "higgs_mass": (fourmuon.z1.p4 + fourmuon.z2.p4).mass[region_selection],
            "higgs_pt": (fourmuon.z1.p4 + fourmuon.z2.p4).pt[region_selection],
            "z1_mass": fourmuon.z1.p4.mass[region_selection],
            "z2_mass": fourmuon.z2.p4.mass[region_selection],
            "z1_mu1_pt": fourmuon.z1.lep1.pt[region_selection],
            "z1_mu2_pt": fourmuon.z1.lep2.pt[region_selection],
            "z2_mu1_pt": fourmuon.z2.lep1.pt[region_selection],
            "z2_mu2_pt": fourmuon.z2.lep2.pt[region_selection],
            "jet_pt": jets.pt[region_selection],
            "jet_eta": jets.eta[region_selection],
            "jet_phi": jets.phi[region_selection],
            "weights": region_weights,
            "sumw": ak.sum(weights),
        }
        return out_dict
    
    def postprocess(self, accumulator):
        pass