import hist
import dask
import copy
import numba
import numpy as np
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea import processor
from collections import defaultdict
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector


def normalize(array):
    return ak.fill_none(ak.flatten(array), -99)


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
        ak.typetracer.length_zero_if_typetracer(
            events_leptons.charge
        )  # force touching of the necessary data
        return ak.Array(
            ak.Array([[(0, 0, 0, 0)]]).layout.to_typetracer(forget_length=True)
        )
    return find_4lep_kernel(events_leptons, ak.ArrayBuilder()).snapshot()


class SignalProcessor(processor.ProcessorABC):
    def __init__(self):
        # set histogram axes
        higgs_mass_axis = hist.axis.Regular(
            120, 10, 150, name="higgs_mass", label=r"$m(H)$ [GeV]"
        )
        higgs_pt_axis = hist.axis.Regular(
            40, 0, 300, name="higgs_pt", label=r"$p_T(H)$ [GeV]"
        )
        z1_mass_axis = hist.axis.Regular(
            100, 10, 150, name="z1_mass", label=r"$m(Z)$ [GeV]"
        )
        z2_mass_axis = hist.axis.Regular(
            50, 10, 150, name="z2_mass", label=r"$m(Z^*)$ [GeV]"
        )
        jet_pt_axis = hist.axis.Regular(
            30, 30, 150, name="cjet_pt", label=r"Jet $p_T$ [GeV]"
        )
        jet_eta_axis = hist.axis.Regular(
            bins=50, start=-2.5, stop=2.5, name="cjet_eta", label="Jet $\eta$"
        )
        jet_phi_axis = hist.axis.Regular(
            bins=50, start=-np.pi, stop=np.pi, name="cjet_phi", label="Jet $\phi$"
        )
        deltaphi_axis = hist.axis.Regular(
            bins=50,
            start=-np.pi,
            stop=np.pi,
            name="cjet_higgs_deltaphi",
            label="$\Delta\phi$(Jet, H)",
        )
        # set histogram map
        self.histograms = {
            "z1_mass": hda.hist.Hist(z1_mass_axis, hist.storage.Weight()),
            "z2_mass": hda.hist.Hist(z2_mass_axis, hist.storage.Weight()),
            "higgs_mass": hda.hist.Hist(higgs_mass_axis, hist.storage.Weight()),
            "higgs_pt": hda.hist.Hist(higgs_pt_axis, hist.storage.Weight()),
            "cjet_pt": hda.hist.Hist(jet_pt_axis, hist.storage.Weight()),
            "cjet_eta": hda.hist.Hist(jet_eta_axis, hist.storage.Weight()),
            "cjet_phi": hda.hist.Hist(jet_phi_axis, hist.storage.Weight()),
            "cjet_higgs_deltaphi": hda.hist.Hist(deltaphi_axis, hist.storage.Weight()),
        }

    def process(self, events):
        # copy histogram map
        histograms = copy.deepcopy(self.histograms)
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
        # impose some quality and minimum pt cuts on the jets
        jets = events.Jet
        jets = jets[(jets.pt >= 30) & (np.abs(jets.eta) < 2.5) & (jets.jetId == 6)]
        # cross-cleaning of jets with respect to muons
        jets = jets[(ak.all(jets.metric_table(muons) > 0.4, axis=-1))]
        # selec c-tagged jets (ParticleNet tight WP)
        cjets = jets[(jets.btagPNetCvB > 0.258) & (jets.btagPNetCvL > 0.491)]

        # build Lorentz vectors
        muons = ak.zip(
            {
                "pt": muons.pt,
                "eta": muons.eta,
                "phi": muons.phi,
                "mass": muons.mass,
                "charge": muons.charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # make sure they are sorted by transverse momentum
        muons = muons[ak.argsort(muons.pt, axis=1)]
        # find all candidates with helper function
        fourmuon = dak.map_partitions(find_4lep, muons)
        fourmuon = [muons[fourmuon[idx]] for idx in "0123"]
        fourmuon = ak.zip(
            {
                "z1": ak.zip(
                    {
                        "lep1": fourmuon[0],
                        "lep2": fourmuon[1],
                        "p4": fourmuon[0] + fourmuon[1],
                    }
                ),
                "z2": ak.zip(
                    {
                        "lep1": fourmuon[2],
                        "lep2": fourmuon[3],
                        "p4": fourmuon[2] + fourmuon[3],
                    }
                ),
            }
        )
        # require minimum dimuon mass and minimum dimuon deltaR
        z1_mass_window = (
            (LorentzVector.delta_r(fourmuon.z1.lep1, fourmuon.z1.lep2) > 0.02)
            & (fourmuon.z1.p4.mass < 120.0)
            & (fourmuon.z1.p4.mass > 12.0)
        )
        z2_mass_window = (
            (LorentzVector.delta_r(fourmuon.z2.lep1, fourmuon.z2.lep2) > 0.02)
            & (fourmuon.z2.p4.mass < 120.0)
            & (fourmuon.z2.p4.mass > 12.0)
        )
        fourmuon = fourmuon[z1_mass_window & z2_mass_window]
        # choose permutation with z1 mass closest to nominal Z boson mass
        bestz1 = ak.singletons(ak.argmin(abs(fourmuon.z1.p4.mass - 91.1876), axis=1))
        fourmuon = fourmuon[bestz1]
        # select events with 4 muons, one c-tagged jet and one higgs candidate
        four_muons = ak.num(muons) == 4
        one_cjet = ak.num(cjets) == 1
        one_higgs = ak.num((fourmuon.z1.p4 + fourmuon.z2.p4)) == 1
        region_selection = four_muons & one_cjet & one_higgs

        # define feature map with non-flat arrays
        feature_dict = {
            "higgs_pt": (fourmuon.z1.p4 + fourmuon.z2.p4).pt[region_selection],
            "higgs_mass": (fourmuon.z1.p4 + fourmuon.z2.p4).mass[region_selection],
            "cjet_pt": cjets.pt[region_selection],
            "cjet_eta": cjets.eta[region_selection],
            "cjet_phi": cjets.phi[region_selection],
            "z1_mass": fourmuon.z1.p4.mass[region_selection],
            "z2_mass": fourmuon.z2.p4.mass[region_selection],
            "z1_mu1_pt": fourmuon.z1.lep1.pt[region_selection],
            "z1_mu2_pt": fourmuon.z1.lep2.pt[region_selection],
            "z2_mu1_pt": fourmuon.z2.lep1.pt[region_selection],
            "z2_mu2_pt": fourmuon.z2.lep2.pt[region_selection],
            "cjet_higgs_deltaphi": LorentzVector.delta_phi(
                ak.pad_none(cjets[region_selection], 1),
                ak.pad_none((fourmuon.z1.p4 + fourmuon.z2.p4)[region_selection], 1),
            ),
        }
        feature_dict = {f: normalize(feature_dict[f]) for f in feature_dict}

        # get region weights
        weights = events.genWeight
        region_weights = weights[region_selection]
        
        # fill histograms
        for feature in histograms:
            fill_args = {
                feature: feature_dict[feature],
                "weight": region_weights,
            }
            histograms[feature].fill(**fill_args)

        return {"histograms": histograms, "sumw": ak.sum(weights)}

    def postprocess(self, accumulator):
        pass