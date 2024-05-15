import hist
import copy
import numba
import numpy as np
import awkward as ak
import hist.dask as hda
import dask_awkward as dak
from coffea import processor
from coffea.nanoevents import PFNanoAODSchema
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector

PFNanoAODSchema.warn_missing_crossrefs = False


def normalize(array):
    return ak.fill_none(ak.flatten(array), -99)


@numba.njit
def find_2lep_kernel(events_leptons, builder):
    """Search for valid 2-lepton combinations from an array of events * leptons {charge, ...}

    A valid candidate has a pair of leptons that each have balanced charge
    Outputs an array of events * candidates corresponding to all valid
    permutations of all valid combinations of unique leptons in each event
    (omitting permutations of the pairs)
    """
    for leptons in events_leptons:
        builder.begin_list()
        nlep = len(leptons)
        for i0 in range(nlep):
            for i1 in range(i0 + 1, nlep):
                if len({i0, i1}) < 2:
                    continue
                if leptons[i0].charge + leptons[i1].charge != 0:
                    continue
                builder.begin_tuple(2)
                builder.index(0).integer(i0)
                builder.index(1).integer(i1)
                builder.end_tuple()
        builder.end_list()
    return builder


def find_2lep(events_leptons):
    if ak.backend(events_leptons) == "typetracer":
        # here we fake the output of find_2lep_kernel since
        # operating on length-zero data returns the wrong layout!
        ak.typetracer.length_zero_if_typetracer(
            events_leptons.charge
        )  # force touching of the necessary data
        return ak.Array(ak.Array([[(0, 0)]]).layout.to_typetracer(forget_length=True))
    return find_2lep_kernel(events_leptons, ak.ArrayBuilder()).snapshot()


class ZPlusJetProcessor(processor.ProcessorABC):
    def __init__(self):
        # define histogram axes
        z_mass_axis = hist.axis.Regular(
            100, 10, 150, name="z_mass", label=r"$m(Z)$ [GeV]"
        )
        mu1_pt_axis = hist.axis.Regular(
            50, 0, 300, name="mu1_pt", label=r"$p_T(\mu_1)$ [GeV]"
        )
        mu2_pt_axis = hist.axis.Regular(
            50, 0, 300, name="mu2_pt", label=r"$p_T(\mu_2)$ [GeV]"
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
            name="cjet_z_deltaphi",
            label="$\Delta\phi$(Jet, Z)",
        )
        n_jets_axis = hist.axis.IntCategory(categories=np.arange(0, 16), name="njets")
        n_vertices_axis = hist.axis.Regular(60, 0, 60, name="npvs")
        # define histogram map
        self.histograms = {
            "z_mass": hda.hist.Hist(z_mass_axis, hist.storage.Weight()),
            "mu1_pt": hda.hist.Hist(mu1_pt_axis, hist.storage.Weight()),
            "mu2_pt": hda.hist.Hist(mu2_pt_axis, hist.storage.Weight()),
            "cjet_pt": hda.hist.Hist(jet_pt_axis, hist.storage.Weight()),
            "cjet_eta": hda.hist.Hist(jet_eta_axis, hist.storage.Weight()),
            "cjet_phi": hda.hist.Hist(jet_phi_axis, hist.storage.Weight()),
            "cjet_z_deltaphi": hda.hist.Hist(deltaphi_axis, hist.storage.Weight()),
            "njets": hda.hist.Hist(n_jets_axis, hist.storage.Weight()),
            "npvs": hda.hist.Hist(n_vertices_axis, hist.storage.Weight()),
        }

    def process(self, events):
        # copy histogram map
        histograms = copy.deepcopy(self.histograms)
        # impose some quality and minimum pt cuts on the muons
        muons = events.Muon
        muons = muons[
            (muons.pt > 10)
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
        # selec c-tagged jets using ParticleNet tight WP
        cjets = jets[(jets.btagPNetCvB > 0.258) & (jets.btagPNetCvL > 0.491)]
        # build Lorentz vectors for muons
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
        dimuon = dak.map_partitions(find_2lep, muons)
        dimuon = [muons[dimuon[idx]] for idx in "01"]
        dimuon = ak.zip(
            {
                "z": ak.zip(
                    {
                        "mu1": dimuon[0],
                        "mu2": dimuon[1],
                        "p4": dimuon[0] + dimuon[1],
                    }
                )
            }
        )
        # require minimum dimuon mass and minimum dimuon deltaR
        z_mass_window = (
            (LorentzVector.delta_r(dimuon.z.mu1, dimuon.z.mu2) > 0.02)
            & (dimuon.z.p4.mass < 120.0)
            & (dimuon.z.p4.mass > 12.0)
        )
        dimuon = dimuon[z_mass_window]
        # select events with:
        #    2 muons
        #    one c-tagged jet
        #    one Z candidate
        #    at least one good vertex
        two_muons = ak.num(muons) == 2
        one_cjet = ak.num(cjets) == 1
        one_z = ak.num(dimuon.z.p4) == 1
        atleast_one_goodvertex = events.PV.npvsGood > 0
        region_selection = two_muons & one_cjet & one_z & atleast_one_goodvertex

        # define feature map with non-flat arrays
        feature_dict = {
            # c-jet
            "cjet_pt": cjets.pt[region_selection],
            "cjet_eta": cjets.eta[region_selection],
            "cjet_phi": cjets.phi[region_selection],
            # z candidate
            "z_mass": dimuon.z.p4.mass[region_selection],
            "mu1_pt": dimuon.z.mu1.pt[region_selection],
            "mu2_pt": dimuon.z.mu2.pt[region_selection],
            # c-jet and z candidate
            "cjet_z_deltaphi": LorentzVector.delta_phi(
                ak.pad_none(cjets[region_selection], 1),
                ak.pad_none(dimuon.z.p4[region_selection], 1),
            ),
        }
        feature_dict = {f: normalize(feature_dict[f]) for f in feature_dict}
        # update feature map with flat arrays
        feature_dict.update(
            {
                # jet multiplicity
                "njets": ak.num(jets[region_selection]),
                # number of primary vertices
                "npvs": events.PV.npvsGood[region_selection],
            },
        )
        
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