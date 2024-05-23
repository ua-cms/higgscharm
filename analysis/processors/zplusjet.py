import copy
import numba
import numpy as np
import awkward as ak
import dask_awkward as dak
from coffea import processor
from coffea.lumi_tools import LumiMask
from coffea.nanoevents import PFNanoAODSchema
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector
from coffea.analysis_tools import Weights, PackedSelection
from analysis.histograms.zplusjet import histograms
from analysis.corrections.muon import MuonWeights
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.jetvetomaps import jetvetomaps_mask

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
    def __init__(self, year: str, config: dict):
        self.year = year
        self.config = config
        self.histograms = histograms

    def process(self, events):
        # check if dataset is MC or Data
        is_mc = hasattr(events, "genWeight")
        
        # --------------------------------------------------------------
        # Object corrections
        # --------------------------------------------------------------
        # apply JEC/JER corrections
        apply_jec = True
        apply_jer = False
        apply_junc = False
        if is_mc:
            apply_jer = True
        apply_jerc_corrections(
            events, 
            era=events.metadata["metadata"]["era"], 
            year=self.year,
            apply_jec=apply_jec,
            apply_jer=apply_jer,
            apply_junc=apply_junc
        )

        # --------------------------------------------------------------
        # Weights
        # --------------------------------------------------------------
        # initialize weights container
        weights_container = Weights(None, storeIndividual=True)
        if is_mc:
            # add genweights
            weights_container.add("genweight", events.genWeight)
            # add pileup weights
            add_pileup_weight(
                events=events,
                year=self.year,
                variation="nominal",
                weights_container=weights_container,
            )
            # add muon id and pfiso weights
            muon_weights = MuonWeights(
                muons=events.Muon,
                year=self.year,
                variation="nominal",
                weights=weights_container,
                id_wp=self.config["muon_id_wp"],
                iso_wp=self.config["muon_iso_wp"],
            )
            muon_weights.add_id_weights()
            muon_weights.add_iso_weights()
        else:
            weights_container.add("genweight", ak.ones_like(events.PV.npvsGood))
            
        # --------------------------------------------------------------
        # Object selection
        # --------------------------------------------------------------
        # impose some quality and minimum pt cuts on the muons
        muons = events.Muon
        muons_id_wps = {
            "loose": muons.looseId,
            "medium": muons.mediumId,
            "tight": muons.tightId,
        }
        muons_iso_wps = {
            "loose": (
                muons.pfRelIso04_all < 0.25
                if hasattr(muons, "pfRelIso04_all")
                else muons.pfRelIso03_all < 0.25
            ),
            "medium": (
                muons.pfRelIso04_all < 0.20
                if hasattr(muons, "pfRelIso04_all")
                else muons.pfRelIso03_all < 0.20
            ),
            "tight": (
                muons.pfRelIso04_all < 0.15
                if hasattr(muons, "pfRelIso04_all")
                else muons.pfRelIso03_all < 0.15
            ),
        }
        muons = muons[
            (muons.pt > 10)
            & (np.abs(muons.eta) < 2.4)
            & (muons_id_wps[self.config["muon_id_wp"]])
            & (muons_iso_wps[self.config["muon_iso_wp"]])
            & (muons.dxy < 0.5)
            & (muons.dz < 1)
            & (muons.sip3d < 4)
        ]
        # impose some quality and minimum pt cuts on the jets
        jets = events.Jet
        jets = jets[(jets.pt >= 30) & (np.abs(jets.eta) < 2.5) & (jets.jetId == 6)]
        # cross-cleaning with muons
        jets = jets[(ak.all(jets.metric_table(muons) > 0.4, axis=-1))]
        # apply veto maps
        jets = jets[jetvetomaps_mask(jets, self.year)]
        # selec c-tagged jets using ParticleNet tight WP
        # https://indico.cern.ch/event/1304360/contributions/5518916/attachments/2692786/4673101/230731_BTV.pdf
        jets_ctagging_wps = {
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
        cjets = jets[jets_ctagging_wps[self.config["tagger"]][self.config["tagger_wp"]]]

        # build lorentz vectors for muons
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
        # find all dimuon candidates with helper function
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

        # --------------------------------------------------------------
        # Event selection
        # --------------------------------------------------------------
        # get luminosity mask
        if is_mc:
            lumi_mask = ak.ones_like(events.PV.npvsGood)
        else:
            lumi_info = LumiMask(self.config["lumimask"])
            lumi_mask = lumi_info(events.run, events.luminosityBlock)
            
        # get trigger mask
        trigger_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
        for hlt_path in self.config["hlt_paths"]:
            if hlt_path in events.HLT.fields:
                trigger_mask = trigger_mask | events.HLT[hlt_path]
                
        # define region selection
        selection = PackedSelection()
        selections = {
            "two_muons": ak.num(muons) == 2,
            "one_cjet": ak.num(cjets) == 1,
            "one_z": ak.num(dimuon.z.p4) == 1,
            "atleast_one_goodvertex": events.PV.npvsGood > 0,
            "lumimask": lumi_mask == 1,
            "trigger": trigger_mask,
        }
        selection.add_multiple(selections)
        region_selection = selection.all(*(selections.keys()))

        # --------------------------------------------------------------
        # Histogram filling
        # --------------------------------------------------------------
        if dak.sum(region_selection) > 0:
            # define feature map with non-flat arrays
            feature_dict = {
                "cjet_pt": cjets.pt[region_selection],
                "cjet_eta": cjets.eta[region_selection],
                "cjet_phi": cjets.phi[region_selection],
                "z_mass": dimuon.z.p4.mass[region_selection],
                "mu1_pt": dimuon.z.mu1.pt[region_selection],
                "mu2_pt": dimuon.z.mu2.pt[region_selection],
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
            histograms = copy.deepcopy(self.histograms)
            if is_mc:
                # get event weight systematic variations for MC samples
                variations = ["nominal"] + list(weights_container.variations)
                for variation in variations:
                    if variation == "nominal":
                        region_weight = weights_container.weight()[region_selection]
                    else:
                        region_weight = weights_container.weight(modifier=variation)[
                            region_selection
                        ]
                    for feature in histograms:
                        fill_args = {
                            feature: feature_dict[feature],
                            "variation": variation,
                            "weight": region_weight,
                        }
                        histograms[feature].fill(**fill_args)
            else:
                region_weight = weights_container.weight()[region_selection]
                for feature in histograms:
                    fill_args = {
                        feature: feature_dict[feature],
                        "variation": "nominal",
                        "weight": region_weight,
                    }
                    histograms[feature].fill(**fill_args)
                    
        return {"histograms": histograms, "sumw": ak.sum(weights_container.weight())}

    def postprocess(self, accumulator):
        pass