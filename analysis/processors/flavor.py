import numba
import numpy as np
import awkward as ak
import dask_awkward as dak
from copy import deepcopy
from coffea import processor
from coffea.nanoevents import PFNanoAODSchema
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector
from coffea.analysis_tools import Weights, PackedSelection
from coffea.lumi_tools import LumiData, LumiList, LumiMask
from analysis.utils import load_config
from analysis.utils.trigger_matching import trigger_match
from analysis.working_points import working_points
from analysis.histograms import HistBuilder, fill_histogram
from analysis.corrections.muon import MuonWeights
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.jetvetomaps import jetvetomaps_mask

PFNanoAODSchema.warn_missing_crossrefs = False

def normalize(array):
    if array.ndim == 2:
        return ak.fill_none(ak.flatten(array), -99)
    else:
        return ak.fill_none(array, -99)

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


class FlavorProcessor(processor.ProcessorABC):
    def __init__(self, year: str):
        self.year = year

        self.config = load_config(
            config_type="processor", config_name="flavor", year=year
        )
        self.histogram_config = load_config(
            config_type="histogram", config_name="flavor"
        )
        self.histograms = HistBuilder(self.histogram_config).build_histogram()

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
                id_wp=self.config.selection["muon"]["id_wp"],
                iso_wp=self.config.selection["muon"]["iso_wp"],
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
        muons = muons[
            (muons.pt > self.config.selection["muon"]["pt"])
            & (np.abs(muons.eta) < self.config.selection["muon"]["abs_eta"])
            & (muons.dxy < self.config.selection["muon"]["dxy"])
            & (muons.dz < self.config.selection["muon"]["dz"])
            & (muons.sip3d < self.config.selection["muon"]["sip3d"])
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
        # impose some quality and minimum pt cuts on the jets
        jets = events.Jet
        jets = jets[
            (jets.pt >= self.config.selection["jet"]["pt"])
            & (np.abs(jets.eta) < self.config.selection["jet"]["abs_eta"])
            & (jets.jetId == self.config.selection["jet"]["id"])
        ]
        if self.config.selection["jet"]["delta_r_lepton"]:
            jets = jets[(ak.all(jets.metric_table(muons) > 0.4, axis=-1))]
        if self.config.selection["jet"]["veto_maps"]:
            jets = jets[jetvetomaps_mask(jets, self.year)]
        if self.config.selection["jet"]["c_pnet_wp"]:
            jets = jets[
                working_points.jet_tagger(
                    jets=jets,
                    flavor="c",
                    tagger="pnet",
                    wp=self.config.selection["jet"]["c_pnet_wp"],
                    year=self.year,
                )
            ]

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
            & (dimuon.z.p4.mass > 60.0)
        )
        dimuon = dimuon[z_mass_window]

        # --------------------------------------------------------------
        # Event selection
        # --------------------------------------------------------------
        # get luminosity mask
        if is_mc:
            lumi_mask = ak.ones_like(events.PV.npvsGood)
        else:
            lumi_info = LumiMask(self.config.lumimask)
            lumi_mask = lumi_info(events.run, events.luminosityBlock)
            
            # get integrated luminosity in pb^-1
            lumi_data = LumiData(self.config.lumidata)
            lumi_list = LumiList(
                events[lumi_mask].run, events[lumi_mask].luminosityBlock
            )
            lumi = lumi_data.get_lumi(lumi_list)

        # get trigger mask and DeltaR matched trigger objects mask
        trig_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
        trig_match_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
        for hlt_path in self.config.hlt_paths:
            if hlt_path in events.HLT.fields:
                trig_mask = trig_mask | events.HLT[hlt_path]
                trig_obj_mask = trigger_match(
                    leptons=events.Muon,
                    trigobjs=events.TrigObj,
                    hlt_path=hlt_path,
                )
                trig_match_mask = trig_match_mask | trig_obj_mask

        # define region selection
        selection = PackedSelection()
        selections = {
            "two_muons": ak.num(muons) == 2,
            "one_z": ak.num(dimuon.z.p4) == 1,
            "atleast_one_goodvertex": events.PV.npvsGood > 0,
            "lumimask": lumi_mask == 1,
            "trigger": trig_mask,
            "trigger_matching": dak.sum(trig_match_mask, axis=-1) > 0,
        }
        selection.add_multiple(selections)
        region_selection = selection.all(*(selections.keys()))

        # --------------------------------------------------------------
        # Histogram filling
        # --------------------------------------------------------------
        if dak.sum(region_selection) > 0:
            histograms = deepcopy(self.histograms)
            if not is_mc:
                jets["hadronFlavour"] = ak.ones_like(jets.pt)
            # define feature map with non-flat arrays
            feature_map = {
                "jet_pt": jets.pt[region_selection],
                "jet_eta": jets.eta[region_selection],
                "jet_phi": jets.phi[region_selection],
                "pnet_cvsb": jets.btagPNetCvB[region_selection],
                "pnet_cvsl": jets.btagPNetCvL[region_selection],
                "flavor": ak.values_astype(jets.hadronFlavour[region_selection], "int32"),
            }
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
                    for key, features in self.histogram_config.layout.items():
                        fill_args = {}
                        for feature in features:
                            fill_args[feature] = normalize(feature_map[feature])

                        fill_args.update(
                            {
                                "variation": variation,
                                "weight": (
                                    ak.flatten(
                                        ak.ones_like(feature_map[feature])
                                        * region_weight
                                    )
                                    if feature_map[feature].ndim == 2
                                    else region_weight
                                )
                            }
                        )
                        histograms[key].fill(**fill_args)
            else:
                region_weight = weights_container.weight()[region_selection]
                for key, features in self.histogram_config.layout.items():
                    fill_args = {}
                    for feature in features:
                        fill_args[feature] = normalize(feature_map[feature])
                    fill_args.update(
                        {
                            "variation": "nominal",
                            "weight": (
                                ak.flatten(
                                    ak.ones_like(feature_map[feature])
                                    * region_weight
                                )
                                if feature_map[feature].ndim == 2
                                else region_weight
                            )
                        }
                    )
                    histograms[key].fill(**fill_args)

        output = {"histograms": histograms, "sumw": ak.sum(weights_container.weight())}
        if not is_mc:
            output.update({"lumi": lumi})
            
        return output
    
    def postprocess(self, accumulator):
        pass