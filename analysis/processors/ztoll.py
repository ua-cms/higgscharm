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
from analysis.working_points import working_points
from analysis.utils import load_config
from analysis.utils.trigger_matching import trigger_match
from analysis.histograms import HistBuilder, fill_histogram
from analysis.corrections.muon import MuonWeights
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.electron import ElectronWeights, ElectronSS


PFNanoAODSchema.warn_missing_crossrefs = False


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


class ZtoLLProcessor(processor.ProcessorABC):
    def __init__(self, year: str, lepton_flavor: str):
        self.year = year
        self.lepton_flavor = lepton_flavor
        self.config = load_config(
            config_type="processor", config_name="ztoll", year=year
        )
        self.histogram_config = load_config(
            config_type="histogram", config_name="ztoll"
        )
        self.histograms = HistBuilder(self.histogram_config).build_histogram()

    def process(self, events):
        # check if dataset is MC or Data
        is_mc = hasattr(events, "genWeight")

        # initialize output dictionary
        output = {}
        # initialize metadata info
        nevents = ak.num(events, axis=0)
        output["metadata"] = {}
        output["metadata"].update({"raw_initial_nevents": nevents})

        # -------------------------------------------------------------
        # Object corrections
        # -------------------------------------------------------------
        # electron scale and smearing corrections
        electron_ss = ElectronSS(
            events=events,
            year=self.year,
            variation="nominal",
        )
        if is_mc:
            # energies in MC are smeared
            electron_ss.apply_smearing()
        else:
            # energies in data are scaled
            electron_ss.apply_scale()

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
            if self.lepton_flavor == "muon":
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

            elif self.lepton_flavor == "electron":
                # add electron Id, Reco and HLT weights
                electron_weights = ElectronWeights(
                    events=events,
                    year=self.year,
                    weights=weights_container,
                    variation="nominal",
                    id_wp=self.config.selection["electron"]["id_wp"],
                    hlt_paths=self.config.hlt_paths["electron"],
                    
                )
                electron_weights.add_id_weights()
                electron_weights.add_hlt_weights()
                electron_weights.add_reco_weights("RecoBelow20")
                electron_weights.add_reco_weights("Reco20to75")
                electron_weights.add_reco_weights("RecoAbove75")
        else:
            weights_container.add("genweight", ak.ones_like(events.PV.npvsGood))

        # save nevents (sum of weights) before selections
        sumw = ak.sum(weights_container.weight())
        output["metadata"].update({"sumw": sumw})

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
        # impose some quality and minimum pt cuts on the electrons
        electrons = events.Electron
        electrons = electrons[
            (electrons.pt > self.config.selection["electron"]["pt"])
            & (np.abs(electrons.eta) < self.config.selection["electron"]["abs_eta"])
            & (
                working_points.electron_id(
                    electrons=electrons, wp=self.config.selection["electron"]["id_wp"]
                )
            )
        ]
        leptons = muons if self.lepton_flavor == "muon" else electrons
        # build lorentz vectors for leptons
        leptons = ak.zip(
            {
                "pt": leptons.pt,
                "eta": leptons.eta,
                "phi": leptons.phi,
                "mass": leptons.mass,
                "charge": leptons.charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # make sure they are sorted by transverse momentum
        leptons = leptons[ak.argsort(leptons.pt, axis=1)]
        # find all dilepton candidates with helper function
        dilepton = dak.map_partitions(find_2lep, leptons)
        dilepton = [leptons[dilepton[idx]] for idx in "01"]
        dilepton = ak.zip(
            {
                "z": ak.zip(
                    {
                        "leading_lepton": dilepton[0],
                        "subleading_lepton": dilepton[1],
                        "p4": dilepton[0] + dilepton[1],
                    }
                )
            }
        )
        # require minimum dilepton mass and minimum dilepton deltaR
        z_mass_window = (
            (
                LorentzVector.delta_r(
                    dilepton.z.leading_lepton, dilepton.z.subleading_lepton
                )
                > 0.02
            )
            & (dilepton.z.p4.mass < 120.0)
            & (dilepton.z.p4.mass > 60.0)
        )
        dilepton = dilepton[z_mass_window]

        # --------------------------------------------------------------
        # Event selection
        # --------------------------------------------------------------
        # get luminosity mask
        if is_mc:
            lumi_mask = ak.ones_like(events.PV.npvsGood)
        else:
            lumi_info = LumiMask(self.config.lumimask)
            lumi_mask = lumi_info(events.run, events.luminosityBlock)

            # compute integrated luminosity (/pb)
            lumi_data = LumiData(self.config.lumidata)
            lumi_list = LumiList(
                events[lumi_mask].run, events[lumi_mask].luminosityBlock
            )
            lumi = lumi_data.get_lumi(lumi_list)
            # save luminosity to metadata
            output["metadata"].update({"lumi": lumi})

        # get trigger mask and DeltaR matched trigger objects mask
        trig_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
        trig_match_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
        for hlt_path in self.config.hlt_paths[self.lepton_flavor]:
            if hlt_path in events.HLT.fields:
                trig_mask = trig_mask | events.HLT[hlt_path]
                trig_obj_mask = trigger_match(
                    leptons=(
                        events.Muon if self.lepton_flavor == "muon" else events.Electron
                    ),
                    trigobjs=events.TrigObj,
                    hlt_path=hlt_path,
                )
                trig_match_mask = trig_match_mask | trig_obj_mask

        # define region selection cuts
        selection = PackedSelection()
        selections = {
            "atleast_one_goodvertex": events.PV.npvsGood > 0,
            "lumimask": lumi_mask == 1,
            "trigger": trig_mask,
            "trigger_matching": dak.sum(trig_match_mask, axis=-1) > 0,
            "two_leptons": ak.num(leptons) == 2,
            "one_z": ak.num(dilepton.z.p4) == 1,
        }
        # get region selection cut mask
        selection.add_multiple(selections)
        region_selection = selection.all(*(selections.keys()))

        # save cutflow
        output["metadata"].update({"cutflow": {"initial": sumw}})
        current_selection = []
        for cut_name in selections.keys():
            current_selection.append(cut_name)
            output["metadata"]["cutflow"][cut_name] = ak.sum(
                weights_container.weight()[selection.all(*current_selection)]
            )

        # save raw and weighted number of events after selection
        final_nevents = dak.sum(region_selection)
        weighted_final_nevents = ak.sum(weights_container.weight()[region_selection])
        output["metadata"].update(
            {
                "weighted_final_nevents": weighted_final_nevents,
                "raw_final_nevents": final_nevents,
            }
        )
        # --------------------------------------------------------------
        # Histogram filling
        # --------------------------------------------------------------
        if final_nevents > 0:
            histograms = deepcopy(self.histograms)
            # define feature map
            feature_map = {
                "z_mass": dilepton.z.p4.mass[region_selection],
                "leading_lepton_pt": dilepton.z.leading_lepton.pt[region_selection],
                "subleading_lepton_pt": dilepton.z.subleading_lepton.pt[
                    region_selection
                ],
                "lepton_pt": leptons.pt[region_selection],
                "lepton_eta": leptons.eta[region_selection],
                "lepton_phi": leptons.phi[region_selection],
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

                    fill_histogram(
                        histograms=histograms,
                        histogram_config=self.histogram_config,
                        feature_map=feature_map,
                        weights=region_weight,
                        variation=variation,
                        flow=True,
                    )
            else:
                region_weight = weights_container.weight()[region_selection]
                fill_histogram(
                    histograms=histograms,
                    histogram_config=self.histogram_config,
                    feature_map=feature_map,
                    weights=region_weight,
                    variation="nominal",
                    flow=True,
                )
        # add histograms to output dictionary
        output["histograms"] = histograms
        return output

    def postprocess(self, accumulator):
        pass
