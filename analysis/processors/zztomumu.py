import numpy as np
import awkward as ak
from copy import deepcopy
from coffea import processor
from coffea.nanoevents import PFNanoAODSchema
from coffea.lumi_tools import LumiData, LumiList
from coffea.analysis_tools import Weights, PackedSelection
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.configs import ProcessorConfigBuilder
from analysis.corrections.muon import MuonWeights
from analysis.corrections.electron import ElectronSS
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.histograms import HistBuilder, fill_histogram
from analysis.selections import (
    ObjectSelector,
    get_lumi_mask,
    get_trigger_mask,
    get_trigger_match_mask,
)


PFNanoAODSchema.warn_missing_crossrefs = False


class ZZToMuMuProcessor(processor.ProcessorABC):
    def __init__(self, year: str):
        self.year = year

        config_builder = ProcessorConfigBuilder(processor="zztomumu", year=year)
        self.processor_config = config_builder.build_processor_config()
        self.histogram_config = self.processor_config.histogram_config
        self.histograms = HistBuilder(self.processor_config).build_histogram()

    def process(self, events):
        dataset = events.metadata["dataset"]

        # get golden json, triggers, selections and histograms
        year = self.year
        goldenjson = self.processor_config.goldenjson
        hlt_paths = self.processor_config.hlt_paths
        object_selections = self.processor_config.object_selection
        event_selection = self.processor_config.event_selection
        histograms = deepcopy(self.histograms)

        # check if dataset is MC or Data
        is_mc = hasattr(events, "genWeight")
        if not is_mc:
            events["Jet", "hadronFlavour"] = ak.zeros_like(events.Jet.pt)

        # initialize output dictionary
        output = {}

        # initialize metadata info
        nevents = len(events)
        output["metadata"] = {}
        output["metadata"].update({"raw_initial_nevents": nevents})

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
            year=year,
            dataset=dataset,
            apply_jec=apply_jec,
            apply_jer=apply_jer,
            apply_junc=apply_junc,
        )
        # electron scale and smearing corrections
        electron_ss = ElectronSS(
            events=events,
            year=year,
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
            # add muon id, iso and trigger weights
            muon_weights = MuonWeights(
                events=events,
                year=self.year,
                variation="nominal",
                weights=weights_container,
                id_wp=object_selections["muons"]["cuts"]["muon_id"],
                iso_wp=object_selections["muons"]["cuts"]["muon_iso"],
            )
            muon_weights.add_id_weights()
            muon_weights.add_iso_weights()
            muon_weights.add_trigger_weights(hlt_paths=hlt_paths)
        else:
            weights_container.add("genweight", ak.ones_like(events.PV.npvsGood))

        # save nevents (sum of weights) before selections
        sumw = ak.sum(weights_container.weight())
        output["metadata"].update({"sumw": sumw})

        # save integrated luminosity (/pb) to metadata
        if not is_mc:
            lumi_mask = eval(event_selection["selections"]["lumimask"])
            lumi_data = LumiData(self.processor_config.lumidata)
            lumi_list = LumiList(
                events[lumi_mask].run, events[lumi_mask].luminosityBlock
            )
            lumi = lumi_data.get_lumi(lumi_list)
            # save luminosity to metadata
            output["metadata"].update({"lumi": lumi})
        # --------------------------------------------------------------
        # Object selection
        # --------------------------------------------------------------
        object_selector = ObjectSelector(object_selections, year)
        objects = object_selector.select_objects(events)
        
        print(f"{ak.sum(ak.num(objects['muons'])==4)}")

        # --------------------------------------------------------------
        # Event selection
        # --------------------------------------------------------------
        # itinialize selection manager
        selection_manager = PackedSelection()
        # add all selections to selector manager
        for selection, mask in event_selection["selections"].items():
            selection_manager.add(selection, eval(mask))

        # run over each category
        categories = event_selection["categories"]
        for category, category_cuts in categories.items():
            # get selection mask by category
            category_mask = selection_manager.all(*category_cuts)
            # save cutflow
            output["metadata"][category] = {"cutflow": {"initial": sumw}}
            selections = []
            for cut_name in category_cuts:
                selections.append(cut_name)
                current_selection = selection_manager.all(*selections)
                output["metadata"][category]["cutflow"][cut_name] = ak.sum(
                    weights_container.weight()[current_selection]
                )
            # save number of events after selection to metadata
            nevents_after = ak.sum(category_mask)
            weighted_final_nevents = ak.sum(weights_container.weight()[category_mask])
            output["metadata"][category].update(
                {
                    "weighted_final_nevents": weighted_final_nevents,
                    "raw_final_nevents": nevents_after,
                }
            )
            # --------------------------------------------------------------
            # Histogram filling
            # --------------------------------------------------------------
            if nevents_after > 0:
                # get analysis variables
                variables_map = {}
                for variable, axis in self.histogram_config.axes.items():
                    variables_map[variable] = eval(axis.expression)[category_mask]
                # fill histograms
                if is_mc:
                    # get event weight systematic variations for MC samples
                    variations = ["nominal"] + list(weights_container.variations)
                    for variation in variations:
                        if variation == "nominal":
                            region_weight = weights_container.weight()[category_mask]
                        else:
                            region_weight = weights_container.weight(
                                modifier=variation
                            )[category_mask]
                        fill_histogram(
                            histograms=histograms,
                            histogram_config=self.histogram_config,
                            variables_map=variables_map,
                            weights=region_weight,
                            variation=variation,
                            category=category,
                            flow=True,
                        )
                else:
                    region_weight = weights_container.weight()[category_mask]
                    fill_histogram(
                        histograms=histograms,
                        histogram_config=self.histogram_config,
                        variables_map=variables_map,
                        weights=region_weight,
                        variation="nominal",
                        category=category,
                        flow=True,
                    )
        # add histograms to output dictionary
        output["histograms"] = histograms
        return output

    def postprocess(self, accumulator):
        pass
