import numpy as np
import awkward as ak
from copy import deepcopy
from coffea import processor
from coffea.nanoevents import PFNanoAODSchema
from coffea.lumi_tools import LumiData, LumiList
from coffea.analysis_tools import Weights, PackedSelection
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.utils import dump_lumi
from analysis.configs import ProcessorConfigBuilder
from analysis.histograms import HistBuilder, fill_histogram
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.nnlops import add_nnlops_weight
from analysis.corrections.lhepdf import add_lhepdf_weight
from analysis.corrections.partonshower import add_partonshower_weight
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.electron import ElectronWeights, ElectronSS
from analysis.selections import (
    ObjectSelector,
    get_lumi_mask,
    get_zzto4l_trigger_mask,
)


PFNanoAODSchema.warn_missing_crossrefs = False


class ZZTo4LProcessor(processor.ProcessorABC):
    def __init__(self, year: str):
        self.year = year

        config_builder = ProcessorConfigBuilder(processor="zzto4l", year="2022" if year.startswith("2022") else "2023")
        self.processor_config = config_builder.build_processor_config()
        self.histogram_config = self.processor_config.histogram_config
        self.histograms = HistBuilder(self.processor_config).build_histogram()
        

    def process(self, events):
        year = self.year
        dataset = events.metadata["dataset"]
        
        object_selections = self.processor_config.object_selection
        event_selection = self.processor_config.event_selection
        hlt_paths = event_selection["hlt_paths"]
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
            # add NNLOPS weights for ggH
            if dataset.startswith("GluGluH"):
                add_nnlops_weight(
                    events=events,
                    weights_container=weights_container,
                )
            # add parton shower variations
            add_partonshower_weight(
                events=events,
                weights_container=weights_container,
            )
            # add LHEPDF variations
            add_lhepdf_weight(
                events=events,
                weights_container=weights_container,
            )
            # add electron reco weights
            electron_weights = ElectronWeights(
                events=events,
                year=self.year,
                weights=weights_container,
                variation="nominal",
            )
            electron_weights.add_reco_weights("RecoBelow20")
            electron_weights.add_reco_weights("Reco20to75")
            electron_weights.add_reco_weights("RecoAbove75")
        else:
            weights_container.add("genweight", ak.ones_like(events.PV.npvsGood))

        # save nevents (sum of weights) before selections
        sumw = ak.sum(weights_container.weight())
        output["metadata"].update({"sumw": sumw})

        # --------------------------------------------------------------
        # Luminosity
        # --------------------------------------------------------------
        if not is_mc:
            # get luminosity mask for lumi calibration
            lumi_mask = eval(event_selection["selections"]["lumimask"])
            # save integrated luminosity (/pb) to metadata
            dump_lumi(events[lumi_mask], output)
            
        # --------------------------------------------------------------
        # Object selection
        # --------------------------------------------------------------
        object_selector = ObjectSelector(object_selections, year)
        objects = object_selector.select_objects(events)

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
