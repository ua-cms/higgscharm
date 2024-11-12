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


class ZToMuMuProcessor(processor.ProcessorABC):
    def __init__(self, year: str):
        self.year = year

        config_builder = ProcessorConfigBuilder(processor="ztomumu", year=year)
        self.processor_config = config_builder.build_processor_config()
        self.histogram_config = self.processor_config.histogram_config
        self.histograms = HistBuilder(self.histogram_config).build_histogram()

    def process(self, events):
        dataset = events.metadata["dataset"]

        # get golden json, HLT paths and selections
        year = self.year
        goldenjson = self.processor_config.goldenjson
        hlt_paths = self.processor_config.hlt_paths
        object_selections = self.processor_config.object_selection
        event_selections = self.processor_config.event_selection

        # check if dataset is MC or Data
        is_mc = hasattr(events, "genWeight")

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
                id_wp=object_selections["leptons"]["cuts"]["muon_id"],
                iso_wp=object_selections["leptons"]["cuts"]["muon_iso"],
            )
            muon_weights.add_id_weights()
            muon_weights.add_iso_weights()
            muon_weights.add_trigger_weights(hlt_paths=hlt_paths)
        else:
            weights_container.add("genweight", ak.ones_like(events.PV.npvsGood))

        # save nevents (sum of weights) before selections
        sumw = ak.sum(weights_container.weight())
        output["metadata"].update({"sumw": sumw})

        # --------------------------------------------------------------
        # Object selection
        # --------------------------------------------------------------
        object_selector = ObjectSelector(object_selections, year)
        objects = object_selector.select_objects(events)

        # --------------------------------------------------------------
        # Event selection
        # --------------------------------------------------------------
        event_selection = PackedSelection()
        for selection, str_mask in event_selections.items():
            event_selection.add(selection, eval(str_mask))
        region_selection = event_selection.all(*event_selections.keys())

        # save cutflow
        output["metadata"].update({"cutflow": {"initial": sumw}})
        current_selection = []
        for cut_name in event_selections.keys():
            current_selection.append(cut_name)
            output["metadata"]["cutflow"][cut_name] = ak.sum(
                weights_container.weight()[event_selection.all(*current_selection)]
            )
        # save raw and weighted number of events after selection to metadata
        final_nevents = np.sum(region_selection)
        weighted_final_nevents = ak.sum(weights_container.weight()[region_selection])
        output["metadata"].update(
            {
                "weighted_final_nevents": weighted_final_nevents,
                "raw_final_nevents": final_nevents,
            }
        )
        # save integrated luminosity (/pb) to metadata
        if not is_mc:
            lumi_mask = eval(event_selections["lumimask"])
            lumi_data = LumiData(self.processor_config.lumidata)
            lumi_list = LumiList(
                events[lumi_mask].run, events[lumi_mask].luminosityBlock
            )
            lumi = lumi_data.get_lumi(lumi_list)
            # save luminosity to metadata
            output["metadata"].update({"lumi": lumi})

        # --------------------------------------------------------------
        # Histogram filling
        # --------------------------------------------------------------
        if final_nevents > 0:
            histograms = deepcopy(self.histograms)
            # get analysis features
            feature_map = {}
            for feature, axis_info in self.histogram_config.axes.items():
                feature_map[feature] = eval(axis_info["expression"])[region_selection]
            # fill histograms
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
