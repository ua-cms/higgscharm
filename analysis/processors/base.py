import warnings
import numpy as np
import awkward as ak
from copy import deepcopy
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.utils import dump_lumi, dump_parquet
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.histograms import HistBuilder, fill_histograms

from analysis.corrections.correction_manager import (
    object_corrector_manager,
    weight_manager,
)
from analysis.selections import (
    ObjectSelector,
    get_lumi_mask,
    get_trigger_mask,
    get_zzto4l_trigger_mask,
    get_metfilters_mask,
    get_trigger_match_mask,
    get_stitching_mask,
)

NanoAODSchema.warn_missing_crossrefs = False


def update(events, collections):
    """Return a shallow copy of events array with some collections swapped out"""
    out = events
    for name, value in collections.items():
        out = ak.with_field(out, value, name)
    return out


class BaseProcessor(processor.ProcessorABC):
    def __init__(
        self,
        workflow: str,
        year: str,
        output_format: str,
        output_location: str,
        partition_key: str,
        mode="virtual",
    ):
        assert mode in ["eager", "virtual", "dask"]
        self._mode = mode
        self.year = year
        self.workflow = workflow
        self.output_format = output_format
        self.output_location = output_location
        self.partition_key = partition_key
        self.run = "2" if year.startswith("201") else "3"

        config_builder = WorkflowConfigBuilder(workflow)
        self.workflow_config = config_builder.build_workflow_config()
        self.histogram_config = self.workflow_config.histogram_config
        self.histograms = HistBuilder(self.workflow_config).build_histogram()

    def add_cutflow(self, events, output, selection_manager):
        for category, category_cuts in self.workflow_config.event_selection[
            "categories"
        ].items():
            output["metadata"].update({category: {"cutflow": {"initial": self.sumw}}})
            selections = []
            for cut_name in category_cuts:
                selections.append(cut_name)
                current_selection = selection_manager.all(*selections)
                if ak.sum(current_selection) != 0:
                    sumw_cutflow = (
                        ak.sum(self.genweight[current_selection])
                        if self.is_mc
                        else len(events[current_selection])
                    )
                    output["metadata"][category]["cutflow"][cut_name] = sumw_cutflow

                else:
                    output["metadata"][category]["cutflow"][cut_name] = 0

    def process(self, events):
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        np.seterr(divide="ignore", invalid="ignore")

        # identify data and simulation
        self.is_mc = hasattr(events, "genWeight")

        # compute sum of gen weights for normalization
        self.sumw = ak.sum(events.genWeight) if self.is_mc else len(events)
        if self.is_mc:
            self.genweight = events.genWeight

        # apply object corrections, jet veto and prepare systematic variations
        vetoed_events, shifts = object_corrector_manager(
            events=events,
            year=self.year,
            run=self.run,
            dataset=events.metadata["dataset"],
            workflow_config=self.workflow_config,
        )
        return processor.accumulate(
            self.process_shift(update(vetoed_events, collections), shift)
            for collections, shift in shifts
        )

    def process_shift(self, events, shift):
        # define variables required by workflow expressions evaluated with eval()
        year = self.year
        dataset = events.metadata["dataset"]
        hlt_paths = self.workflow_config.event_selection["hlt_paths"]

        # initialize output metadata
        output = {}
        output["metadata"] = {}
        if shift is None:
            # store the initial sum of weights before object selection
            output["metadata"].update({"sumw": self.sumw})

        if not self.is_mc:
            events["Jet", "hadronFlavour"] = ak.zeros_like(events.Jet.pt)

        # object selection
        object_selection = self.workflow_config.object_selection
        object_selector = ObjectSelector(object_selection, year)
        objects = object_selector.select_objects(events)

        # event selection
        selection_manager = PackedSelection()
        event_selection = self.workflow_config.event_selection
        for selection, mask in event_selection["selections"].items():
            selection_manager.add(selection, eval(mask))

        if not self.is_mc:
            # save (run, luminosityBlock) pairs to metadata
            lumi_mask = eval(event_selection["selections"]["lumimask"])
            dump_lumi(events[lumi_mask], output)

        # save cutflow to metadata
        self.add_cutflow(events, output, selection_manager)

        # Histogram filling / array dumping
        histograms = deepcopy(self.histograms)
        categories = event_selection["categories"]
        for category, category_cuts in categories.items():
            # get selection mask by category
            category_mask = selection_manager.all(*category_cuts)
            nevents_after = ak.sum(category_mask)
            if nevents_after > 0:
                # get pruned events
                pruned_ev = events[category_mask]
                # add each selected object to 'pruned_ev' as a new field
                for obj in objects:
                    pruned_ev[f"selected_{obj}"] = objects[obj][category_mask]
                # get weights container
                weights_container = weight_manager(
                    pruned_ev=pruned_ev,
                    year=year,
                    dataset=dataset,
                    workflow_config=self.workflow_config,
                    category=category,
                    shift=shift,
                )
                # evaluate variables required by the histogram layout
                variables_map = {}
                layout_axes = self.histogram_config.layout.values()
                for variable, axis in self.histogram_config.axes.items():
                    if variable in ak.flatten(layout_axes):
                        variables_map[variable] = eval(axis.expression)[category_mask]

                if self.output_format == "coffea":
                    fill_histograms(
                        histogram_config=self.histogram_config,
                        weights_container=weights_container,
                        variables_map=variables_map,
                        histograms=histograms,
                        category=category,
                        is_mc=self.is_mc,
                        shift=shift,
                        flow=True,
                    )
                elif self.output_format == "parquet":
                    dump_parquet(
                        events=events,
                        weights_container=weights_container,
                        variables_map=variables_map,
                        workflow=self.workflow,
                        year=year,
                        category=category,
                        output_location=self.output_location,
                        partition_key=self.partition_key,
                        shift=shift,
                    )

        # add histograms to output dictionary
        if self.output_format == "coffea":
            output["histograms"] = histograms
        return output

    def postprocess(self, accumulator):
        pass
