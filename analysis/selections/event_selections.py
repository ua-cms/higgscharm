import yaml
import json
import numpy as np
import awkward as ak
import importlib.resources
from coffea.lumi_tools import LumiMask
from analysis.selections.trigger import trigger_mask, trigger_match_mask, zzto4l_trigger


def get_lumi_mask(events, goldenjson):
    if hasattr(events, "genWeight"):
        lumi_mask = np.ones(len(events), dtype="bool")
    else:
        lumi_info = LumiMask(goldenjson)
        lumi_mask = lumi_info(events.run, events.luminosityBlock)
    return lumi_mask == 1

def get_zzto4l_trigger_mask(events, hlt_paths, dataset_key):
    return zzto4l_trigger(events, hlt_paths, dataset_key)

def get_trigger_mask(events, hlt_paths, dataset_key):
    return trigger_mask(events, hlt_paths, dataset_key)


def get_trigger_match_mask(events, leptons, hlt_paths):
    mask = trigger_match_mask(events, leptons, hlt_paths)
    return ak.sum(mask, axis=-1) > 0


def get_metfilters_mask(events, year):
    with importlib.resources.path("analysis.data", "metfilters.json") as path:
        with open(path, "r") as handle:
            metfilters = json.load(handle)[year]
    metfilters_mask = np.ones(len(events), dtype="bool")
    metfilterkey = "mc" if hasattr(events, "genWeight") else "data"
    for mf in metfilters[metfilterkey]:
        if mf in events.Flag.fields:
            metfilters_mask = metfilters_mask & events.Flag[mf]
    return metfilters_mask