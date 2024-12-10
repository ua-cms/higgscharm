import yaml
import numpy as np
import awkward as ak
import importlib.resources
from coffea.lumi_tools import LumiMask
from analysis.selections.trigger import trigger_mask, trigger_match, trigger_match_mask


def get_lumi_mask(events, goldenjson):
    if hasattr(events, "genWeight"):
        lumi_mask = np.ones(len(events), dtype="bool")
    else:
        lumi_info = LumiMask(goldenjson)
        lumi_mask = lumi_info(events.run, events.luminosityBlock)
    return lumi_mask == 1


def get_trigger_mask(events, hlt_paths, dataset_key):
    return trigger_mask(events, hlt_paths, dataset_key)


def get_trigger_match_mask(events, leptons, hlt_paths):
    mask = trigger_match_mask(events, leptons, hlt_paths)
    return ak.sum(mask, axis=-1) > 0
