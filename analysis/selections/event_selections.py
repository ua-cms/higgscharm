import yaml
import numpy as np
import awkward as ak
import importlib.resources
from coffea.lumi_tools import LumiMask
from analysis.selections.trigger import trigger_mask, trigger_match


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
    trig_match_mask = np.zeros(len(events), dtype="bool")
    for hlt_path in hlt_paths:
        if hlt_path in events.HLT.fields:
            trig_obj_mask = trigger_match(
                leptons=leptons,
                trigobjs=events.TrigObj,
                hlt_path=hlt_path,
            )
            trig_match_mask = trig_match_mask | trig_obj_mask
    return ak.sum(trig_match_mask, axis=-1) > 0
