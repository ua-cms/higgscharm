import awkward as ak
import dask_awkward as dak
from coffea.lumi_tools import LumiMask
from analysis.selections.utils import trigger_match


def get_lumi_mask(events, goldenjson):
    if hasattr(events, "genWeight"):
        lumi_mask = ak.ones_like(events.PV.npvsGood)
    else:
        lumi_info = LumiMask(goldenjson)
        lumi_mask = lumi_info(events.run, events.luminosityBlock)
    return lumi_mask == 1


def get_trigger_mask(events, hlt_paths):
    trig_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
    for hlt_path in hlt_paths:
        if hlt_path in events.HLT.fields:
            trig_mask = trig_mask | events.HLT[hlt_path]
    return trig_mask


def get_trigger_match_mask(events, leptons, hlt_paths):
    trig_match_mask = ak.zeros_like(events.PV.npvsGood, dtype="bool")
    for hlt_path in hlt_paths:
        if hlt_path in events.HLT.fields:
            trig_obj_mask = trigger_match(
                leptons=leptons,
                trigobjs=events.TrigObj,
                hlt_path=hlt_path,
            )
            trig_match_mask = trig_match_mask | trig_obj_mask
    return dak.sum(trig_match_mask, axis=-1) > 0
