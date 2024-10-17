import json
import numba
import vector
import numpy as np
import awkward as ak
import dask_awkward as dak
import importlib.resources
from coffea.lumi_tools import LumiMask
from coffea.analysis_tools import PackedSelection
from analysis.working_points import working_points
from analysis.utils.trigger_matching import trigger_match
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector


def object_selector(events, object_selection_config):
    # initialize dictionary to store analysis objects
    objects = {}
    for obj_name, obj_config in object_selection_config.items():
        # get object through field expression evaluation
        obj = eval(obj_config["expression"])
        objects[obj_name] = obj
        if obj_config["cuts"]:
            # initialize selection mask
            selection_mask = ak.ones_like(obj.pt, dtype=bool)
            # iterate over all cuts
            for selection, str_mask in obj_config["cuts"].items():
                # if 'str_mask' contains 'events' or 'objects' evaluate the string expression
                # otherwise read the mask from the working points function
                if "events" in str_mask or "objects" in str_mask:
                    mask = eval(str_mask)
                else:
                    mask = getattr(working_points, selection)(events, str_mask)
                selection_mask = np.logical_and(selection_mask, mask)
            objects[obj_name] = objects[obj_name][selection_mask]
    return objects


# -------------------------
# dilepton selection
# -------------------------
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

def select_dileptons(leptons):
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
            ),
            "pt": (dilepton[0] + dilepton[1]).pt
        }
    )
    return dilepton


def get_lumi_mask(events, goldenjson):
    is_mc = hasattr(events, "genWeight")
    if is_mc:
        lumi_mask = ak.ones_like(events.PV.npvsGood)
    else:
        lumi_info = LumiMask(self.config.lumimask)
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