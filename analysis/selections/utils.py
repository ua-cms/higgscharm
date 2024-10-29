import numba
import awkward as ak
import dask_awkward as dak


def trigger_match(leptons, trigobjs, hlt_path):
    """
    Returns DeltaR matched trigger objects

    leptons:
        Muons array
    trigobjs:
        trigobjs array
    hlt_path:
        trigger to match (IsoMu24, Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8, Ele30_WPTight_Gsf)

    how to:
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/EgammaNanoAOD#Trigger_bits_how_to

    NanoAOD docs:
    https://cms-nanoaod-integration.web.cern.ch/autoDoc/NanoAODv11/2022postEE/doc_WZ_TuneCP5_13p6TeV_pythia8_Run3Summer22EENanoAODv11-126X_mcRun3_2022_realistic_postEE_v1-v1.html#TrigObj
    """
    match_configs = {
        # filterbit: 3 => 1mu
        # id: 13 => mu
        "IsoMu24": {
            "pt": trigobjs.pt > 23,
            "id": abs(trigobjs.id) == 13,
            "filterbit": trigobjs.filterBits & (0x1 << 3) > 0,
        },
        # filterbit: 0 => TrkIsoVVL
        # id: 13 => mu
        "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8": {
            "pt": trigobjs.pt > 7,
            "id": abs(trigobjs.id) == 13,
            "filterbit": trigobjs.filterBits & (0x1 << 0) > 0,
        },
        # filterbit: 1 => 1e (WPTight)
        # id: 11 => ele
        "Ele30_WPTight_Gsf": {
            "pt": trigobjs.pt > 28,
            "id": abs(trigobjs.id) == 11,
            "filterbit": trigobjs.filterBits & (0x1 << 1) > 0,
        },
    }
    pass_pt = match_configs[hlt_path]["pt"]
    pass_id = match_configs[hlt_path]["id"]
    pass_filterbit = match_configs[hlt_path]["filterbit"]
    trigger_cands = trigobjs[pass_pt & pass_id & pass_filterbit]
    delta_r = leptons.metric_table(trigger_cands)
    pass_delta_r = delta_r < 0.1
    n_of_trigger_matches = dak.sum(pass_delta_r, axis=2)
    trig_matched_locs = n_of_trigger_matches >= 1
    return trig_matched_locs


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