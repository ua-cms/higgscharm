import awkward as ak


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
    n_of_trigger_matches = ak.sum(pass_delta_r, axis=2)
    trig_matched_locs = n_of_trigger_matches >= 1
    return trig_matched_locs


def delta_r_mask(first, second, threshold=0.4):
    # select objects from 'first' which are at least 'threshold' away from all objects in 'second'.
    mval = first.metric_table(second)
    return ak.all(mval > threshold, axis=-1)