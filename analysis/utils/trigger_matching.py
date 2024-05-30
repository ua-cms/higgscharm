import dask_awkward as dak

def trigger_match(leptons, trigobjs, hlt_path):
    """
    Returns DeltaR matched trigger objects 
    
    leptons:
        Muons array
    trigobjs:
        trigobjs array
    trigger_path:
        trigger to match (IsoMu24, Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8)
        
    https://twiki.cern.ch/twiki/bin/viewauth/CMS/EgammaNanoAOD#Trigger_bits_how_to
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