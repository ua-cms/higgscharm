from analysis.selections.utils import (
    delta_r_higher,
    delta_r_lower,
    select_dileptons,
    transverse_mass,
    get_closest_lepton,
    assign_lepton_fsr_idx,
    fourlepcand,
    make_cand,
    select_best_zllcandidate,
    get_flavor
)
from analysis.selections.object_selections import ObjectSelector
import analysis.selections.event_selections as event_selections

get_lumi_mask = event_selections.get_lumi_mask
get_trigger_mask = event_selections.get_trigger_mask
get_metfilters_mask = event_selections.get_metfilters_mask
get_zzto4l_trigger_mask = event_selections.get_zzto4l_trigger_mask