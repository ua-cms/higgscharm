import warnings
import awkward as ak


def unflat_sf(sf: ak.Array, in_limit_mask: ak.Array, n: ak.Array):
    """
    get scale factors for in-limit objects (otherwise assign 1).
    Unflat array to original shape and multiply scale factors event-wise

    Parameters:
    -----------
        sf:
            Array with 1D scale factors
        in_limit_mask:
            Array mask for events with objects within correction limits
        n:
            Array with number of objects per event
    """
    sf = ak.where(in_limit_mask, sf, ak.ones_like(sf))
    return ak.fill_none(ak.prod(ak.unflatten(sf, n), axis=1), value=1)


# Best-candidate fields of the H->ZZ->4l signal region and the Z+ll control regions (selected_<object name>).
CANDIDATE_FIELDS = (
    "selected_best_zzcandidate",
    "selected_best_1fcr_zllcandidate_os",
    "selected_best_2fcr_zllcandidate_os",
    "selected_best_sscr_zllcandidate_ss",
    "selected_best_1fcr_zllcandidate",
    "selected_best_2fcr_zllcandidate",
    "selected_best_sscr_zllcandidate",
)


def candidate_leptons_only(events, leptons):
    """Keep only the leptons that are legs of the event's best ZZ (or Z+ll) candidate.

    The HZZ lepton SFs are measured for the leptons of the selected candidate (as CJLST LeptonSFHelper);
    events.selected_muons/electrons hold every reconstructed lepton (the object selection only adds
    is_loose/is_tight flags), so applying the SF to all of them would also weight extra leptons, e.g.
    soft muons from heavy-flavour decays inside jets. Legs are matched by pdgId, eta and phi (unchanged
    by FSR recovery). Without a best-candidate field the collection is returned unchanged (with a warning).
    """
    field = next((f for f in CANDIDATE_FIELDS if f in events.fields), None)
    if field is None:
        warnings.warn(
            "no best-candidate field: HZZ lepton SFs applied to every selected lepton",
            RuntimeWarning,
            stacklevel=2,
        )
        return leptons
    cand = ak.firsts(events[field])
    is_leg = ak.zeros_like(leptons.pt, dtype=bool)
    for leg in (cand.z1.l1, cand.z1.l2, cand.z2.l1, cand.z2.l2):
        match = (
            (leptons.pdgId == leg.pdgId)
            & (abs(leptons.eta - leg.eta) < 1e-5)
            & (abs(leptons.phi - leg.phi) < 1e-5)
        )
        is_leg = is_leg | ak.fill_none(match, False)
    return leptons[is_leg]

# backward compatibility: the HcZZ modules (ctag2d, hczz_legacy/*) import the table from here
from analysis.corrections.correctionlib_files import correction_files  # noqa: E402,F401
