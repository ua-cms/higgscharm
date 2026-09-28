#!/usr/bin/env python3
import numpy as np
import awkward as ak
import pyarrow.dataset as ds
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc
import xgboost as xgb
import math
import sys
import glob
from pathlib import Path
import yaml
from coffea.util import load as coffea_load

# ONNX
from onnxmltools.convert import convert_xgboost
from skl2onnx.common.data_types import FloatTensorType
import onnxruntime as rt

# ----------------------------
# Analysis Configuration
# ----------------------------
CVL_WP = 0.16
CVB_WP = 0.306
DR_MATCH = 0.8

USE_FLAVOUR_IN_RANKER      = True
USE_WEIGHTS_IN_TRAINING    = True
ASSOC_PICK_ONLY_WITHIN_WP  = False

DROP_NEG_W_IN_TRAIN = False
DROP_NEG_W_IN_TEST  = False

# pT bins for purity-by-truth-pT
PTBIN_LOW_LO,  PTBIN_LOW_HI = 20.0, 30.0
PTBIN_HIGH_MIN               = 30.0

# Thresholds (applied to globally-normalised scores in [0,1])
thr_values = [0.0, 0.3, 0.6]

# Paths
BASE     = "/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/"
sig_file = BASE + "SomeSMSignal/base"
bkg_file = BASE + "GluGluHtoZZto4L/base"

YEAR           = "2022postEE"
SIG_SAMPLE     = "SomeSMSignal"
BKG_SAMPLE     = "GluGluHtoZZto4L"
DATASET_CONFIG = Path("analysis/filesets/2022postEE_nanov12.yaml")
LUMI_CONFIG    = Path("analysis/postprocess/luminosity.yaml")

ONNX_MODEL_PATH = "xgb_ranker.onnx"
ONNX_RTOL       = 1e-4
ONNX_ATOL       = 1e-4

# GenPart flags
FLAG_IS_HARD_PROCESS = (1 << 7)
FLAG_IS_LAST_COPY    = (1 << 13)
CHARM_PDGID          = 4

# Validation fraction for early stopping (fraction of training set)
VAL_FRACTION = 0.15

# ----------------------------
# Utilities
# ----------------------------
def safe_div(a, b):
    return a / b if b > 0 else float("nan")

def delta_phi(phi1, phi2):
    return (phi1 - phi2 + np.pi) % (2 * np.pi) - np.pi

def signif_s_over_sqrt_sb(S, B):
    tot = S + B
    return S / math.sqrt(tot) if tot > 0 else float("nan")

def significance_with_neg_weights(S_pos, S_neg, B):
    """
    Proper significance when negative-weight events are present.

    The naive Z = S/sqrt(S+B) assumes Poisson statistics where all weights
    are positive. With negative weights the variance of S is no longer |S|
    but instead Var(S) = S_pos^2 + S_neg^2 (sum in quadrature of absolute
    yields). This gives a more conservative, correct estimate of Z.

    Parameters
    ----------
    S_pos : sum of positive event weights after selection
    S_neg : sum of negative event weights after selection (already negative)
    B     : total background yield after selection
    """
    S_net   = S_pos + S_neg
    var_sig = S_pos**2 + S_neg**2          # variance from neg-weight events
    denom   = math.sqrt(var_sig + abs(B)) if (var_sig + abs(B)) > 0 else float("nan")
    return S_net / denom if denom > 0 else float("nan")

def effective_sample_size(weights):
    """
    N_eff = (Σ|w|)² / Σ(w²)

    Measures how many unweighted events your sample is equivalent to.
    With all-positive unit weights N_eff = N_events.
    Negative weights reduce N_eff below N_events, quantifying the
    statistical power lost due to the NLO interference terms.
    """
    w = np.asarray(weights, float)
    sum_abs_w = np.sum(np.abs(w))
    sum_w2    = np.sum(w**2)
    return (sum_abs_w**2 / sum_w2) if sum_w2 > 0 else 0.0

def split_posneg_sumw(w):
    w = np.asarray(w, float)
    Sp = float(np.sum(w[w > 0]))
    Sn = float(np.sum(w[w < 0]))
    return Sp, Sn, Sp + Sn

def purity_and_err(weights, is_correct, use_abs_w=True):
    w = np.asarray(weights, dtype=float)
    I = np.asarray(is_correct, dtype=bool).astype(float)
    if use_abs_w:
        w = np.abs(w)
    W = np.sum(w)
    if W <= 0:
        return float("nan"), float("nan"), 0.0
    p  = float(np.sum(w * I) / W)
    W2 = float(np.sum(w * w))
    if W2 <= 0:
        return p, float("nan"), 0.0
    Neff = float((W * W) / W2)
    var  = p * (1.0 - p) / Neff if Neff > 0 else float("nan")
    err  = math.sqrt(var) if (np.isfinite(var) and var >= 0) else float("nan")
    return p, err, Neff

def top1_correct_per_event(evt, scores, y):
    correct = []
    for ev in np.unique(evt):
        m = evt == ev
        i = int(np.argmax(scores[m]))
        correct.append(bool(y[m][i] == 1))
    return np.array(correct, dtype=bool)

def event_weight_map(evt_ids, w_evt_perjet):
    _, first_idx = np.unique(evt_ids, return_index=True)
    return dict(zip(evt_ids[first_idx].tolist(), w_evt_perjet[first_idx].tolist()))

def make_group_sizes(evt_sorted):
    _, counts = np.unique(evt_sorted, return_counts=True)
    return counts.astype(np.uint32)

def group_weights_from_sorted(evt_sorted, w_sorted, use_abs=True):
    """Use mean |weight| per event group (more robust than first-jet only)."""
    w = np.abs(np.asarray(w_sorted, float)) if use_abs else np.asarray(w_sorted, float)
    unique_evts = np.unique(evt_sorted)
    gw = np.array([w[evt_sorted == ev].mean() for ev in unique_evts], dtype=float)
    m  = np.mean(gw) if len(gw) else 1.0
    return (gw / m).astype(float) if m > 0 else gw

def sort_by_event(evt, *arrays):
    order = np.argsort(evt, kind="mergesort")
    return evt[order], [a[order] for a in arrays], order

def normalize_scores_global(sig_scores, bkg_scores):
    """
    Global min-max normalisation using the combined sig+bkg score range.

    Why global and not per-event?
      Per-event normalisation forces the best jet in every event to score 1.0,
      so a threshold of 0.3 keeps 100% of events (every event has a jet=1.0).
      Global normalisation preserves the absolute score differences so that
      thresholds actually cut events: an event whose best jet scores 0.2
      globally is genuinely lower-quality than one scoring 0.9.

    Why joint sig+bkg?
      Signal and background are evaluated on the same score axis. Using the
      same lo/hi ensures that thr=0.5 means the same thing for both samples.
    """
    all_scores = np.concatenate([sig_scores, bkg_scores])
    lo, hi     = all_scores.min(), all_scores.max()
    if hi > lo:
        norm_sig = (sig_scores - lo) / (hi - lo)
        norm_bkg = (bkg_scores - lo) / (hi - lo)
    else:
        norm_sig = np.zeros_like(sig_scores)
        norm_bkg = np.zeros_like(bkg_scores)
    return norm_sig, norm_bkg, lo, hi


def find_signal_last_copy_charms(pdg, mother_idx, status_flags):
    is_hard      = (status_flags & FLAG_IS_HARD_PROCESS).astype(bool)
    is_last_copy = (status_flags & FLAG_IS_LAST_COPY).astype(bool)

    n = len(pdg)
    daughters = [[] for _ in range(n)]
    for child, mom in enumerate(mother_idx):
        if 0 <= int(mom) < n:
            daughters[int(mom)].append(child)

    hard_charm_indices = np.where(
        (np.abs(pdg) == CHARM_PDGID) & is_hard
    )[0]

    if len(hard_charm_indices) == 0:
        return np.array([], dtype=int)

    signal_charms = []
    for hard_idx in hard_charm_indices:
        queue   = list(daughters[hard_idx])
        visited = set()
        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)
            if abs(int(pdg[current])) == CHARM_PDGID:
                if is_last_copy[current]:
                    signal_charms.append(current)
                else:
                    queue.extend(daughters[current])

    return np.array(signal_charms, dtype=int)


# ----------------------------
# I/O
# ----------------------------
def get_lumi_weight(year, sample):
    with open(LUMI_CONFIG) as f:
        lumi = yaml.safe_load(f)[year]

    with open(DATASET_CONFIG) as f:
        dataset_cfg = yaml.safe_load(f)

    entry = dataset_cfg[sample]
    xsec  = entry["xsec"]
    era   = entry.get("era", "mc")

    if era not in ("mc", "signal"):
        print(f"  [{sample}] era={era} → lumi_weight=1.0 (data)")
        return 1.0

    coffea_dir   = Path(BASE) / sample
    coffea_files = sorted(coffea_dir.glob("*.coffea"))
    if not coffea_files:
        raise FileNotFoundError(f"No .coffea files found in {coffea_dir}")

    total_sumw = 0.0
    for cf in coffea_files:
        acc = coffea_load(str(cf))
        total_sumw += float(acc["metadata"]["sumw"])

    lumi_weight = (lumi * xsec) / total_sumw
    print(f"  [{sample}] lumi={lumi:.1f} pb-1, xsec={xsec:.6g} pb, "
          f"sumw={total_sumw:.2f} → lumi_weight={lumi_weight:.6e}")
    return lumi_weight


def load_parquet(path, sample):
    table = ds.dataset(path, format="parquet").to_table()
    arr   = ak.from_arrow(table)

    lumi_wgt = get_lumi_weight(YEAR, sample)
    w = np.array(arr["weight_nominal"], dtype=float) * lumi_wgt

    def _flat(name, default=0.0):
        data = arr[name]
        if hasattr(data.type, "content"):
            return np.array(ak.fill_none(ak.firsts(data), default), dtype=float)
        return np.array(ak.fill_none(data, default), dtype=float)

    hpt   = _flat("zz_pt_inclusive")
    hphi  = _flat("zz_phi_inclusive")
    hmass = (_flat("zz_mass_inclusive")
             if "zz_mass_inclusive" in arr.fields
             else np.full(len(hpt), float("nan")))
    uids  = np.arange(len(hpt), dtype=np.int64)
    return arr, w, hpt, hphi, hmass, uids


# ----------------------------
# Build per-jet dataset
# ----------------------------
def build_jet_rows(arrays, w_evt_true, H_pt_evt, H_phi_evt, H_mass_evt, evt_uid,
                   need_truth, drop_unmatched):
    X_list, cvl_list, cvb_list, evt_list = [], [], [], []
    w_list, mass_list = [], []
    y_list, matchable_list, truthpt_list = [], [], []
    charm_pt_all, charm_eta_all, charm_w_all = [], [], []

    stats = {
        "total_events_with_jets": 0,
        "used_events":            0,
        "matchable_events":       0,
        "unmatchable_events":     0,
        "dropped_no_genpart":     0,
        "dropped_no_cand":        0,
        "dropped_no_jet_match":   0,
        "dropped_bestdr_fail":    0,
    }

    fields      = arrays.fields
    has_genpart = all(b in fields for b in [
        "genpart_pdgId", "genpart_eta", "genpart_phi",
        "genpart_statusFlags", "genpart_genPartIdxMother",
        "genpart_isHardProcess", "genpart_isLastCopy",
    ])
    has_genpt = "genpart_pt" in fields

    n_feat   = 5 if USE_FLAVOUR_IN_RANKER else 3
    n_events = len(H_pt_evt)

    for ievt in range(n_events):
        raw_pt = arrays["cjets_pt"][ievt]
        if raw_pt is None or (hasattr(raw_pt, "__len__") and len(raw_pt) == 0):
            continue

        j_pt  = np.atleast_1d(np.array(raw_pt,                       dtype=float))
        j_eta = np.atleast_1d(np.array(arrays["cjets_eta"][ievt],    dtype=float))
        j_phi = np.atleast_1d(np.array(arrays["cjets_phi"][ievt],    dtype=float))
        j_cvl = np.atleast_1d(np.nan_to_num(
                    np.array(arrays["jets_cvsl"][ievt], dtype=float), nan=-1.0))
        j_cvb = np.atleast_1d(np.nan_to_num(
                    np.array(arrays["jets_cvsb"][ievt], dtype=float), nan=-1.0))

        n_jets = len(j_pt)
        if n_jets == 0:
            continue

        if need_truth:
            stats["total_events_with_jets"] += 1

        w_event = float(w_evt_true[ievt])
        if not np.isfinite(w_event):
            continue

        uid  = int(evt_uid[ievt])
        Hpt  = float(H_pt_evt[ievt])
        Hphi = float(H_phi_evt[ievt])
        Hm   = float(H_mass_evt[ievt]) if H_mass_evt is not None else float("nan")

        matched_mask       = np.zeros(n_jets, dtype=bool)
        evt_is_matchable   = False
        evt_truth_charm_pt = float("nan")

        if need_truth:
            if not has_genpart:
                stats["dropped_no_genpart"] += 1
                if drop_unmatched:
                    continue
            else:
                raw_pdg = arrays["genpart_pdgId"][ievt]
                if raw_pdg is None or len(raw_pdg) == 0:
                    stats["dropped_no_genpart"] += 1
                    if drop_unmatched:
                        continue
                else:
                    pdg          = np.array(raw_pdg,                                  dtype=int)
                    g_eta        = np.array(arrays["genpart_eta"][ievt],              dtype=float)
                    g_phi        = np.array(arrays["genpart_phi"][ievt],              dtype=float)
                    mother_idx   = np.array(arrays["genpart_genPartIdxMother"][ievt], dtype=int)
                    status_flags = np.array(arrays["genpart_statusFlags"][ievt],      dtype=int)
                    g_pt         = (np.array(arrays["genpart_pt"][ievt],              dtype=float)
                                    if has_genpt else None)

                    cand = find_signal_last_copy_charms(pdg, mother_idx, status_flags)

                    if g_pt is not None:
                        for ci in cand:
                            charm_pt_all.append(float(g_pt[ci]))
                            charm_eta_all.append(float(g_eta[ci]))
                            charm_w_all.append(float(w_event))

                    if cand.size == 0:
                        stats["dropped_no_cand"] += 1
                        if drop_unmatched:
                            continue
                    else:
                        best_dr, best_j, best_ci = np.inf, None, None
                        for ci in cand:
                            deta   = j_eta - g_eta[ci]
                            dphi_v = delta_phi(j_phi, g_phi[ci])
                            drs    = np.sqrt(deta**2 + dphi_v**2)
                            j_min  = int(np.argmin(drs))
                            if drs[j_min] < best_dr:
                                best_dr, best_j, best_ci = drs[j_min], j_min, ci

                        if best_j is None:
                            stats["dropped_no_jet_match"] += 1
                            if drop_unmatched:
                                continue
                        elif best_dr >= DR_MATCH:
                            stats["dropped_bestdr_fail"] += 1
                            if drop_unmatched:
                                continue
                        else:
                            matched_mask[best_j] = True
                            evt_is_matchable      = True
                            if g_pt is not None:
                                evt_truth_charm_pt = float(g_pt[best_ci])

        for j in range(n_jets):
            r       = Hpt / j_pt[j] if j_pt[j] > 0 else 0.0
            dphi_hj = abs(delta_phi(j_phi[j], Hphi))

            feat = ([r, dphi_hj, Hpt, j_cvl[j], j_cvb[j]]
                    if USE_FLAVOUR_IN_RANKER else [r, dphi_hj, Hpt])

            X_list.append(feat)
            cvl_list.append(j_cvl[j])
            cvb_list.append(j_cvb[j])
            evt_list.append(uid)
            w_list.append(w_event)
            mass_list.append(Hm)

            if need_truth:
                y_list.append(1 if matched_mask[j] else 0)
                matchable_list.append(bool(evt_is_matchable))
                truthpt_list.append(float(evt_truth_charm_pt))

        if need_truth:
            stats["used_events"] += 1
            if evt_is_matchable:
                stats["matchable_events"] += 1
            else:
                stats["unmatchable_events"] += 1

    if len(X_list) == 0:
        X = np.empty((0, n_feat), dtype=float)
    else:
        X = np.array(X_list, dtype=float)
        if X.ndim == 1:
            X = X.reshape(1, n_feat)

    cvl = np.array(cvl_list,  dtype=float)
    cvb = np.array(cvb_list,  dtype=float)
    evt = np.array(evt_list,  dtype=np.int64)
    w   = np.array(w_list,    dtype=float)
    hm  = np.array(mass_list, dtype=float)

    y         = np.array(y_list,         dtype=int)  if need_truth else None
    matchable = np.array(matchable_list, dtype=bool) if need_truth else None
    truth_pt  = np.array(truthpt_list,   dtype=float) if need_truth else None

    if len(X) == 0:
        mask = np.zeros(0, dtype=bool)
    else:
        mask = (np.isfinite(X).all(axis=1)
                & np.isfinite(cvl) & np.isfinite(cvb) & np.isfinite(w))
    if need_truth:
        mask &= np.isfinite(y.astype(float))
        y         = y[mask]
        matchable = matchable[mask]
        truth_pt  = truth_pt[mask]
    X, cvl, cvb, evt, w, hm = X[mask], cvl[mask], cvb[mask], evt[mask], w[mask], hm[mask]

    mean_abs_w = float(np.mean(np.abs(w))) if len(w) else 0.0
    w_ml       = np.abs(w) / mean_abs_w if mean_abs_w > 0 else np.abs(w)

    out = {"X": X, "cvl": cvl, "cvb": cvb, "evt": evt,
           "w_evt": w, "w_ml": w_ml, "H_mass": hm}
    if need_truth:
        out.update({
            "y": y, "matchable": matchable, "truth_pt": truth_pt,
            "stats": stats,
            "charm_pt":  np.array(charm_pt_all,  dtype=float),
            "charm_eta": np.array(charm_eta_all, dtype=float),
            "charm_w":   np.array(charm_w_all,   dtype=float),
        })
        tag = "DROP" if drop_unmatched else "KEEP"
        print(f"[build_jet_rows:{tag}] total_events_with_jets={stats['total_events_with_jets']}")
        print(f"[build_jet_rows:{tag}] used={stats['used_events']}  "
              f"matchable={stats['matchable_events']}  "
              f"unmatchable={stats['unmatchable_events']}")
        print(f"[build_jet_rows:{tag}] dropped: no_genpart={stats['dropped_no_genpart']}  "
              f"no_cand={stats['dropped_no_cand']}  "
              f"no_jet_match={stats['dropped_no_jet_match']}  "
              f"bestdr_fail={stats['dropped_bestdr_fail']}")
    return out


# ----------------------------
# Reduce jets → event-level
# ----------------------------
def reduce_to_events_consistent(evt_ids, scores, cvl, cvb, w_evt, H_mass_perjet,
                                 y_truth=None, matchable_perjet=None, truth_pt_perjet=None):
    unique_evt = np.unique(evt_ids)
    ev_w, ev_mass, ev_wp_any = [], [], []
    ev_matchable, ev_truthpt  = [], []
    ev_score_best_all, ev_picked_pass_wp_all, ev_score_best_wp = [], [], []
    ev_pick_correct_all, ev_pick_correct_wp, ev_pick_correct_wp_cvl = [], [], []

    for ev in unique_evt:
        m = evt_ids == ev
        if not np.any(m):
            continue
        wval = float(w_evt[m][0])
        if not np.isfinite(wval):
            continue

        s_ev    = scores[m]
        cvl_ev  = cvl[m]
        cvb_ev  = cvb[m]
        wp_mask = (cvl_ev > CVL_WP) & (cvb_ev > CVB_WP)
        wp_any  = bool(np.any(wp_mask))

        ia        = int(np.argmax(s_ev))
        score_a   = float(s_ev[ia])
        pass_wp_a = bool(wp_mask[ia])

        if wp_any:
            idxs    = np.where(wp_mask)[0]
            iw      = int(idxs[np.argmax(s_ev[idxs])])
            score_w = float(s_ev[iw])
            icvl    = int(idxs[np.argmax(cvl_ev[idxs])])
        else:
            iw, score_w, icvl = -1, float("nan"), -1

        ev_wp_any.append(1 if wp_any else 0)
        ev_w.append(wval)
        ev_mass.append(float(H_mass_perjet[m][0]))
        ev_score_best_all.append(score_a)
        ev_picked_pass_wp_all.append(1 if pass_wp_a else 0)
        ev_score_best_wp.append(score_w)
        ev_matchable.append(bool(matchable_perjet[m][0]) if matchable_perjet is not None else False)
        ev_truthpt.append(float(truth_pt_perjet[m][0]) if truth_pt_perjet is not None else float("nan"))

        if y_truth is not None:
            y_ev = y_truth[m]
            ev_pick_correct_all.append(bool(y_ev[ia] == 1))
            ev_pick_correct_wp.append(bool(wp_any and iw >= 0 and y_ev[iw] == 1))
            ev_pick_correct_wp_cvl.append(bool(wp_any and icvl >= 0 and y_ev[icvl] == 1))

    out = {
        "w":                  np.array(ev_w,                  float),
        "mass":               np.array(ev_mass,               float),
        "wp_any":             np.array(ev_wp_any,             int),
        "matchable":          np.array(ev_matchable,          bool),
        "truth_pt":           np.array(ev_truthpt,            float),
        "score_best_all":     np.array(ev_score_best_all,     float),
        "picked_pass_wp_all": np.array(ev_picked_pass_wp_all, int),
        "score_best_wp":      np.array(ev_score_best_wp,      float),
    }
    if y_truth is not None:
        out["pick_correct_all"]    = np.array(ev_pick_correct_all,    bool)
        out["pick_correct_wp"]     = np.array(ev_pick_correct_wp,     bool)
        out["pick_correct_wp_cvl"] = np.array(ev_pick_correct_wp_cvl, bool)
    return out


# ----------------------------
# Selection helpers
# ----------------------------
def wp_only_selection(ev):
    return ev["wp_any"] == 1

def wp_assoc_selection(ev, thr):
    if ASSOC_PICK_ONLY_WITHIN_WP:
        return (ev["wp_any"] == 1) & (ev["score_best_wp"] >= thr)
    return ((ev["wp_any"] == 1)
            & (ev["picked_pass_wp_all"] == 1)
            & (ev["score_best_all"] >= thr))

def ml_only_selection(ev, thr):
    return ev["score_best_all"] >= thr

def wp_only_purity(ev, sel):
    return ev["pick_correct_wp_cvl"][sel]

def wp_assoc_purity(ev, sel):
    return (ev["pick_correct_wp"] if ASSOC_PICK_ONLY_WITHIN_WP
            else ev["pick_correct_all"])[sel]

def ml_only_purity(ev, sel):
    return ev["pick_correct_all"][sel]


def assoc_purity_in_truth_ptbin(method_name, sel_s_base, pt_lo, pt_hi=None):
    sel = sel_s_base & sig_ev["matchable"]
    pt  = sig_ev["truth_pt"]
    if pt_hi is not None:
        sel = sel & (pt >= pt_lo) & (pt < pt_hi)
    else:
        sel = sel & (pt >= pt_lo)
    if np.sum(sel) == 0:
        return float("nan"), float("nan"), 0.0
    corr = (wp_only_purity(sig_ev, sel)  if method_name.startswith("WP-only") else
            ml_only_purity(sig_ev, sel)  if method_name.startswith("ML-only") else
            wp_assoc_purity(sig_ev, sel))
    return purity_and_err(sig_ev["w"][sel], corr, use_abs_w=True)


# ----------------------------
# Diagnostic metrics
# ----------------------------
def compute_association_purity(ev, method="ML-only", thr=0.0):
    """
    Association purity = fraction of selected matchable events where the
    ranker's top-scored jet is the truth-matched charm jet.

    This is the primary ranker quality metric: it tells you how often the
    model picks the RIGHT jet, not just any jet that passes some WP.
    Computed only on matchable events (events where a truth jet exists).
    """
    if method == "WP-only":
        sel  = wp_only_selection(ev) & ev["matchable"]
        corr = wp_only_purity(ev, sel)
    elif method.startswith("WP+ML"):
        sel  = wp_assoc_selection(ev, thr) & ev["matchable"]
        corr = wp_assoc_purity(ev, sel)
    else:
        sel  = ml_only_selection(ev, thr) & ev["matchable"]
        corr = ml_only_purity(ev, sel)
    return purity_and_err(ev["w"][sel], corr, use_abs_w=True)


def compute_signal_efficiency(ev_sig, method="ML-only", thr=0.0, S_tot=None):
    """
    Signal efficiency = sum(|w| selected) / sum(|w| total).

    Measures what fraction of the signal survives your selection.
    Uses |w| so negative-weight events contribute positively to the count
    (they are real events, just with a sign flip from NLO interference).
    High signal efficiency is good but must be balanced against purity.
    """
    if method == "WP-only":
        sel = wp_only_selection(ev_sig)
    elif method.startswith("WP+ML"):
        sel = wp_assoc_selection(ev_sig, thr)
    else:
        sel = ml_only_selection(ev_sig, thr)

    S_sel = float(np.sum(np.abs(ev_sig["w"][sel])))
    S_all = S_tot if S_tot is not None else float(np.sum(np.abs(ev_sig["w"])))
    return safe_div(S_sel, S_all), S_sel, S_all


def compute_background_efficiency(ev_bkg, method="ML-only", thr=0.0, B_tot=None):
    """
    Background efficiency = sum(|w| selected bkg) / sum(|w| total bkg).

    Measures what fraction of the background passes your selection.
    You want this to be as LOW as possible (good background rejection).
    Together with signal efficiency, this gives the S/B trade-off curve.
    """
    if method == "WP-only":
        sel = wp_only_selection(ev_bkg)
    elif method.startswith("WP+ML"):
        sel = wp_assoc_selection(ev_bkg, thr)
    else:
        sel = ml_only_selection(ev_bkg, thr)

    B_sel = float(np.sum(np.abs(ev_bkg["w"][sel])))
    B_all = B_tot if B_tot is not None else float(np.sum(np.abs(ev_bkg["w"])))
    return safe_div(B_sel, B_all), B_sel, B_all


def print_metrics_table(ev_sig, ev_bkg, S_tot, B_tot):
    """Print association purity, signal efficiency, background efficiency."""
    header = (f"\n{'='*115}\n"
              f"{'METHOD':<20} {'THR':>5} | "
              f"{'ASSOC PURITY':>14} {'±ERR':>8} | "
              f"{'SIG EFF':>10} | "
              f"{'BKG EFF':>10} | "
              f"{'SIG/BKG EFF':>12}\n"
              f"{'='*115}")
    print(header)
    print("  Association Purity : fraction of selected matchable events where ranker picked the correct jet")
    print("  Signal Efficiency  : fraction of ALL signal events passing selection  (uses |w|)")
    print("  Background Eff     : fraction of ALL background events passing selection (lower = better)")
    print("  SIG/BKG Eff        : ratio > 1 means selection preferentially keeps signal")
    print("-"*115)

    for thr in thr_values:
        for method in ["WP-only", "ML-only", "WP+ML (AND)"]:
            if method == "WP-only" and thr > 0:
                continue            # WP-only has no threshold, print once
            if method == "WP+ML (AND)" and thr == 0.0:
                continue
            pur, err, _ = compute_association_purity(ev_sig, method, thr)
            s_eff, _, _ = compute_signal_efficiency(ev_sig, method, thr, S_tot)
            b_eff, _, _ = compute_background_efficiency(ev_bkg, method, thr, B_tot)
            ratio       = safe_div(s_eff, b_eff)
            thr_str     = "–" if method == "WP-only" else f"{thr:.1f}"
            row = (f"{method:<20} {thr_str:>5} | "
                   f"{pur:>14.4f} {err:>8.4f} | "
                   f"{s_eff:>10.4f} | "
                   f"{b_eff:>10.4f} | "
                   f"{ratio:>12.4f}")
            print(row)
        print("-"*115)
    print("="*115)


def plot_learning_curve(ranker):
    """
    Learning curve: validation NDCG@1 (or pairwise loss) vs boosting round.

    What to look for:
      - A curve that rises then plateaus = good, early stopping found the optimum
      - A curve that is still rising when training stops = more trees might help
      - A gap between train and val = overfitting; increase regularisation
      - Val curve worse than round 0 = something is wrong with the data/weights
    """
    results = ranker.evals_result()
    if not results:
        print("  [WARN] No eval results found.")
        return

    plt.figure(figsize=(8, 5))
    for split_name, metrics in results.items():
        for metric_name, vals in metrics.items():
            plt.plot(vals, linewidth=2, label=f"{split_name} – {metric_name}")
    if hasattr(ranker, "best_iteration"):
        plt.axvline(ranker.best_iteration, color="gray", linestyle="--",
                    alpha=0.7, label=f"best iter = {ranker.best_iteration}")
    plt.xlabel("Boosting Round")
    plt.ylabel("Metric")
    plt.title("XGBRanker Learning Curve\n"
              "(plateau = converged; gap between train/val = overfitting)")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig("learning_curve.png"); plt.close()
    print("  Saved: learning_curve.png")


def plot_efficiency_vs_threshold(ev_sig, ev_bkg, S_tot, B_tot):
    """
    Three-panel efficiency diagnostic.

    Panel 1 — Efficiency vs Threshold:
      Shows how signal efficiency (blue) and background efficiency (orange)
      change as you tighten the ML score cut. The gap between them is your
      discrimination power. Ideal: signal efficiency stays high while
      background efficiency drops quickly.

    Panel 2 — Signal Efficiency vs Background Efficiency (ROC-style):
      Each point is a different threshold. Upper-left corner = perfect
      discrimination. Comparing WP-only (single point) against the ML curve
      shows the gain from adding the ranker score.

    Panel 3 — Association Purity vs Threshold:
      As you raise the threshold you keep only high-confidence jets, so
      purity tends to rise. But if it drops at high threshold it means you
      are keeping unusual events where the ranker is less reliable.
    """
    thresholds = np.linspace(0.0, 1.0, 101)

    sig_eff_ml, bkg_eff_ml, pur_ml = [], [], []
    sig_eff_wp, bkg_eff_wp, pur_wp = [], [], []

    for thr in thresholds:
        se, _, _  = compute_signal_efficiency(ev_sig, "ML-only", thr, S_tot)
        be, _, _  = compute_background_efficiency(ev_bkg, "ML-only", thr, B_tot)
        p, _, _   = compute_association_purity(ev_sig, "ML-only", thr)
        sig_eff_ml.append(se); bkg_eff_ml.append(be); pur_ml.append(p)

        if thr > 0:
            se2, _, _ = compute_signal_efficiency(ev_sig, "WP+ML (AND)", thr, S_tot)
            be2, _, _ = compute_background_efficiency(ev_bkg, "WP+ML (AND)", thr, B_tot)
            p2,  _, _ = compute_association_purity(ev_sig, "WP+ML (AND)", thr)
            sig_eff_wp.append(se2); bkg_eff_wp.append(be2); pur_wp.append(p2)

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    ax = axes[0]
    ax.plot(thresholds, sig_eff_ml, linewidth=2, label="ML-only Sig Eff")
    ax.plot(thresholds, bkg_eff_ml, linewidth=2, linestyle="--", label="ML-only Bkg Eff")
    for thr_mark in thr_values[1:]:
        ax.axvline(thr_mark, color="gray", linestyle=":", alpha=0.5)
    ax.set_xlabel("ML Score Threshold (global norm)")
    ax.set_ylabel("Efficiency")
    ax.set_title("Signal & Background Efficiency vs Threshold\n"
                 "Gap between curves = discrimination power")
    ax.legend(); ax.grid(alpha=0.3)

    ax = axes[1]
    ax.plot(bkg_eff_ml, sig_eff_ml, linewidth=2, label="ML-only")
    ax.plot(bkg_eff_wp, sig_eff_wp, linewidth=2, linestyle="--", label="WP+ML")
    # WP-only single point
    se_wp, _, _ = compute_signal_efficiency(ev_sig, "WP-only", 0.0, S_tot)
    be_wp, _, _ = compute_background_efficiency(ev_bkg, "WP-only", 0.0, B_tot)
    ax.scatter([be_wp], [se_wp], s=120, zorder=5, marker="*", label="WP-only")
    for thr_mark in thr_values[1:]:
        idx = int(round(thr_mark * 100))
        ax.scatter([bkg_eff_ml[idx]], [sig_eff_ml[idx]], s=60, zorder=5,
                   label=f"ML thr={thr_mark:.1f}")
    ax.set_xlabel("Background Efficiency (lower = better bkg rejection)")
    ax.set_ylabel("Signal Efficiency")
    ax.set_title("Signal Eff vs Background Eff  [ROC-style]\n"
                 "Upper-left corner = ideal")
    ax.legend(fontsize=8); ax.grid(alpha=0.3)

    ax = axes[2]
    ax.plot(thresholds, pur_ml, linewidth=2, label="ML-only")
    ax.plot(thresholds[1:], pur_wp, linewidth=2, linestyle="--", label="WP+ML")
    ax.axhline(float(compute_association_purity(ev_sig, "WP-only")[0]),
               color="green", linestyle=":", linewidth=1.5, label="WP-only baseline")
    for thr_mark in thr_values[1:]:
        ax.axvline(thr_mark, color="gray", linestyle=":", alpha=0.5)
    ax.set_xlabel("ML Score Threshold")
    ax.set_ylabel("Association Purity")
    ax.set_title("Association Purity vs Threshold\n"
                 "(matchable events only — higher = ranker picks correct jet more often)")
    ax.legend(); ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig("efficiency_vs_threshold.png"); plt.close()
    print("  Saved: efficiency_vs_threshold.png")

    # Purity vs Signal Efficiency trade-off
    plt.figure(figsize=(7, 5))
    plt.plot(sig_eff_ml, pur_ml, linewidth=2, label="ML-only")
    plt.plot(sig_eff_wp, pur_wp, linewidth=2, linestyle="--", label="WP+ML")
    plt.xlabel("Signal Efficiency")
    plt.ylabel("Association Purity")
    plt.title("Association Purity vs Signal Efficiency\n"
              "Upper-right corner = keep many events AND pick the right jet")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig("efficiency_purity_vs_sig_eff.png"); plt.close()
    print("  Saved: efficiency_purity_vs_sig_eff.png")


def plot_negative_weight_impact(ev_sig, ev_bkg, S_tot, B_tot):
    """
    Negative weight diagnostic.

    Top row — N_eff vs threshold:
      N_eff = (Σ|w|)² / Σ(w²) measures how many unweighted events your
      selected sample is equivalent to. With purely positive weights
      N_eff = N_selected. Negative weights reduce N_eff. The drop tells
      you how much statistical power you are losing at each working point.

    Bottom row — Z_naive vs Z_corrected vs threshold:
      Z_naive   = S / sqrt(S+B)  — assumes Poisson, ignores neg weights
      Z_corrected = (S_pos+S_neg) / sqrt(S_pos²+S_neg²+|B|)
                  — accounts for variance inflation from neg weights
      The gap between the two curves is the overestimation in your
      significance from ignoring negative weights.
    """
    thresholds = np.linspace(0.0, 1.0, 51)
    neff_ml, neff_wp = [], []
    z_naive_ml, z_corr_ml = [], []
    z_naive_wp, z_corr_wp = [], []

    for thr in thresholds:
        for method, neff_list, zn_list, zc_list in [
            ("ML-only",    neff_ml,  z_naive_ml, z_corr_ml),
            ("WP+ML (AND)", neff_wp, z_naive_wp, z_corr_wp),
        ]:
            if method == "WP+ML (AND)" and thr == 0:
                neff_list.append(float("nan"))
                zn_list.append(float("nan"))
                zc_list.append(float("nan"))
                continue
            sel_s = ml_only_selection(ev_sig, thr) if method == "ML-only" else wp_assoc_selection(ev_sig, thr)
            sel_b = ml_only_selection(ev_bkg, thr) if method == "ML-only" else wp_assoc_selection(ev_bkg, thr)

            w_s = ev_sig["w"][sel_s]
            w_b = ev_bkg["w"][sel_b]
            Sp, Sn, _ = split_posneg_sumw(w_s)
            B          = float(np.sum(w_b))

            neff_list.append(effective_sample_size(w_s))
            zn_list.append(signif_s_over_sqrt_sb(Sp + Sn, B))
            zc_list.append(significance_with_neg_weights(Sp, Sn, B))

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle("Negative Weight Impact Diagnostics", fontsize=13)

    titles_top = ["ML-only: N_eff vs Threshold", "WP+ML: N_eff vs Threshold"]
    titles_bot = ["ML-only: Significance vs Threshold", "WP+ML: Significance vs Threshold"]
    neff_data  = [neff_ml, neff_wp]
    zn_data    = [z_naive_ml, z_naive_wp]
    zc_data    = [z_corr_ml, z_corr_wp]

    for col in range(2):
        ax = axes[0, col]
        ax.plot(thresholds, neff_data[col], linewidth=2, color="steelblue")
        ax.set_xlabel("ML Score Threshold")
        ax.set_ylabel("N_eff = (Σ|w|)² / Σ(w²)")
        ax.set_title(titles_top[col] + "\n(lower N_eff = more statistical power lost)")
        ax.grid(alpha=0.3)

        ax = axes[1, col]
        ax.plot(thresholds, zn_data[col], linewidth=2, label="Z naive  S/√(S+B)", color="tomato")
        ax.plot(thresholds, zc_data[col], linewidth=2, label="Z corrected", color="steelblue",
                linestyle="--")
        ax.set_xlabel("ML Score Threshold")
        ax.set_ylabel("Significance Z")
        ax.set_title(titles_bot[col] + "\n(gap = overestimation from ignoring neg weights)")
        ax.legend(); ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig("negative_weight_impact.png"); plt.close()
    print("  Saved: negative_weight_impact.png")


# ==============================
# [1] Load data
# ==============================
print("[1/6] Loading samples...")
sig_arr, sig_w, sig_hpt, sig_hphi, sig_hmass, sig_uids = load_parquet(sig_file, SIG_SAMPLE)
bkg_arr, bkg_w, bkg_hpt, bkg_hphi, bkg_hmass, bkg_uids = load_parquet(bkg_file, BKG_SAMPLE)

print("\nBuilding signal (matchable-only, for training / purity)...")
sig_matchable = build_jet_rows(sig_arr, sig_w, sig_hpt, sig_hphi, sig_hmass, sig_uids,
                               need_truth=True, drop_unmatched=True)

print("\nBuilding signal (ALL events, for yields / significance)...")
sig_all = build_jet_rows(sig_arr, sig_w, sig_hpt, sig_hphi, sig_hmass, sig_uids,
                         need_truth=True, drop_unmatched=False)

print("\nBuilding background...")
bkg_jets = build_jet_rows(bkg_arr, bkg_w, bkg_hpt, bkg_hphi, bkg_hmass, bkg_uids,
                          need_truth=False, drop_unmatched=False)

st = sig_all["stats"]
print("\n" + "="*60)
print("DIAGNOSTIC AUDIT:")
print(f"  Signal : total_with_jets={st['total_events_with_jets']}, "
      f"matchable={st['matchable_events']}, unmatchable={st['unmatchable_events']}")
print(f"  Bkg    : rows={len(bkg_jets['X'])}")
print("="*60)

if len(sig_matchable["X"]) == 0:
    print("!!! ERROR: No matchable events found in Signal.")
    sys.exit(1)

evt2w  = event_weight_map(sig_all["evt"], sig_all["w_evt"])
wvals  = np.array(list(evt2w.values()), float)
n_evt  = len(wvals)
n_neg  = int(np.sum(wvals < 0))
wabs_neg = float(np.sum(np.abs(wvals[wvals < 0])))
wabs_tot = float(np.sum(np.abs(wvals)))
N_eff_full = effective_sample_size(wvals)

print(f"\n=== Signal negative-weight report ===")
print(f"  N_events={n_evt},  N_w<0={n_neg} ({100*n_neg/n_evt if n_evt else 0:.2f}%)")
if wabs_tot > 0:
    print(f"  |w| fraction from negative events = {wabs_neg/wabs_tot:.4f}")
print(f"  N_eff (full sample) = {N_eff_full:.1f}  "
      f"(vs N_events={n_evt} → {100*N_eff_full/n_evt:.1f}% statistical power retained)")


# ==============================
# [2] Charm parton plots
# ==============================
print("\n[2/6] Charm parton plots...")

if len(sig_matchable.get("charm_pt", [])) > 0:
    wch = np.abs(sig_matchable["charm_w"])

    plt.figure(figsize=(7, 5))
    plt.hist(sig_matchable["charm_pt"], bins=np.linspace(0, 200, 41),
             weights=wch, histtype="step", linewidth=2)
    plt.xlabel("Charm parton pT [GeV]")
    plt.ylabel("Weighted entries (|w_evt|)")
    plt.title("Signal charm pT (last-copy, mother-chain validated)")
    plt.grid(True, alpha=0.3); plt.tight_layout()
    plt.savefig("charm_parton_pt.png"); plt.close()

    plt.figure(figsize=(7, 5))
    plt.hist(sig_matchable["charm_eta"], bins=np.linspace(-5, 5, 51),
             weights=wch, histtype="step", linewidth=2)
    plt.xlabel("Charm parton η")
    plt.ylabel("Weighted entries (|w_evt|)")
    plt.title("Signal charm η (last-copy, mother-chain validated)")
    plt.grid(True, alpha=0.3); plt.tight_layout()
    plt.savefig("charm_parton_eta.png"); plt.close()
    print("  Saved: charm_parton_pt.png, charm_parton_eta.png")
else:
    print("  [WARN] charm_pt not filled.")


# ==============================
# [3] Training — rank:pairwise + improved hyperparameters + early stopping
# ==============================
print("\n[3/6] Training ranker...")

feature_names = ["pT(H)/pT(j)", "|Δφ(H,j)|", "pT(H)"]
if USE_FLAVOUR_IN_RANKER:
    feature_names += ["CvsL", "CvsB"]

sig_events = np.unique(sig_matchable["evt"])

# 70% train / 15% val (early stopping) / 15% test
ev_trainval, ev_test = train_test_split(sig_events, test_size=0.15, random_state=42)
ev_train, ev_val     = train_test_split(ev_trainval,
                                        test_size=VAL_FRACTION / (1.0 - 0.15),
                                        random_state=42)

evt2w_match = event_weight_map(sig_matchable["evt"], sig_matchable["w_evt"])
if DROP_NEG_W_IN_TRAIN:
    ev_train = np.array([e for e in ev_train if evt2w_match.get(int(e), 0.0) >= 0.0], np.int64)
    ev_val   = np.array([e for e in ev_val   if evt2w_match.get(int(e), 0.0) >= 0.0], np.int64)
if DROP_NEG_W_IN_TEST:
    ev_test  = np.array([e for e in ev_test  if evt2w_match.get(int(e), 0.0) >= 0.0], np.int64)

def make_split(ev_ids, data):
    mask = np.isin(data["evt"], ev_ids)
    return (data["X"][mask], data["y"][mask], data["w_ml"][mask], data["evt"][mask])

X_train, y_train, w_train, e_train = make_split(ev_train, sig_matchable)
X_val,   y_val,   w_val,   e_val   = make_split(ev_val,   sig_matchable)
X_test,  y_test,  w_test,  e_test  = make_split(ev_test,  sig_matchable)

e_train_s, (X_train_s, y_train_s, w_train_s), _ = sort_by_event(e_train, X_train, y_train, w_train)
e_val_s,   (X_val_s,   y_val_s,   w_val_s),   _ = sort_by_event(e_val,   X_val,   y_val,   w_val)
e_test_s,  (X_test_s,  y_test_s,  w_test_s),  _ = sort_by_event(e_test,  X_test,  y_test,  w_test)

group_train   = make_group_sizes(e_train_s)
group_w_train = group_weights_from_sorted(e_train_s, w_train_s, use_abs=True)
group_val     = make_group_sizes(e_val_s)

print(f"  Train events: {len(ev_train)}, Val events: {len(ev_val)}, Test events: {len(ev_test)}")
print(f"  Objective: rank:pairwise  (original, interpretable scores)")

# rank:pairwise — original objective, kept as requested
# Hyperparameters improved vs original:
#   max_depth 4→6, lr 0.05→0.02, added min_child_weight/gamma/reg_alpha/reg_lambda
#   n_estimators 1000→2000 (early stopping will find the true optimum)
ranker = xgb.XGBRanker(
    objective="rank:pairwise",
    tree_method="hist",
    n_estimators=2000,
    max_depth=6,
    learning_rate=0.02,
    subsample=0.7,
    colsample_bytree=0.7,
    min_child_weight=5,
    gamma=1.0,
    reg_alpha=0.1,
    reg_lambda=1.5,
    n_jobs=8,
    random_state=42,
    early_stopping_rounds=50,
)

ranker.fit(
    X_train_s, y_train_s,
    group=group_train,
    sample_weight=group_w_train,
    eval_set=[(X_val_s, y_val_s)],
    eval_group=[group_val],
    verbose=200,
)

best_iter = getattr(ranker, "best_iteration", ranker.n_estimators)
print(f"  XGBRanker training complete. Best iteration: {best_iter}")
plot_learning_curve(ranker)


# ==============================
# [3b] Export to ONNX
# ==============================
print("\n[3b] Exporting ranker to ONNX...")
import onnx

n_features = X_train_s.shape[1]
try:
    booster = ranker.get_booster()
    proxy   = xgb.XGBRegressor()
    proxy._Booster = booster

    initial_type = [("float_input", FloatTensorType([None, n_features]))]
    onnx_model   = convert_xgboost(proxy, initial_types=initial_type)

    with open(ONNX_MODEL_PATH, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"  ONNX model saved to : {ONNX_MODEL_PATH}")
    print(f"  Number of features  : {n_features}")
    print(f"  Feature names       : {feature_names}")
except Exception as e:
    print(f"  [ERROR] ONNX export failed: {e}")
    sys.exit(1)


# ==============================
# [4] Feature shape plots
# ==============================
print("\n[4/6] Feature shape plots...")

for i, fname in enumerate(feature_names):
    mask_t = y_train_s == 1
    mask_o = y_train_s == 0
    vals_t = X_train_s[mask_t, i]
    vals_o = X_train_s[mask_o, i]
    wt     = np.abs(w_train_s[mask_t])
    wo     = np.abs(w_train_s[mask_o])

    if fname in ("CvsL", "CvsB"):
        good_t = vals_t > -1;  vals_t = vals_t[good_t];  wt = wt[good_t]
        good_o = vals_o > -1;  vals_o = vals_o[good_o];  wo = wo[good_o]

    if wt.sum() > 0: wt = wt / wt.sum()
    if wo.sum() > 0: wo = wo / wo.sum()

    plt.figure(figsize=(7, 5))
    plt.hist(vals_o, bins=40, weights=wo, histtype="step", linewidth=2, label="Non-truth jets")
    plt.hist(vals_t, bins=40, weights=wt, histtype="step", linewidth=2, label="Truth-matched jets")
    plt.xlabel(fname); plt.ylabel("Normalised entries")
    plt.title(f"Training Feature Shape: {fname}\n"
              f"Separation between curves = discriminating power of this feature")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(f"feature_shape_{i}.png"); plt.close()

print("  Saved: feature_shape_*.png")


# ==============================
# [5] Evaluation
# ==============================
print("\n[5/6] Evaluating...")
y_score_test_raw = ranker.predict(X_test_s)

# --- Score distributions (jet-level) ---
# Shows how well the ranker separates truth-matched jets from non-truth jets.
# Good separation (well-separated peaks) → high jet-level AUC.
# Note: score distributions use raw scores; thresholds use globally-normalised scores.
mask_t = y_test_s == 1
mask_o = y_test_s == 0
plt.figure(figsize=(7, 5))
plt.hist(y_score_test_raw[mask_o], bins=40, weights=np.abs(w_test_s[mask_o]),
         histtype="step", linewidth=2, label="Non-truth jets")
plt.hist(y_score_test_raw[mask_t], bins=40, weights=np.abs(w_test_s[mask_t]),
         histtype="step", linewidth=2, label="Truth-matched jets")
plt.xlabel("Ranker score (raw)")
plt.ylabel("Weighted entries (|w|)")
plt.title("Jet-level Ranker Score Distribution (matchable test)\n"
          "Better separation → ranker more confidently identifies the correct jet")
plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
plt.savefig("ranker_score_distribution_test.png"); plt.close()
print("  Saved: ranker_score_distribution_test.png")

# --- Jet-level ROC ---
# AUC = probability that the ranker scores a random truth jet higher than
# a random non-truth jet. AUC=1.0 is perfect, AUC=0.5 is random.
fpr, tpr, _ = roc_curve(y_test_s, y_score_test_raw, sample_weight=w_test_s)
roc_auc = auc(fpr, tpr)
print(f"  Jet-level ROC AUC: {roc_auc:.3f}")
plt.figure(figsize=(6, 5))
plt.plot(fpr, tpr, linewidth=2, label=f"Ranker score (AUC = {roc_auc:.3f})")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
plt.title("Jet-level ROC Curve\n"
          "AUC = P(truth jet scored higher than non-truth jet)")
plt.grid(True, alpha=0.3); plt.legend(loc="lower right"); plt.tight_layout()
plt.savefig("jet_level_ROC_XGBRankerScore.png"); plt.close()
print("  Saved: jet_level_ROC_XGBRankerScore.png")

top1 = top1_correct_per_event(e_test_s, y_score_test_raw, y_test_s)
print(f"  Event-level top-1 correct (matchable test): {top1.mean()*100:.2f}%")

# --- Predict on FULL samples + global normalisation ---
raw_sig = ranker.predict(sig_all["X"])
raw_bkg = ranker.predict(bkg_jets["X"])
print(f"  Raw score range — sig: [{raw_sig.min():.4f}, {raw_sig.max():.4f}]")
print(f"  Raw score range — bkg: [{raw_bkg.min():.4f}, {raw_bkg.max():.4f}]")

sig_scores_all, bkg_scores_all, score_lo, score_hi = normalize_scores_global(raw_sig, raw_bkg)
print(f"  Global normalisation range: [{score_lo:.4f}, {score_hi:.4f}]")

# Reduce to event-level
sig_ev = reduce_to_events_consistent(
    sig_all["evt"], sig_scores_all, sig_all["cvl"], sig_all["cvb"],
    sig_all["w_evt"], sig_all["H_mass"],
    y_truth=sig_all["y"], matchable_perjet=sig_all["matchable"],
    truth_pt_perjet=sig_all.get("truth_pt"),
)
bkg_ev = reduce_to_events_consistent(
    bkg_jets["evt"], bkg_scores_all, bkg_jets["cvl"], bkg_jets["cvb"],
    bkg_jets["w_evt"], bkg_jets["H_mass"],
)

# --- Selected jet pT distribution ---
# Shows the pT spectrum of the jet the ranker chose as the charm candidate.
# Signal should peak at lower pT (soft charm jets); background peaks higher.
# A large difference in shape → good discriminating variable for downstream analysis.
def selected_jet_pt(d, scores, evt_ids):
    pts, ws = [], []
    for ev in np.unique(evt_ids):
        m   = evt_ids == ev
        r   = d["X"][m, 0]
        Hpt = d["X"][m, 2]
        jpt = np.where(r > 0, Hpt / r, 0.0)
        idx = int(np.argmax(scores[m]))
        pts.append(jpt[idx]); ws.append(d["w_evt"][m][0])
    return np.array(pts), np.array(ws)

sig_sel_pt, sig_sel_w = selected_jet_pt(sig_all,  sig_scores_all, sig_all["evt"])
bkg_sel_pt, bkg_sel_w = selected_jet_pt(bkg_jets, bkg_scores_all, bkg_jets["evt"])

plt.figure(figsize=(7, 5))
plt.hist(bkg_sel_pt, bins=np.linspace(0, 200, 41), weights=bkg_sel_w,
         histtype="step", linewidth=2, label="ggH")
plt.hist(sig_sel_pt, bins=np.linspace(0, 200, 41), weights=sig_sel_w,
         histtype="step", linewidth=2, label="H+c")
plt.xlabel("pT of selected jet [GeV]")
plt.ylabel("Weighted events")
plt.title(f"Selected Jet pT  (USE_FLAVOUR={USE_FLAVOUR_IN_RANKER})\n"
          "pT of the jet the ranker chose as charm candidate")
plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
plt.savefig("selected_jet_pt_distribution.png"); plt.close()
print("  Saved: selected_jet_pt_distribution.png")

# --- Event-level ROC ---
# At event level, "correct" = ranker picked the truth-matched jet.
# This ROC shows: can the ranker's confidence (score_best_all) discriminate
# events where it got the right jet from events where it got the wrong one?
# Low AUC here means the ranker doesn't know when it's wrong — a calibration issue.
mask_m    = sig_ev["matchable"]
y_evt_roc = sig_ev["pick_correct_all"][mask_m].astype(int)
s_evt_roc = sig_ev["score_best_all"][mask_m]
w_evt_roc = np.abs(sig_ev["w"][mask_m])

if len(np.unique(y_evt_roc)) >= 2:
    fpr_e, tpr_e, _ = roc_curve(y_evt_roc, s_evt_roc, sample_weight=w_evt_roc)
    auc_e = auc(fpr_e, tpr_e)
    print(f"  Event-level ROC AUC (correct vs incorrect, matchable signal): {auc_e:.3f}")
    plt.figure(figsize=(6, 5))
    plt.plot(fpr_e, tpr_e, linewidth=2, label=f"Event ROC (AUC = {auc_e:.3f})")
    plt.xlabel("FPR (incorrect events kept)")
    plt.ylabel("TPR (correct events kept)")
    plt.title("Event-level ROC: Correct vs Incorrect (Signal, matchable)\n"
              "High AUC = ranker's score reliably flags events where it picked the right jet")
    plt.grid(True, alpha=0.3); plt.legend(loc="lower right"); plt.tight_layout()
    plt.savefig("event_level_ROC_correct_vs_incorrect.png"); plt.close()
    print("  Saved: event_level_ROC_correct_vs_incorrect.png")
else:
    print("  [WARN] Event-level ROC skipped.")


# ==============================
# [5b] Association Purity + Efficiency metrics + Negative weight diagnostics
# ==============================
print("\n[5b] Association Purity & Efficiency metrics...")

S_tot = float(np.sum(np.abs(sig_ev["w"])))
B_tot = float(np.sum(np.abs(bkg_ev["w"])))

S_tot_full     = float(np.sum(sig_ev["w"]))
S_tot_match    = float(np.sum(sig_ev["w"][sig_ev["matchable"]]))
S_tot_unmatch  = float(np.sum(sig_ev["w"][~sig_ev["matchable"]]))
B_tot_signed   = float(np.sum(bkg_ev["w"]))


print_metrics_table(sig_ev, bkg_ev, S_tot, B_tot)
plot_efficiency_vs_threshold(sig_ev, bkg_ev, S_tot, B_tot)
plot_negative_weight_impact(sig_ev, bkg_ev, S_tot, B_tot)

# Negative weight impact table
print("\n=== Negative weight impact per working point ===")
print(f"  {'METHOD':<20} {'THR':>5} | {'N_eff':>10} | "
      f"{'neg_frac':>10} | {'Z_naive':>10} | {'Z_corrected':>12}")
print("  " + "-"*75)
for method, thr in [("WP-only", None), ("ML-only", 0.0), ("ML-only", 0.3), ("ML-only", 0.6),
                    ("WP+ML (AND)", 0.3), ("WP+ML (AND)", 0.6)]:
    if method == "WP-only":
        sel_s = wp_only_selection(sig_ev)
        sel_b = wp_only_selection(bkg_ev)
    elif method == "ML-only":
        sel_s = ml_only_selection(sig_ev, thr)
        sel_b = ml_only_selection(bkg_ev, thr)
    else:
        sel_s = wp_assoc_selection(sig_ev, thr)
        sel_b = wp_assoc_selection(bkg_ev, thr)

    w_s = sig_ev["w"][sel_s]
    w_b = bkg_ev["w"][sel_b]
    Sp, Sn, _ = split_posneg_sumw(w_s)
    B          = float(np.sum(w_b))
    neff       = effective_sample_size(w_s)
    neg_frac   = abs(Sn) / (Sp + abs(Sn)) if (Sp + abs(Sn)) > 0 else float("nan")
    z_n        = signif_s_over_sqrt_sb(Sp + Sn, B)
    z_c        = significance_with_neg_weights(Sp, Sn, B)
    thr_str    = "–" if method == "WP-only" else f"{thr:.1f}"
    print(f"  {method:<20} {thr_str:>5} | {neff:>10.1f} | "
          f"{neg_frac:>10.4f} | {z_n:>10.4f} | {z_c:>12.4f}")

print("\n=== Association Purity in truth-pT bins ===")
for method, thr in [("ML-only", 0.0), ("ML-only", 0.3), ("ML-only", 0.6),
                    ("WP-only", 0.0), ("WP+ML (AND)", 0.3), ("WP+ML (AND)", 0.6)]:
    if method == "WP+ML (AND)":
        sel_base = wp_assoc_selection(sig_ev, thr)
    elif method == "WP-only":
        sel_base = wp_only_selection(sig_ev)
    else:
        sel_base = ml_only_selection(sig_ev, thr)

    p20, e20, _ = assoc_purity_in_truth_ptbin(method, sel_base, PTBIN_LOW_LO, PTBIN_LOW_HI)
    p30, e30, _ = assoc_purity_in_truth_ptbin(method, sel_base, PTBIN_HIGH_MIN, None)
    thr_str = f"thr={thr:.1f}" if method != "WP-only" else "  –  "
    print(f"  {method:<20} {thr_str}  "
          f"purity(20-30 GeV)={p20:.4f}±{e20:.4f}   "
          f"purity(>=30 GeV)={p30:.4f}±{e30:.4f}")


# ==============================
# [6] Tables
# ==============================
print("\n[6/6] Building comparison tables...")


print(f"  S_full        = {S_tot_full:.6e}")
print(f"  S_matchable   = {S_tot_match:.6e}")
print(f"  S_unmatchable = {S_tot_unmatch:.6e}")
print(f"  B_total       = {B_tot_signed:.6e}")


def significance_breakdown(method, thr, sel_s, sel_b):
    S_full = float(np.sum(sig_ev["w"][sel_s]))
    B      = float(np.sum(bkg_ev["w"][sel_b]))
    mflag  = sig_ev["matchable"]
    S_m    = float(np.sum(sig_ev["w"][sel_s &  mflag]))
    S_u    = float(np.sum(sig_ev["w"][sel_s & ~mflag]))
    Sp, Sn, _ = split_posneg_sumw(sig_ev["w"][sel_s])
    Z_naive = signif_s_over_sqrt_sb(S_full, B)
    Z_corr  = significance_with_neg_weights(Sp, Sn, B)
    print(f"{method:<12} thr={thr:<4} | "
          f"S={S_full: .4e} (S+={Sp: .4e}, S-={Sn: .4e}) "
          f"S_m={S_m: .4e} S_u={S_u: .4e} B={B: .4e} | "
          f"Z_naive={Z_naive: .4f}  Z_corr={Z_corr: .4f}")


print("\n=== Significance breakdown (Z_naive vs Z_corrected for neg weights) ===")
for thr in thr_values:
    significance_breakdown("WP-only",  "-",          wp_only_selection(sig_ev),       wp_only_selection(bkg_ev))
    significance_breakdown("ML-only",  f"{thr:.1f}", ml_only_selection(sig_ev, thr),  ml_only_selection(bkg_ev, thr))
    if thr > 0:
        significance_breakdown("WP+ML", f"{thr:.1f}", wp_assoc_selection(sig_ev, thr), wp_assoc_selection(bkg_ev, thr))
    print("-" * 140)


def compute_row(method, thr_str, sel_s_base, sel_b, sig_cat_mask, S_tot_cat):
    sel_s_cat = sel_s_base & sig_cat_mask
    S  = float(np.sum(sig_ev["w"][sel_s_cat]))
    B  = float(np.sum(bkg_ev["w"][sel_b]))
    Sp, Sn, _ = split_posneg_sumw(sig_ev["w"][sel_s_cat])

    Z_naive      = signif_s_over_sqrt_sb(S, B)
    Z_corr       = significance_with_neg_weights(Sp, Sn, B)
    S_over_sqrtB = safe_div(S, math.sqrt(B)) if B > 0 else float("nan")
    S_eff        = safe_div(S, S_tot_cat)     if np.isfinite(S_tot_cat) else float("nan")
    B_eff        = safe_div(B, B_tot_signed)
    event_pur    = safe_div(S, S + B)

    sel_m = sel_s_base & sig_ev["matchable"]
    corr  = (wp_only_purity(sig_ev, sel_m)  if method.startswith("WP-only") else
             ml_only_purity(sig_ev, sel_m)  if method.startswith("ML-only") else
             wp_assoc_purity(sig_ev, sel_m))
    p, e, _ = purity_and_err(sig_ev["w"][sel_m], corr, use_abs_w=True)

    row = (f"{method:<22} {thr_str:>4}  {S: .6e}  {B: .6e}  "
           f"{Z_naive: .4f}  {Z_corr: .4f}  {S_over_sqrtB: .4e}  "
           f"{S_eff: .3f}  {B_eff: .3f}  {p: .3f}  {e: .3f}  {event_pur: .3f}")

    p20, e20, _ = assoc_purity_in_truth_ptbin(method, sel_s_base, PTBIN_LOW_LO,   PTBIN_LOW_HI)
    p30, e30, _ = assoc_purity_in_truth_ptbin(method, sel_s_base, PTBIN_HIGH_MIN, None)
    extra = f"    pur(20-30)={p20: .3f}±{e20: .3f}  pur(>=30)={p30: .3f}±{e30: .3f}"
    return row, extra


def make_comparison_table(cat_name, sig_cat_mask, out_txt):
    S_tot_cat = float(np.sum(sig_ev["w"][sig_cat_mask]))
    header = (f"{'Method':<22} {'thr':>4}  {'S_yield':>12}  {'B_yield':>12}  "
              f"{'Z_naive':>8}  {'Z_corr':>8}  {'S/sqrtB':>12}  "
              f"{'S_eff':>6}  {'B_eff':>6}  {'pur':>6}  {'err':>6}  {'S/(S+B)':>8}")
    lines = [
        f"=== {cat_name} ===",
        f"Totals: S_tot={S_tot_cat:.6e}  B_tot={B_tot_signed:.6e}",
        "Z_naive = S/sqrt(S+B)  |  Z_corr accounts for neg-weight variance",
        header,
    ]
    for l in lines: print(l)

    for thr in thr_values:
        for method, sel_s_fn, sel_b_fn in [
            ("WP-only",      lambda _: wp_only_selection(sig_ev),      lambda _: wp_only_selection(bkg_ev)),
            ("ML-only",      lambda t: ml_only_selection(sig_ev, t),    lambda t: ml_only_selection(bkg_ev, t)),
            ("WP+ML (AND)",  lambda t: wp_assoc_selection(sig_ev, t),   lambda t: wp_assoc_selection(bkg_ev, t)),
        ]:
            if method == "WP+ML (AND)" and thr == 0:
                continue
            thr_str = "-" if method == "WP-only" else f"{thr:.1f}"
            row, extra = compute_row(method, thr_str,
                                     sel_s_fn(thr), sel_b_fn(thr),
                                     sig_cat_mask, S_tot_cat)
            lines += [row, extra]
            print(row); print(extra)
        sep = "-" * 130; lines.append(sep); print(sep)

    with open(out_txt, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  Saved: {out_txt}\n")


make_comparison_table("FULL signal",      np.ones_like(sig_ev["w"], dtype=bool), "comparison_table_full.txt")
make_comparison_table("MATCHABLE-only",   sig_ev["matchable"].astype(bool),       "comparison_table_matchable.txt")
make_comparison_table("UNMATCHABLE-only", (~sig_ev["matchable"]).astype(bool),    "comparison_table_unmatchable.txt")

print("\n=== All outputs saved ===")
print("  charm_parton_pt.png / charm_parton_eta.png")
print("  feature_shape_*.png")
print("  ranker_score_distribution_test.png")
print("  jet_level_ROC_XGBRankerScore.png")
print("  event_level_ROC_correct_vs_incorrect.png")
print("  selected_jet_pt_distribution.png")
print("  learning_curve.png")
print("  efficiency_vs_threshold.png")
print("  efficiency_purity_vs_sig_eff.png")
print("  negative_weight_impact.png")
print("  comparison_table_full.txt / comparison_table_matchable.txt / comparison_table_unmatchable.txt")
print(f"  {ONNX_MODEL_PATH}")