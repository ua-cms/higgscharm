#!/usr/bin/env python3
"""
H+c (H->4l) jet association: Parquet Version
Includes Tiered Truth Matching (Strict -> HardProcess -> Any Charm)
and diagnostic unit checks.
"""

import numpy as np
import awkward as ak
import pyarrow.dataset as ds
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc
import xgboost as xgb
import math
import os

# ============================================================
# CONFIGURATION & PATHS
# ============================================================
BASE = "/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/"
sig_file = BASE + "SomeSMSignal/"
bkg_file = BASE + "GluGluHtoZZto4L/"

CVL_WP = 0.16
CVB_WP = 0.306
DR_MATCH = 0.5 # Slightly relaxed for initial matching
USE_FLAVOUR_IN_RANKER = True

# Significance bins
thr_values = [0.0, 0.3, 0.6]

# ============================================================
# UTILITIES
# ============================================================
def delta_phi(phi1, phi2):
    return (phi1 - phi2 + np.pi) % (2 * np.pi) - np.pi

def significance(S, B):
    return S / math.sqrt(S + B) if (S + B) > 0 else 0.0

# ============================================================
# PARQUET LOADER
# ============================================================
def load_arrays_parquet(path):
    print(f"\n[INFO] Loading dataset from: {path}")
    if not os.path.exists(path):
        print(f"[ERROR] Path does not exist: {path}")
        return None, None, None, None, None, None

    dataset = ds.dataset(path, format="parquet")
    columns = [
        "weight_nominal", "zz_pt_inclusive", "zz_mass_inclusive", "zz_phi_inclusive",
        "cjets_pt", "cjets_eta", "cjets_phi", "leadingjet_cvsl", "leadingjet_cvsb",
        "genpart_pdgId", "genpart_pt", "genpart_eta", "genpart_phi", 
        "genpart_isHardProcess", "genpart_isLastCopy"
    ]
    
    avail = dataset.schema.names
    cols_to_load = [c for c in columns if c in avail]
    
    table = dataset.to_table(columns=cols_to_load)
    arrays = ak.from_arrow(table)

    w_evt = arrays["weight_nominal"]
    H_pt_evt = ak.fill_none(ak.firsts(arrays["zz_pt_inclusive"]), 0)
    H_mass_evt = ak.fill_none(ak.firsts(arrays["zz_mass_inclusive"]), 0)
    H_phi_evt = ak.fill_none(ak.firsts(arrays["zz_phi_inclusive"]), 0)
    evt_uid = np.arange(len(H_pt_evt), dtype=np.int64)

    return arrays, w_evt, H_pt_evt, H_mass_evt, H_phi_evt, evt_uid

# ============================================================
# BUILD JET DATASET (WITH TIERED TRUTH MATCHING)
# ============================================================
def build_jet_rows(arrays, w_evt, H_pt_evt, H_mass_evt, H_phi_evt, evt_uid, need_truth, drop_unmatched=False):
    X_list, evt_list, w_list, mass_list = [], [], [], []
    cvl_list, cvb_list = [], []
    y_list, matchable_list, truthpt_list = [], [], []
    
    n_events = len(H_pt_evt)
    matched_count = 0

    for ievt in range(n_events):
        jets_pt = np.array(arrays["cjets_pt"][ievt])
        if len(jets_pt) == 0: continue

        uid = int(evt_uid[ievt])
        w_event = float(w_evt[ievt])
        Hpt = float(H_pt_evt[ievt])
        Hm  = float(H_mass_evt[ievt])
        Hphi = float(H_phi_evt[ievt])
        
        cvl = float(arrays["leadingjet_cvsl"][ievt])
        cvb = float(arrays["leadingjet_cvsb"][ievt])
        jets_eta = np.array(arrays["cjets_eta"][ievt])
        jets_phi = np.array(arrays["cjets_phi"][ievt])
        
        matched_mask = np.zeros(len(jets_pt), dtype=bool)
        evt_is_matchable = False
        evt_truth_charm_pt = np.nan

        if need_truth:
            pdg = np.abs(np.array(arrays["genpart_pdgId"][ievt]))
            is_hard = np.array(arrays["genpart_isHardProcess"][ievt]).astype(bool)
            is_last = np.array(arrays["genpart_isLastCopy"][ievt]).astype(bool)
            
            # --- TIERED TRUTH SELECTION ---
            # 1. Strict (HardProcess AND LastCopy)
            mask = (pdg == 4) & is_hard & is_last
            cand_idx = np.where(mask)[0]
            
            # 2. Fallback: Any HardProcess charm
            if len(cand_idx) == 0:
                mask = (pdg == 4) & is_hard
                cand_idx = np.where(mask)[0]
                
            # 3. Last Resort: Any charm at all
            if len(cand_idx) == 0:
                mask = (pdg == 4)
                cand_idx = np.where(mask)[0]

            if len(cand_idx) > 0:
                g_pts = np.array(arrays["genpart_pt"][ievt])[cand_idx]
                g_etas = np.array(arrays["genpart_eta"][ievt])[cand_idx]
                g_phis = np.array(arrays["genpart_phi"][ievt])[cand_idx]

                best_dr = 999.0
                best_j = None
                best_pt = np.nan

                for i in range(len(cand_idx)):
                    dr = np.sqrt((jets_eta - g_etas[i])**2 + delta_phi(jets_phi, g_phis[i])**2)
                    idx_min = np.argmin(dr)
                    if dr[idx_min] < best_dr:
                        best_dr = dr[idx_min]
                        best_j = idx_min
                        best_pt = g_pts[i]
                
                if best_j is not None and best_dr < DR_MATCH:
                    matched_mask[best_j] = True
                    evt_is_matchable = True
                    evt_truth_charm_pt = best_pt
                    matched_count += 1

            if drop_unmatched and not evt_is_matchable:
                continue

        # Append jets to dataset
        for j in range(len(jets_pt)):
            ratio = jets_pt[j] / Hpt if Hpt > 0 else 0
            dphi = abs(delta_phi(jets_phi[j], Hphi))
            
            feat = [ratio, dphi, Hpt]
            if USE_FLAVOUR_IN_RANKER:
                feat += [cvl, cvb]
            
            X_list.append(feat)
            evt_list.append(uid)
            w_list.append(w_event)
            mass_list.append(Hm)
            cvl_list.append(cvl)
            cvb_list.append(cvb)
            if need_truth:
                y_list.append(1 if matched_mask[j] else 0)
                matchable_list.append(evt_is_matchable)
                truthpt_list.append(evt_truth_charm_pt)

    print(f"   -> Matchable events found: {matched_count} / {n_events}")

    out = {
        "X": np.array(X_list), "evt": np.array(evt_list), "w_evt": np.array(w_list),
        "H_mass": np.array(mass_list), "cvl": np.array(cvl_list), "cvb": np.array(cvb_list)
    }
    if need_truth:
        out.update({"y": np.array(y_list), "matchable": np.array(matchable_list), "truth_pt": np.array(truthpt_list)})
    return out

# ============================================================
# REDUCTION
# ============================================================
def reduce_to_events(data, scores):
    u_evts = np.unique(data["evt"])
    res = {k: [] for k in ["w", "mass", "matchable", "truth_pt", "score_best", "pass_wp", "correct"]}
    
    for ev in u_evts:
        m = (data["evt"] == ev)
        idx = np.argmax(scores[m])
        res["w"].append(data["w_evt"][m][idx])
        res["mass"].append(data["H_mass"][m][idx])
        res["score_best"].append(scores[m][idx])
        is_wp = (data["cvl"][m][idx] > CVL_WP) and (data["cvb"][m][idx] > CVB_WP)
        res["pass_wp"].append(is_wp)
        if "y" in data:
            res["matchable"].append(data["matchable"][m][idx])
            res["truth_pt"].append(data["truth_pt"][m][idx])
            res["correct"].append(data["y"][m][idx] == 1)
            
    return {k: np.array(v) for k, v in res.items()}

# ============================================================
# MAIN
# ============================================================
sig_arr, sig_w, sig_hpt, sig_hm, sig_hphi, sig_uids = load_arrays_parquet(sig_file)
bkg_arr, bkg_w, bkg_hpt, bkg_hm, bkg_hphi, bkg_uids = load_arrays_parquet(bkg_file)

# Unit check
if len(sig_arr) > 0:
    mean_gen_pt = np.mean(ak.flatten(sig_arr["genpart_pt"]))
    mean_jet_pt = np.mean(ak.flatten(sig_arr["cjets_pt"]))
    print(f"\n[DIAGNOSTIC] Mean Gen Pt: {mean_gen_pt:.2f}, Mean Jet Pt: {mean_jet_pt:.2f}")
    if mean_gen_pt > 500 and mean_jet_pt < 500:
        print("[WARNING] GenPart pT appears to be in MeV, but jets are in GeV! Truth matching may fail.")

print("\n[BUILD] Creating sig_matchable (Training set)...")
sig_matchable = build_jet_rows(sig_arr, sig_w, sig_hpt, sig_hm, sig_hphi, sig_uids, True, True)

unique_matchable_ids = np.unique(sig_matchable["evt"])
if len(unique_matchable_ids) == 0:
    exit("\n[ERROR] Still no matchable events found. Verify coordinate units and DR_MATCH.")

# Split
u_sig_train, u_sig_test = train_test_split(unique_matchable_ids, test_size=0.3, random_state=42)
train_mask = np.isin(sig_matchable["evt"], u_sig_train)

# Fit
print("\n[TRAIN] Training XGBRanker...")
ranker = xgb.XGBRanker(objective="rank:pairwise", n_estimators=600, learning_rate=0.05, max_depth=4, n_jobs=8)
_, groups = np.unique(sig_matchable["evt"][train_mask], return_counts=True)
ranker.fit(sig_matchable["X"][train_mask], sig_matchable["y"][train_mask], group=groups)

# Eval
print("\n[EVAL] Running significance analysis...")
sig_all_jets = build_jet_rows(sig_arr, sig_w, sig_hpt, sig_hm, sig_hphi, sig_uids, True, False)
bkg_all_jets = build_jet_rows(bkg_arr, bkg_w, bkg_hpt, bkg_hm, bkg_hphi, bkg_uids, False, False)

sig_scores = ranker.predict(sig_all_jets["X"])
bkg_scores = ranker.predict(bkg_all_jets["X"])

sig_ev = reduce_to_events(sig_all_jets, sig_scores)
bkg_ev = reduce_to_events(bkg_all_jets, bkg_scores)

print("\n" + "="*60)
print(f"{'Method':<15} | {'Thr':<5} | {'S':<10} | {'B':<10} | {'Z':<7}")
print("-" * 60)
for thr in thr_values:
    s_mask = (sig_ev["pass_wp"]) & (sig_ev["score_best"] >= thr)
    b_mask = (bkg_ev["pass_wp"]) & (bkg_ev["score_best"] >= thr)
    S, B = np.sum(sig_ev["w"][s_mask]), np.sum(bkg_ev["w"][b_mask])
    print(f"{'WP+ML':<15} | {thr:<5.1f} | {S:<10.2e} | {B:<10.2e} | {significance(S, B):<7.3f}")

ranker.save_model("ranker_hplusc_v1.ubj")
print("\nModel saved. Run complete.")