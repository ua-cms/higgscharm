#!/usr/bin/env python3
"""
Compute Z+X background yield using AN-18-340 Eq. 11 (true SS method).

Formula (applied per-event over the CR_SS DATA parquets):
    N_{Z+X} = (OS/SS)^MC * sum_{DATA, CR_SS} f1 * f2

where:
    (OS/SS)^MC  = weightedN_MC_reducible(CR_2P2F, OS) / weightedN_MC_reducible(CR_SS, SS)
                  Ratio of weight_nominal-weighted reducible MC (DY+WZ+tt)
                  event sums in the OS 2P2F CR vs. the SS CR. Corrects for
                  FSR and charge-asymmetric fake topologies. Computed strictly
                  from MC (the 'reducible' bucket) — data never enters this ratio.
    f1 * f2     = product of fake rates for the two SS Z2 leptons (l1, l2),
                  evaluated at their (pT, |eta|, flavour).
                  MUON legs  -> fake_rates.npz (unchanged).
                  ELECTRON legs -> fit_ZLL.py's corrected_fr_EB/EE graphs, if
                  --electron-fr-zll is given (see CHANGE note below);
                  otherwise falls back to fake_rates.npz for electrons too.
    DATA sum    = over ALL CR_SS data events (no m4l window).

This version matches the *actual* configs in use:
  - OS side:  hplus_FR.yaml -> categories CR_3P1F / CR_2P2F
              (branches: m4l_3p1f, m4l_2p2f, l3_*_3p1f/2p2f, l4_*_3p1f/2p2f, ...)
              Only CR_2P2F is needed here, for the MC ratio numerator.
  - SS side:  zplus_ss-based config -> single category CR_SS
              (branches: l1_pt/eta/flavor, l2_pt/eta/flavor,
               zll_mass_{4e,4mu,2e2mu,2mu2e}_sscr)

Because the OS and SS configs write to two SEPARATE output trees, this
script takes two separate input directories (--cr-input-os, --cr-input-ss)
rather than a single --cr-input.

======================== CHANGE: electron FR from fit_ZLL ===================
Added --electron-fr-zll <path to fit_ZLL.py's output pkl>. When given, any
lepton leg with |pdgId| == 11 has its fake rate (and error) looked up from
that pkl's "corrected_fr_EB"/"corrected_fr_EE" graphs (barrel/endcap split
at |eta| < 1.479, same convention as fit_ZL.py/fit_ZLL.py) instead of
fake_rates.npz. Muon legs (|pdgId| == 13) are completely unaffected -- they
always use fake_rates.npz, exactly as before.

The graph lookup rule matches the original FakeRateReader.get() /
graph_lookup() convention used throughout this pipeline: the first graph
point whose x (pT) is strictly greater than the query pT, else the last
point; rate clipped to [0.001, 0.999]. If --electron-fr-zll is not given,
behavior is byte-for-byte the same as before this change (electrons also
use fake_rates.npz).
===============================================================================

Inputs:
    - OS tree: hplus_FR output dir (per-dataset, per-category parquet files)
    - SS tree: hplus_FR_SS output dir (per-dataset, per-category parquet files)
    - fake_rates/<era>/fake_rates.npz (from compute_fake_rates.py)
    - [optional] fit_ZLL.py output pkl (corrected electron FR graphs)

Output:
    - Printed yield table per era and final state

Usage:
    python compute_an_ss_yield.py \\
        --cr-input-os /eos/user/p/pkatris/higgscharm/outputs/hplus_FR \\
        --cr-input-ss /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_SS \\
        --fake-rates  /eos/user/p/pkatris/higgscharm/outputs/fake_rates \\
        --electron-fr-zll /path/to/fit_ZLL_2022preEE.pkl \\
        --year all
"""

import os
import glob
import argparse
import pickle
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# ── Dataset classification keywords ──────────────────────────────────────────
_DATA_KW      = ('Muon', 'EGamma', 'DoubleEG', 'DoubleMuon', 'MuonEG',
                 'SingleMuon', 'SingleElectron', 'SinglePhoton')
_REDUCIBLE_KW = ('DYJetsToLL', 'WZto3LNu', 'WZTo3LNu', 'WZ_', 'TTto2L2Nu')
_ZZ_KW        = ('ZZto4L', 'GluGluToContinto', 'GluGlutoContinto')

# ── Electron barrel/endcap split, matches fit_ZL.py / fit_ZLL.py exactly ────
_ELECTRON_BARREL_ETA_CUT = 1.479


# ── Dataset classification ────────────────────────────────────────────────────

def _classify(folder):
    if any(kw in folder for kw in _DATA_KW):
        return 'data'
    if any(kw in folder for kw in _REDUCIBLE_KW):
        return 'reducible'
    if any(kw in folder for kw in _ZZ_KW):
        return 'zz'
    return 'other'


# ── Fake rate loading (muon / fallback-electron source) ──────────────────────

def load_fake_rates(path):
    """Load fake rates from .npz (numpy-version agnostic) or .pkl fallback."""
    npz_path = path.replace('.pkl', '.npz')
    if os.path.exists(npz_path):
        raw = np.load(npz_path, allow_pickle=False)
        fr = {}
        for key in raw.files:
            parts = key.split('__')
            flavor, subkey, field = parts[0], parts[1], parts[2]
            if flavor not in fr:
                fr[flavor] = {}
            if subkey == 'top':
                fr[flavor][field] = raw[key]
            else:
                if subkey not in fr[flavor]:
                    fr[flavor][subkey] = {}
                fr[flavor][subkey][field] = raw[key]
        return fr
    with open(path, 'rb') as f:
        return pickle.load(f)


def get_fake_rate(pt, eta, pdgid, fake_rates):
    """Return (rate, err) for a single lepton -- always from fake_rates.npz."""
    flavor = 'electron' if abs(int(pdgid)) == 11 else 'muon'
    fr = fake_rates[flavor]
    pt_bin  = int(np.clip(np.searchsorted(fr['pt_edges'],  pt)  - 1, 0, len(fr['pt_edges'])  - 2))
    eta_bin = int(np.clip(np.searchsorted(fr['eta_edges'], abs(eta)) - 1, 0, len(fr['eta_edges']) - 2))
    rate = float(fr['fake_rate'][pt_bin, eta_bin])
    err  = float(fr['fake_rate_err'][pt_bin, eta_bin])
    if err >= 1.0:   # sentinel for undefined bins
        err = 0.0
    return rate, err


# ── NEW: electron fake rate from fit_ZLL.py's corrected FR graphs ────────────

def load_electron_fr_graphs(path):
    """Load fit_ZLL.py's output pkl: {'corrected_fr_EB': graph, 'corrected_fr_EE': graph}
    where graph = {'x','y','ex','ey'} numpy arrays (see fit_ZLL.py / fit_ZL.py)."""
    with open(path, 'rb') as f:
        return pickle.load(f)


def graph_lookup(graph, x_query):
    """Same lookup rule as FakeRateReader.get() throughout this pipeline:
    first point whose x is strictly greater than x_query, else the last
    point. Returns (rate, err)."""
    x, y, ey = graph['x'], graph['y'], graph['ey']
    if len(x) == 0:
        return 0.0, 0.0
    above = np.nonzero(x_query < x)[0]
    idx = int(above[0]) if len(above) else len(x) - 1
    rate = float(np.clip(y[idx], 0.001, 0.999))
    err = float(ey[idx])
    return rate, err


def get_electron_fake_rate_zll(pt, eta, electron_fr_graphs):
    """Electron-only lookup from fit_ZLL's corrected_fr_EB/EE graphs,
    barrel/endcap split at |eta| < 1.479 (matches fit_ZL.py/fit_ZLL.py)."""
    graph = (electron_fr_graphs['corrected_fr_EB'] if abs(eta) < _ELECTRON_BARREL_ETA_CUT
             else electron_fr_graphs['corrected_fr_EE'])
    return graph_lookup(graph, pt)


def get_fake_rate_array(pt_arr, eta_arr, pdgid_arr, fake_rates, electron_fr_graphs=None):
    """
    Vectorised fake rate lookup.
    Returns arrays (rate, err) same length as inputs.

    If electron_fr_graphs is given, any |pdgId| == 11 leg is looked up from
    those graphs instead of fake_rates.npz. Muon legs (|pdgId| == 13) always
    use fake_rates.npz, unaffected by electron_fr_graphs.
    """
    rates = np.zeros(len(pt_arr))
    errs  = np.zeros(len(pt_arr))
    for i, (pt, eta, pdgid) in enumerate(zip(pt_arr, eta_arr, pdgid_arr)):
        if electron_fr_graphs is not None and abs(int(pdgid)) == 11:
            rates[i], errs[i] = get_electron_fake_rate_zll(pt, eta, electron_fr_graphs)
        else:
            rates[i], errs[i] = get_fake_rate(pt, eta, pdgid, fake_rates)
    return rates, errs


def get_fake_rate_variant_array(pt_arr, eta_arr, pdgid_arr, fake_rates, variant, electron_fr_graphs=None):
    """
    Vectorised fake rate lookup returning a single shifted variant:
      variant='up' -> rate + err   (matches FakeRates::GetFakeRate_Up)
      variant='dn' -> max(0, rate - err)  (matches FakeRates::GetFakeRate_Dn)
    """
    rate, err = get_fake_rate_array(pt_arr, eta_arr, pdgid_arr, fake_rates,
                                     electron_fr_graphs=electron_fr_graphs)
    if variant == 'up':
        return np.minimum(rate + err, 0.999999)
    if variant == 'dn':
        return np.maximum(rate - err, 0.0)
    return rate


# ── Parquet loading helpers ───────────────────────────────────────────────────

def _load_parquets_for_category(cr_dir, year, category):
    """
    Walk cr_dir/<year>/<dataset>/<category>/ and load all parquet files.
    Returns a dict {'data': DataFrame, 'reducible': DataFrame, 'zz': DataFrame, 'other': DataFrame}.
    """
    era_dir = os.path.join(cr_dir, year)
    if not os.path.exists(era_dir):
        raise FileNotFoundError(f"CR output not found: {era_dir}")

    pattern = os.path.join(era_dir, '*', category, '*.parquet')
    files   = glob.glob(pattern)
    print(f"  [{category}] found {len(files)} parquet files in {era_dir}")

    frames = {'data': [], 'reducible': [], 'zz': [], 'other': []}
    for fpath in files:
        # dataset name is the directory two levels up from the parquet file
        dataset_dir = os.path.basename(os.path.dirname(os.path.dirname(fpath)))
        kind = _classify(dataset_dir)
        try:
            df = pq.read_table(fpath).to_pandas()
        except Exception as e:
            print(f"    Warning: could not load {fpath}: {e}")
            continue
        frames[kind].append(df)

    result = {}
    for kind, dfs in frames.items():
        if dfs:
            result[kind] = pd.concat(dfs, ignore_index=True)
        else:
            result[kind] = pd.DataFrame()
    return result


def _extract_scalar(series):
    """
    Parquet lists → numpy float array.
    Some columns are written as list-of-one-element in a minority of files
    (see merge script docstrings); extract that element. Handles None entries.
    """
    def _get(val):
        if val is None:
            return np.nan
        if isinstance(val, (list, np.ndarray)):
            return float(val[0]) if len(val) > 0 else np.nan
        return float(val)
    return np.array([_get(v) for v in series])


# ── (OS/SS)^MC ratio — RAW (unweighted) entry counts, split by final state ──

_FINAL_STATES = ['4e', '4mu', '2e2mu', '2mu2e']


def _os_final_state(df):
    """
    Final state (4e/4mu/2e2mu/2mu2e) for CR_2P2F OS rows, built from the
    Z1 leg flavor (z1_l1_pdgid_2p2f) and the Z2 (failing) leg flavor
    (l3_pdgid_2p2f — l3/l4 are always same-flavor since Z2 is OSSF).
    Convention: Z1-flavor first, Z2-flavor second (matches the SS side's
    zll_mass_2e2mu_sscr vs zll_mass_2mu2e_sscr split).
    """
    result = np.full(len(df), '', dtype=object)
    if df.empty or 'z1_l1_pdgid_2p2f' not in df.columns or 'l3_pdgid_2p2f' not in df.columns:
        return result
    z1 = np.abs(_extract_scalar(df['z1_l1_pdgid_2p2f']))
    z2 = np.abs(_extract_scalar(df['l3_pdgid_2p2f']))
    result[(z1 == 11) & (z2 == 11)] = '4e'
    result[(z1 == 13) & (z2 == 13)] = '4mu'
    result[(z1 == 11) & (z2 == 13)] = '2e2mu'
    result[(z1 == 13) & (z2 == 11)] = '2mu2e'
    return result


def _ss_final_state(df):
    """
    Final state (4e/4mu/2e2mu/2mu2e) for CR_SS rows, read directly off
    which of the four flavor-specific zll_mass_*_sscr columns is non-null
    for that row (each event only has its own final state's mass filled).
    """
    result = np.full(len(df), '', dtype=object)
    if df.empty:
        return result
    col_map = {
        '4e':     'zll_mass_4e_sscr',
        '4mu':    'zll_mass_4mu_sscr',
        '2e2mu':  'zll_mass_2e2mu_sscr',
        '2mu2e':  'zll_mass_2mu2e_sscr',
    }
    for fs, col in col_map.items():
        if col not in df.columns:
            continue
        vals = _extract_scalar(df[col])
        result[~np.isnan(vals)] = fs
    return result


def _weighted_sum(df):
    """
    Sum of weight_nominal over the rows of df. Falls back to raw row count
    if weight_nominal isn't present (shouldn't happen for reducible MC).
    NaNs in weight_nominal are treated as 0 (nansum) rather than propagating.
    """
    if df.empty:
        return 0.0
    if 'weight_nominal' not in df.columns:
        print("    WARNING: 'weight_nominal' column missing — falling back to raw count")
        return float(len(df))
    return float(np.nansum(_extract_scalar(df['weight_nominal'])))


def compute_os_ss_ratio(cr_input_os, cr_input_ss, year):
    """
    Compute (OS/SS)^MC = weightedN(reducible MC, CR_2P2F) / weightedN(reducible MC, CR_SS)
    both overall and split by final state (4e / 4mu / 2e2mu / 2mu2e).

    MC-ONLY: computed strictly on the 'reducible' bucket (DY+WZ+tt) from
    each tree — DATA and ZZ never enter this ratio. Weighted by
    weight_nominal (lumi/xsec/genWeight/pileup, i.e. the MC event weight),
    NOT raw entry counts — this properly accounts for cross-section and
    per-event weight differences between the OS and SS MC samples.

    No m4l window applied.

    Returns (overall_ratio, n_os_total, n_ss_total, per_final_state) where
    n_os_total/n_ss_total are WEIGHTED sums (not raw counts), and
    per_final_state = {'4e': (ratio, n_os, n_ss), '4mu': (...), '2e2mu': (...), '2mu2e': (...)}.
    """
    os_frames = _load_parquets_for_category(cr_input_os, year, 'CR_2P2F')
    df_os = os_frames.get('reducible', pd.DataFrame())   # MC only

    ss_frames = _load_parquets_for_category(cr_input_ss, year, 'CR_SS')
    df_ss = ss_frames.get('reducible', pd.DataFrame())   # MC only

    n_os = _weighted_sum(df_os)
    n_ss = _weighted_sum(df_ss)
    print(f"    N_MC_OS  (CR_2P2F, reducible, weight_nominal sum): {n_os:.3f}")
    print(f"    N_MC_SS  (CR_SS,   reducible, weight_nominal sum): {n_ss:.3f}")

    if n_ss <= 0:
        print("    WARNING: N_MC_SS = 0 — cannot compute overall ratio. Defaulting to 1.0")
        overall_ratio = 1.0
    else:
        overall_ratio = n_os / n_ss
        print(f"    (OS/SS)^MC [overall] = {n_os:.3f} / {n_ss:.3f} = {overall_ratio:.4f}")

    # ── per-final-state split (also weighted) ───────────────────────────
    fs_os = _os_final_state(df_os)
    fs_ss = _ss_final_state(df_ss)
    w_os  = _extract_scalar(df_os['weight_nominal']) if not df_os.empty and 'weight_nominal' in df_os.columns else np.array([])
    w_ss  = _extract_scalar(df_ss['weight_nominal']) if not df_ss.empty and 'weight_nominal' in df_ss.columns else np.array([])

    print(f"    Per-final-state (OS/SS)^MC (weighted):")
    per_final_state = {}
    for fs in _FINAL_STATES:
        sel_os = (fs_os == fs)
        sel_ss = (fs_ss == fs)
        n_os_fs = float(np.nansum(w_os[sel_os])) if len(w_os) else 0.0
        n_ss_fs = float(np.nansum(w_ss[sel_ss])) if len(w_ss) else 0.0
        if n_ss_fs <= 0:
            ratio_fs = 1.0
            note = " (N_MC_SS=0, ratio defaulted to 1.0)"
        else:
            ratio_fs = n_os_fs / n_ss_fs
            note = ""
        print(f"      {fs:<8} N_MC_OS={n_os_fs:>10.3f}  N_MC_SS={n_ss_fs:>10.3f}  "
              f"ratio={ratio_fs:>8.4f}{note}")
        per_final_state[fs] = (ratio_fs, n_os_fs, n_ss_fs)

    return overall_ratio, n_os, n_ss, per_final_state


# ── DATA SS weighted sum ──────────────────────────────────────────────────────

def compute_data_ss_weighted(df_data, fake_rates, electron_fr_graphs=None):
    """
    Compute sum_{DATA CR_SS} f1*f2 from parquet DataFrame (l1, l2 legs).
    No mass window applied — inclusive over all CR_SS data events.

    Returns (weighted_sum, stat_err, n_events)
    where stat_err = sqrt(sum_i (f1_i * f2_i)^2) (CR stat uncertainty).
    """
    if df_data.empty:
        print("    WARNING: no DATA events in CR_SS")
        return 0.0, 0.0, 0

    df = df_data.reset_index(drop=True)
    n_events = len(df)
    print(f"    DATA events in CR_SS: {n_events}")

    if n_events == 0:
        return 0.0, 0.0, 0

    pt1  = _extract_scalar(df['l1_pt'])
    eta1 = _extract_scalar(df['l1_eta'])
    pdg1 = _extract_scalar(df['l1_flavor'])   # already |pdgId| per config (11 or 13)
    pt2  = _extract_scalar(df['l2_pt'])
    eta2 = _extract_scalar(df['l2_eta'])
    pdg2 = _extract_scalar(df['l2_flavor'])

    f1, f1_err = get_fake_rate_array(pt1, eta1, pdg1, fake_rates, electron_fr_graphs=electron_fr_graphs)
    f2, f2_err = get_fake_rate_array(pt2, eta2, pdg2, fake_rates, electron_fr_graphs=electron_fr_graphs)

    weights      = f1 * f2
    weighted_sum = float(np.sum(weights))
    stat_err     = float(np.sqrt(np.sum(weights**2)))

    return weighted_sum, stat_err, n_events


# ── Final-state breakdown (4e / 4mu / 2e2mu / 2mu2e) ─────────────────────────

def compute_final_state_breakdown(df_data, fake_rates, electron_fr_graphs=None):
    """
    Breakdown of DATA CR_SS f1*f2 by final state (4e/4mu/2e2mu/2mu2e), read
    from which zll_mass_*_sscr column is filled per row. Returns
    {'4e': (yield_sum, yield_sum_up, yield_sum_dn, n_events), '4mu': (...), ...}

    yield_sum_up/dn use f1,f2 both shifted to their Up/Dn fake-rate variant
    (matches SSmethod.C::MakeHistogramsZX, which fills separate nominal/Up/Dn
    histograms using FR->GetFakeRate / GetFakeRate_Up / GetFakeRate_Dn).
    """
    if df_data.empty:
        return {}

    df = df_data.reset_index(drop=True)
    if len(df) == 0:
        return {}

    fs = _ss_final_state(df)
    pt1  = _extract_scalar(df['l1_pt'])
    eta1 = _extract_scalar(df['l1_eta'])
    pdg1 = _extract_scalar(df['l1_flavor'])
    pt2  = _extract_scalar(df['l2_pt'])
    eta2 = _extract_scalar(df['l2_eta'])
    pdg2 = _extract_scalar(df['l2_flavor'])

    result = {}
    for final_state in _FINAL_STATES:
        sel = (fs == final_state)
        n_sel = int(np.sum(sel))
        if n_sel == 0:
            result[final_state] = (0.0, 0.0, 0.0, 0)
            continue
        f1, _ = get_fake_rate_array(pt1[sel], eta1[sel], pdg1[sel], fake_rates, electron_fr_graphs=electron_fr_graphs)
        f2, _ = get_fake_rate_array(pt2[sel], eta2[sel], pdg2[sel], fake_rates, electron_fr_graphs=electron_fr_graphs)
        f1_up = get_fake_rate_variant_array(pt1[sel], eta1[sel], pdg1[sel], fake_rates, 'up', electron_fr_graphs=electron_fr_graphs)
        f2_up = get_fake_rate_variant_array(pt2[sel], eta2[sel], pdg2[sel], fake_rates, 'up', electron_fr_graphs=electron_fr_graphs)
        f1_dn = get_fake_rate_variant_array(pt1[sel], eta1[sel], pdg1[sel], fake_rates, 'dn', electron_fr_graphs=electron_fr_graphs)
        f2_dn = get_fake_rate_variant_array(pt2[sel], eta2[sel], pdg2[sel], fake_rates, 'dn', electron_fr_graphs=electron_fr_graphs)

        w    = f1 * f2
        w_up = f1_up * f2_up
        w_dn = f1_dn * f2_dn
        result[final_state] = (float(np.sum(w)), float(np.sum(w_up)), float(np.sum(w_dn)), n_sel)

    return result


# ── Per-era calculation ───────────────────────────────────────────────────────

def compute_year(year, cr_input_os, cr_input_ss, fr_dir, electron_fr_graphs=None):
    print(f"\n{'='*64}")
    print(f"  Era: {year}")
    print(f"{'='*64}")

    # ── Fake rates ──────────────────────────────────────────────────────────
    fr_path = os.path.join(fr_dir, year, 'fake_rates.pkl')
    if not os.path.exists(fr_path) and not os.path.exists(fr_path.replace('.pkl', '.npz')):
        print(f"  Fake rates not found: {fr_path} — skipping.")
        return None
    fake_rates = load_fake_rates(fr_path)
    print(f"  Fake rates loaded.")
    if electron_fr_graphs is not None:
        print(f"  Electron fake rates: using fit_ZLL corrected graphs "
              f"(corrected_fr_EB/EE). Muons still use fake_rates.npz.")
    else:
        print(f"  Electron fake rates: using fake_rates.npz (no --electron-fr-zll given).")

    # ── (OS/SS)^MC ratio (raw entries, no weights, no mass window) ──────────
    print(f"\n  Computing (OS/SS)^MC ratio (raw entries)...")
    os_ss_ratio, n_mc_os, n_mc_ss, os_ss_ratio_by_fs = compute_os_ss_ratio(
        cr_input_os, cr_input_ss, year
    )

    # ── DATA SS events ──────────────────────────────────────────────────────
    print(f"\n  Loading DATA CR_SS parquets...")
    ss_frames = _load_parquets_for_category(cr_input_ss, year, 'CR_SS')
    df_data   = ss_frames.get('data', pd.DataFrame())

    if df_data.empty:
        print(f"  No DATA events in CR_SS for {year} — skipping.")
        return None

    # ── DATA SS f1*f2 weighted sum (inclusive, no mass window) ─────────────
    print(f"\n  Computing DATA SS f1*f2 sum (inclusive, no mass window)...")
    data_weighted, data_stat_err, n_data_ss = compute_data_ss_weighted(
        df_data, fake_rates, electron_fr_graphs=electron_fr_graphs
    )

    # ── Final-state breakdown (4e/4mu/2e2mu/2mu2e) ──────────────────────────
    print(f"\n  Computing final-state breakdown (nominal + fake-rate Up/Dn)...")
    fs_breakdown = compute_final_state_breakdown(
        df_data, fake_rates, electron_fr_graphs=electron_fr_graphs
    )

    # ── Final yield — SUM OF PER-FINAL-STATE YIELDS, ASYMMETRIC ERRORS ──────
    # Each final state gets its own (OS/SS)^MC ratio rather than one blended
    # ratio applied uniformly, since the OS/SS correction is final-state
    # dependent (different FSR / charge-asymmetry effects for ee vs mumu legs).
    #
    # Error propagation matches SSmethod.C::MakeHistogramsZX / PrintZXYields:
    #   stat      = Y / sqrt(N_events_CR)          (per final state)
    #   syst_up   = Y * ((Y_up/Y) - 1)              Y_up = ratio * sum(f1_up*f2_up)
    #   syst_dn   = Y * (1 - (Y_dn/Y))              Y_dn = ratio * sum(f1_dn*f2_dn)
    #   syst_comp = Y * 0.30                        (flat 30% bkg-composition syst)
    #   comb_up   = sqrt(stat^2 + syst_up^2 + syst_comp^2)
    #   comb_dn   = sqrt(stat^2 + syst_dn^2 + syst_comp^2)
    # syst_up != syst_dn because Y is a PRODUCT of two independently-shifted
    # fake rates (f1*f2): (f1+e1)(f2+e2) and (f1-e1)(f2-e2) move away from the
    # nominal product by different amounts (their cross term e1*e2 has the same
    # sign in both directions), so the resulting envelope is asymmetric even
    # when the individual fake-rate uncertainties are symmetric.
    BKG_COMPOSITION_SYST = 0.30

    yield_total    = 0.0
    yield_err_up2  = 0.0
    yield_err_dn2  = 0.0
    fs_yields = {}
    for fs in _FINAL_STATES:
        y_raw, y_raw_up, y_raw_dn, n = fs_breakdown.get(fs, (0.0, 0.0, 0.0, 0))
        ratio_fs, n_os_fs, n_ss_fs = os_ss_ratio_by_fs.get(fs, (1.0, 0.0, 0.0))

        Y    = ratio_fs * y_raw
        Y_up = ratio_fs * y_raw_up
        Y_dn = ratio_fs * y_raw_dn

        stat = (Y / np.sqrt(n)) if n > 0 else 0.0
        syst_up   = Y * ((Y_up / Y) - 1.0) if Y != 0 else 0.0
        syst_dn   = Y * (1.0 - (Y_dn / Y)) if Y != 0 else 0.0
        syst_comp = Y * BKG_COMPOSITION_SYST

        comb_up = float(np.sqrt(stat**2 + syst_up**2 + syst_comp**2))
        comb_dn = float(np.sqrt(stat**2 + syst_dn**2 + syst_comp**2))

        fs_yields[fs] = {
            'Y': Y, 'n': n, 'ratio': ratio_fs,
            'stat': stat, 'syst_up': syst_up, 'syst_dn': syst_dn,
            'syst_comp': syst_comp, 'comb_up': comb_up, 'comb_dn': comb_dn,
        }
        yield_total   += Y
        yield_err_up2 += comb_up ** 2
        yield_err_dn2 += comb_dn ** 2

    yield_err_up = float(np.sqrt(yield_err_up2))
    yield_err_dn = float(np.sqrt(yield_err_dn2))

    # overall-ratio version too, for comparison
    yield_total_overall_ratio = os_ss_ratio * data_weighted
    yield_err_overall_ratio   = os_ss_ratio * data_stat_err

    # ── Print results ───────────────────────────────────────────────────────
    print(f"\n  {'─'*60}")
    print(f"  AN Eq. 11 SS Yield — {year}  (inclusive, no m4l window)")
    print(f"  {'─'*60}")
    print(f"  N_DATA_SS (inclusive):           {n_data_ss:>8d}")
    print(f"  N_MC_OS   (CR_2P2F, weighted):   {n_mc_os:>10.3f}")
    print(f"  N_MC_SS   (CR_SS, weighted):     {n_mc_ss:>10.3f}")
    print(f"  >>> (OS/SS)^MC ratio [overall]:  {os_ss_ratio:>10.4f}  <<<")
    print(f"  sum_DATA f1*f2 [overall]:        {data_weighted:>10.4f}  ± {data_stat_err:.4f}")
    print(f"  Net Z+X yield [overall ratio]:   {yield_total_overall_ratio:>10.4f}  "
          f"± {yield_err_overall_ratio:.4f}   (for comparison only)")

    print(f"\n  Per-final-state breakdown (each with ITS OWN OS/SS ratio):")
    print(f"  {'Final state':<10}  {'N_data':>7}  {'Yield':>8}  {'stat':>7}  "
          f"{'syst_up':>8}  {'syst_dn':>8}  {'syst_cmp':>8}  {'+comb':>8}  {'-comb':>8}")
    print(f"  {'─'*90}")
    for fs in _FINAL_STATES:
        v = fs_yields[fs]
        print(f"  {fs:<10}  {v['n']:>7d}  {v['Y']:>8.3f}  {v['stat']:>7.3f}  "
              f"{v['syst_up']:>8.3f}  {v['syst_dn']:>8.3f}  {v['syst_comp']:>8.3f}  "
              f"{v['comb_up']:>8.3f}  {v['comb_dn']:>8.3f}")
    print(f"  {'─'*90}")
    print(f"  >>> Net Z+X yield [sum of per-final-state]: {yield_total:>10.4f}  "
          f"+{yield_err_up:.4f} / -{yield_err_dn:.4f}  <<<")
    print(f"  {'─'*60}")

    return {
        'year':                       year,
        'n_data_ss':                  n_data_ss,
        'n_mc_os':                    n_mc_os,
        'n_mc_ss':                    n_mc_ss,
        'os_ss_ratio':                os_ss_ratio,
        'os_ss_ratio_by_fs':          os_ss_ratio_by_fs,
        'data_weighted':              data_weighted,
        'data_stat_err':              data_stat_err,
        'fs_breakdown':               fs_breakdown,
        'fs_yields':                  fs_yields,
        'yield_total':                yield_total,
        'yield_err_up':               yield_err_up,
        'yield_err_dn':               yield_err_dn,
        'yield_total_overall_ratio':  yield_total_overall_ratio,
        'yield_err_overall_ratio':    yield_err_overall_ratio,
    }


# ── Summary table ─────────────────────────────────────────────────────────────

def print_summary(results):
    print(f"\n{'='*64}")
    print(f"  AN-18-340 Eq. 11 SS Method — Z+X Yield Summary (inclusive)")
    print(f"  (per-final-state OS/SS ratios, asymmetric stat+syst errors)")
    print(f"{'='*64}")
    print(f"  {'Era':<16}  {'N_DATA_SS':>10}  {'N_MC_OS':>12}  {'N_MC_SS':>12}  {'Net Z+X':>20}")
    print(f"  {'─'*80}")
    total_yield    = 0.0
    total_err_up2  = 0.0
    total_err_dn2  = 0.0
    for r in results:
        print(f"  {r['year']:<16}  {r['n_data_ss']:>10d}  {r['n_mc_os']:>12.3f}  {r['n_mc_ss']:>12.3f}  "
              f"{r['yield_total']:>8.2f} +{r['yield_err_up']:.2f}/-{r['yield_err_dn']:.2f}")
        total_yield   += r['yield_total']
        total_err_up2 += r['yield_err_up'] ** 2
        total_err_dn2 += r['yield_err_dn'] ** 2
    total_err_up = float(np.sqrt(total_err_up2))
    total_err_dn = float(np.sqrt(total_err_dn2))
    print(f"  {'─'*80}")
    print(f"  {'Total':<16}  {'':>10}  {'':>12}  {'':>12}  "
          f"{total_yield:>8.2f} +{total_err_up:.2f}/-{total_err_dn:.2f}")
    print(f"{'='*64}")

    # per-final-state totals across all eras shown
    print(f"\n  Per-final-state totals across shown eras:")
    print(f"  {'Final state':<10}  {'Net Z+X':>22}")
    print(f"  {'─'*36}")
    for fs in _FINAL_STATES:
        y      = sum(r['fs_yields'][fs]['Y'] for r in results)
        eu2    = sum(r['fs_yields'][fs]['comb_up'] ** 2 for r in results)
        ed2    = sum(r['fs_yields'][fs]['comb_dn'] ** 2 for r in results)
        print(f"  {fs:<10}  {y:>10.2f} +{np.sqrt(eu2):.2f}/-{np.sqrt(ed2):.2f}")
    print(f"{'='*64}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compute Z+X yield using AN-18-340 Eq. 11 (true SS method)'
    )
    parser.add_argument('--cr-input-os', required=True,
                        help='Path to hplus_FR output dir (OS side: CR_3P1F/CR_2P2F)')
    parser.add_argument('--cr-input-ss', required=True,
                        help='Path to hplus_FR_SS output dir (SS side: CR_SS)')
    parser.add_argument('--fake-rates', required=True,
                        help='Path to fake_rates/<era>/fake_rates.pkl dir')
    parser.add_argument('--electron-fr-zll', default=None,
                        help='[optional] Path to fit_ZLL.py output pkl (corrected_fr_EB/EE '
                             'graphs). If given, ELECTRON legs use this instead of '
                             'fake_rates.npz; muon legs are unaffected. If omitted, '
                             'behavior is unchanged from before (electrons also use '
                             'fake_rates.npz).')
    parser.add_argument('--year', default='all',
                        help="Era to process, or 'all'")
    args = parser.parse_args()

    print("Mass window: DISABLED (inclusive m4l, all CR_SS data used)")
    print("MC ratio: WEIGHTED (weight_nominal) sums, OS/SS, MC-only")

    electron_fr_graphs = None
    if args.electron_fr_zll is not None:
        electron_fr_graphs = load_electron_fr_graphs(args.electron_fr_zll)
        print(f"Electron FR source: {args.electron_fr_zll} "
              f"(corrected_fr_EB/EE from fit_ZLL.py)")
    else:
        print("Electron FR source: fake_rates.npz (default, no --electron-fr-zll given)")

    eras = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix'] \
           if args.year == 'all' else [args.year]

    all_results = []
    for era in eras:
        r = compute_year(era, args.cr_input_os, args.cr_input_ss, args.fake_rates,
                         electron_fr_graphs=electron_fr_graphs)
        if r is not None:
            all_results.append(r)

    if all_results:
        print_summary(all_results)


if __name__ == '__main__':
    main()