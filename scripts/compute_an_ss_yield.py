#!/usr/bin/env python3
"""
Compute Z+X background yield using AN-18-340 Eq. 11 (true SS method).

Formula (applied per-event over the CR_2P2F_SS DATA parquets):
    N_{Z+X} = (OS/SS)^MC * sum_{DATA, CR_2P2F_SS} f1 * f2

where:
    (OS/SS)^MC  = N_MC_reducible(CR_2P2F, OS) / N_MC_reducible(CR_2P2F_SS, SS)
                  Ratio of lumi-weighted reducible MC (DY+WZ+tt) in OS vs SS
                  2P2F CR.  Corrects for FSR and charge-asymmetric fake topologies.
    f1 * f2     = product of fake rates for the two SS failing Z2 leptons,
                  evaluated at their (pT, |eta|, flavour) from fake_rates.npz.
    DATA sum    = over CR_2P2F_SS events in the SR m4l window [100, 150] GeV.

Inputs:
    - hplusc_mva_4class_CR parquet files (new workflow including CR_2P2F_SS)
    - fake_rates/<era>/fake_rates.npz (from compute_fake_rates.py)

Output:
    - Printed yield table per era and final state

Usage:
    python compute_an_ss_yield.py \\
        --cr-input /eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_CR \\
        --fake-rates /eos/user/s/snandaku/higgscharm/outputs/fake_rates \\
        --year all

    # Disable m4l window (inclusive, for comparison):
    python compute_an_ss_yield.py ... --no-mass-window
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

# Default signal-region mass window
_M4L_MIN = 100.0   # GeV
_M4L_MAX = 150.0   # GeV


# ── Dataset classification ────────────────────────────────────────────────────

def _classify(folder):
    if any(kw in folder for kw in _DATA_KW):
        return 'data'
    if any(kw in folder for kw in _REDUCIBLE_KW):
        return 'reducible'
    if any(kw in folder for kw in _ZZ_KW):
        return 'zz'
    return 'other'


# ── Fake rate loading ─────────────────────────────────────────────────────────

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
    """Return (rate, err) for a single lepton."""
    flavor = 'electron' if abs(int(pdgid)) == 11 else 'muon'
    fr = fake_rates[flavor]
    pt_bin  = int(np.clip(np.searchsorted(fr['pt_edges'],  pt)  - 1, 0, len(fr['pt_edges'])  - 2))
    eta_bin = int(np.clip(np.searchsorted(fr['eta_edges'], abs(eta)) - 1, 0, len(fr['eta_edges']) - 2))
    rate = float(fr['fake_rate'][pt_bin, eta_bin])
    err  = float(fr['fake_rate_err'][pt_bin, eta_bin])
    if err >= 1.0:   # sentinel for undefined bins
        err = 0.0
    return rate, err


def get_fake_rate_array(pt_arr, eta_arr, pdgid_arr, fake_rates):
    """
    Vectorised fake rate lookup.
    Returns arrays (rate, err) same length as inputs.
    """
    rates = np.zeros(len(pt_arr))
    errs  = np.zeros(len(pt_arr))
    for i, (pt, eta, pdgid) in enumerate(zip(pt_arr, eta_arr, pdgid_arr)):
        rates[i], errs[i] = get_fake_rate(pt, eta, pdgid, fake_rates)
    return rates, errs


# ── Parquet loading helpers ───────────────────────────────────────────────────

def _load_parquets_for_category(cr_dir, year, category):
    """
    Walk cr_dir/<year>/<dataset>/<category>/ and load all parquet files.
    Returns a dict {'data': DataFrame, 'reducible': DataFrame, 'zz': DataFrame}.
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
    Columns stored as list-of-one-element; extract that element.
    Handles None entries.
    """
    def _get(val):
        if val is None:
            return np.nan
        if isinstance(val, (list, np.ndarray)):
            return float(val[0]) if len(val) > 0 else np.nan
        return float(val)
    return np.array([_get(v) for v in series])


def _apply_m4l_mask(df, col, m4l_min, m4l_max):
    """Return boolean mask for events in [m4l_min, m4l_max] GeV."""
    m4l = _extract_scalar(df[col])
    mask = np.isfinite(m4l)
    if m4l_min is not None:
        mask &= (m4l >= m4l_min)
    if m4l_max is not None:
        mask &= (m4l <= m4l_max)
    return mask


# ── (OS/SS)^MC ratio ─────────────────────────────────────────────────────────

def compute_os_ss_ratio(cr_dir, year, m4l_min, m4l_max):
    """
    Compute (OS/SS)^MC = N_lumi(reducible MC, CR_2P2F) / N_lumi(reducible MC, CR_2P2F_SS)

    Both are summed within the m4l window.  Lumi weights are weight_nominal from MC.
    If not available (data), raw event count is used (should not happen for MC).
    """
    # OS: reducible MC in CR_2P2F, m4l window on m4l_2p2f
    os_frames = _load_parquets_for_category(cr_dir, year, 'CR_2P2F')
    df_os = os_frames.get('reducible', pd.DataFrame())

    # SS: reducible MC in CR_2P2F_SS, m4l window on m4l_2p2f_ss
    ss_frames = _load_parquets_for_category(cr_dir, year, 'CR_2P2F_SS')
    df_ss = ss_frames.get('reducible', pd.DataFrame())

    n_os = 0.0
    n_ss = 0.0

    if not df_os.empty and 'm4l_2p2f' in df_os.columns:
        mask = _apply_m4l_mask(df_os, 'm4l_2p2f', m4l_min, m4l_max)
        df_os_sel = df_os[mask]
        if 'weight_nominal' in df_os_sel.columns:
            n_os = float(df_os_sel['weight_nominal'].sum())
        else:
            n_os = float(len(df_os_sel))
        print(f"    N_MC_OS  (CR_2P2F, reducible, m4l window): {n_os:.2f}")
    else:
        print("    WARNING: no reducible MC found for CR_2P2F")

    if not df_ss.empty and 'm4l_2p2f_ss' in df_ss.columns:
        mask = _apply_m4l_mask(df_ss, 'm4l_2p2f_ss', m4l_min, m4l_max)
        df_ss_sel = df_ss[mask]
        if 'weight_nominal' in df_ss_sel.columns:
            n_ss = float(df_ss_sel['weight_nominal'].sum())
        else:
            n_ss = float(len(df_ss_sel))
        print(f"    N_MC_SS  (CR_2P2F_SS, reducible, m4l window): {n_ss:.2f}")
    else:
        print("    WARNING: no reducible MC found for CR_2P2F_SS")

    if n_ss <= 0:
        print("    WARNING: N_MC_SS = 0 — cannot compute ratio. Defaulting to 1.0")
        return 1.0, n_os, n_ss

    ratio = n_os / n_ss
    print(f"    (OS/SS)^MC = {n_os:.3f} / {n_ss:.3f} = {ratio:.4f}")
    return ratio, n_os, n_ss


# ── DATA SS weighted sum ──────────────────────────────────────────────────────

def compute_data_ss_weighted(df_data, fake_rates, m4l_min, m4l_max):
    """
    Compute sum_{DATA CR_2P2F_SS} f1*f2 from parquet DataFrame.

    Returns (weighted_sum, stat_err, n_events_after_window)
    where stat_err = sqrt(sum_i (f1_i * f2_i)^2) (CR stat uncertainty).
    """
    if df_data.empty:
        print("    WARNING: no DATA events in CR_2P2F_SS")
        return 0.0, 0.0, 0

    # Apply m4l window
    mask = _apply_m4l_mask(df_data, 'm4l_2p2f_ss', m4l_min, m4l_max)
    df = df_data[mask].reset_index(drop=True)
    n_events = len(df)
    print(f"    DATA events in CR_2P2F_SS after m4l window: {n_events}")

    if n_events == 0:
        return 0.0, 0.0, 0

    # Extract lepton kinematics (single-element lists → scalars)
    pt3    = _extract_scalar(df['l3_pt_2p2f_ss'])
    eta3   = _extract_scalar(df['l3_eta_2p2f_ss'])
    pdg3   = _extract_scalar(df['l3_pdgid_2p2f_ss'])
    pt4    = _extract_scalar(df['l4_pt_2p2f_ss'])
    eta4   = _extract_scalar(df['l4_eta_2p2f_ss'])
    pdg4   = _extract_scalar(df['l4_pdgid_2p2f_ss'])

    # Fake rates per lepton
    f1, f1_err = get_fake_rate_array(pt3, eta3, pdg3, fake_rates)
    f2, f2_err = get_fake_rate_array(pt4, eta4, pdg4, fake_rates)

    weights  = f1 * f2
    weighted_sum = float(np.sum(weights))
    stat_err     = float(np.sqrt(np.sum(weights**2)))   # CR stat

    return weighted_sum, stat_err, n_events


# ── Z2 flavor breakdown ───────────────────────────────────────────────────────

def compute_z2_flavor_breakdown(df_data, fake_rates, m4l_min, m4l_max):
    """
    Breakdown by Z2 lepton flavor (|pdgId| of l3_pdgid_2p2f_ss).
    Returns {'electron': (yield_sum, err), 'muon': (yield_sum, err)}.
    """
    if df_data.empty:
        return {}

    mask = _apply_m4l_mask(df_data, 'm4l_2p2f_ss', m4l_min, m4l_max)
    df = df_data[mask].reset_index(drop=True)
    if len(df) == 0:
        return {}

    pdg3 = np.abs(_extract_scalar(df['l3_pdgid_2p2f_ss'])).astype(int)
    pt3  = _extract_scalar(df['l3_pt_2p2f_ss'])
    eta3 = _extract_scalar(df['l3_eta_2p2f_ss'])
    pt4  = _extract_scalar(df['l4_pt_2p2f_ss'])
    eta4 = _extract_scalar(df['l4_eta_2p2f_ss'])
    pdg4 = np.abs(_extract_scalar(df['l4_pdgid_2p2f_ss'])).astype(int)

    result = {}
    for flav, pdgid_val in [('electron', 11), ('muon', 13)]:
        sel = (pdg3 == pdgid_val)   # SS+SF: both l3 and l4 have same flavor
        if not np.any(sel):
            result[flav] = (0.0, 0.0)
            continue
        f1, _ = get_fake_rate_array(pt3[sel], eta3[sel], pdg3[sel], fake_rates)
        f2, _ = get_fake_rate_array(pt4[sel], eta4[sel], pdg4[sel], fake_rates)
        w = f1 * f2
        result[flav] = (float(np.sum(w)), float(np.sqrt(np.sum(w**2))))

    return result


# ── Per-era calculation ───────────────────────────────────────────────────────

def compute_year(year, cr_input, fr_dir, m4l_min, m4l_max):
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

    # ── (OS/SS)^MC ratio ────────────────────────────────────────────────────
    print(f"\n  Computing (OS/SS)^MC ratio...")
    mw_str = f"[{m4l_min},{m4l_max}] GeV" if m4l_min is not None else "inclusive"
    print(f"  m4l window: {mw_str}")
    os_ss_ratio, n_mc_os, n_mc_ss = compute_os_ss_ratio(cr_input, year, m4l_min, m4l_max)

    # ── DATA SS events ──────────────────────────────────────────────────────
    print(f"\n  Loading DATA CR_2P2F_SS parquets...")
    ss_frames = _load_parquets_for_category(cr_input, year, 'CR_2P2F_SS')
    df_data   = ss_frames.get('data', pd.DataFrame())

    if df_data.empty:
        print(f"  No DATA events in CR_2P2F_SS for {year} — skipping.")
        return None

    # ── DATA SS f1*f2 weighted sum ──────────────────────────────────────────
    print(f"\n  Computing DATA SS f1*f2 sum...")
    data_weighted, data_stat_err, n_data_ss = compute_data_ss_weighted(
        df_data, fake_rates, m4l_min, m4l_max
    )

    # ── Z2 flavor breakdown ─────────────────────────────────────────────────
    print(f"\n  Computing Z2 flavor breakdown...")
    z2_breakdown = compute_z2_flavor_breakdown(df_data, fake_rates, m4l_min, m4l_max)

    # ── Final yield ─────────────────────────────────────────────────────────
    yield_total = os_ss_ratio * data_weighted
    yield_err   = os_ss_ratio * data_stat_err

    # ── Print results ───────────────────────────────────────────────────────
    print(f"\n  {'─'*60}")
    print(f"  AN Eq. 11 SS Yield — {year}")
    print(f"  {'─'*60}")
    print(f"  N_DATA_SS (m4l window):          {n_data_ss:>8d}")
    print(f"  N_MC_OS   (CR_2P2F, weighted):   {n_mc_os:>10.3f}")
    print(f"  N_MC_SS   (CR_2P2F_SS, weighted):{n_mc_ss:>10.3f}")
    print(f"  (OS/SS)^MC:                      {os_ss_ratio:>10.4f}")
    print(f"  sum_DATA f1*f2:                  {data_weighted:>10.4f}  ± {data_stat_err:.4f}")
    print(f"  Net Z+X yield (Eq. 11):          {yield_total:>10.4f}  ± {yield_err:.4f}")

    if z2_breakdown:
        print(f"\n  Z2-flavor breakdown:")
        print(f"  {'Z2 flavor':<14}  {'f1*f2 sum':>10}  {'Scaled yield':>14}")
        print(f"  {'─'*44}")
        for flav, (y, e) in z2_breakdown.items():
            print(f"  {flav:<14}  {y:>10.4f}  {os_ss_ratio*y:>14.4f}  ± {os_ss_ratio*e:.4f}")
    print(f"  {'─'*60}")

    return {
        'year':          year,
        'n_data_ss':     n_data_ss,
        'n_mc_os':       n_mc_os,
        'n_mc_ss':       n_mc_ss,
        'os_ss_ratio':   os_ss_ratio,
        'data_weighted': data_weighted,
        'data_stat_err': data_stat_err,
        'yield_total':   yield_total,
        'yield_err':     yield_err,
        'z2_breakdown':  z2_breakdown,
    }


# ── Summary table ─────────────────────────────────────────────────────────────

def print_summary(results, m4l_min, m4l_max):
    mw_str = f"[{m4l_min},{m4l_max}] GeV" if m4l_min is not None else "inclusive"
    print(f"\n{'='*64}")
    print(f"  AN-18-340 Eq. 11 SS Method — Z+X Yield Summary")
    print(f"  m4l window: {mw_str}")
    print(f"{'='*64}")
    print(f"  {'Era':<16}  {'N_DATA_SS':>10}  {'(OS/SS)^MC':>12}  {'Net Z+X':>12}")
    print(f"  {'─'*56}")
    total_yield = 0.0
    total_err2  = 0.0
    for r in results:
        print(f"  {r['year']:<16}  {r['n_data_ss']:>10d}  {r['os_ss_ratio']:>12.4f}  "
              f"{r['yield_total']:>8.2f} ± {r['yield_err']:.2f}")
        total_yield += r['yield_total']
        total_err2  += r['yield_err'] ** 2
    total_err = float(np.sqrt(total_err2))
    print(f"  {'─'*56}")
    print(f"  {'Total':<16}  {'':>10}  {'':>12}  "
          f"{total_yield:>8.2f} ± {total_err:.2f}")
    print(f"{'='*64}")

    print(f"\n  Comparison:")
    print(f"  {'Method':<30}  {'Total yield':>12}")
    print(f"  {'─'*44}")
    print(f"  {'AN Eq. 11 (SS, this script)':<30}  {total_yield:>8.2f} ± {total_err:.2f}")
    print(f"  {'Eq. 10 + SS FR (estimate_zx)':<30}  {'48.1':>8} ± {'3.4'}")
    print(f"  {'Eq. 10 + OS FR (WZ-only)':<30}  {'46.1':>8} ± {'3.2'}")
    print(f"  {'UAntwerp (OS + WZ+ZZ FR)':<30}  {'121.2':>8} ± {'10.8'}")
    print(f"  {'─'*44}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compute Z+X yield using AN-18-340 Eq. 11 (true SS method)'
    )
    parser.add_argument('--cr-input', required=True,
                        help='Path to hplusc_mva_4class_CR output dir')
    parser.add_argument('--fake-rates', required=True,
                        help='Path to fake_rates/<era>/fake_rates.pkl dir')
    parser.add_argument('--year', default='all',
                        help="Era to process, or 'all'")
    parser.add_argument('--no-mass-window', action='store_true',
                        help='Disable m4l window (inclusive)')
    parser.add_argument('--mass-window', type=float, nargs=2, default=None,
                        metavar=('MIN', 'MAX'),
                        help='Override mass window in GeV (default: 100 150)')
    args = parser.parse_args()

    if args.no_mass_window:
        m4l_min, m4l_max = None, None
        print("Mass window: DISABLED (inclusive m4l)")
    elif args.mass_window is not None:
        m4l_min, m4l_max = args.mass_window
        print(f"Mass window: [{m4l_min}, {m4l_max}] GeV")
    else:
        m4l_min, m4l_max = _M4L_MIN, _M4L_MAX
        print(f"Mass window: [{m4l_min}, {m4l_max}] GeV (default)")

    eras = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix'] \
           if args.year == 'all' else [args.year]

    all_results = []
    for era in eras:
        r = compute_year(era, args.cr_input, args.fake_rates, m4l_min, m4l_max)
        if r is not None:
            all_results.append(r)

    if all_results:
        print_summary(all_results, m4l_min, m4l_max)


if __name__ == '__main__':
    main()
