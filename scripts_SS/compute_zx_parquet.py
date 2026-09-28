#!/usr/bin/env python3
"""
compute_zx_parquets.py

Compute the OS-method (opposite-sign) Z+X reducible background estimate from
the hplus_FR_full / hplus_FR_full_merged workflow output, save the per-event
weighted 3P1F and 2P2F parquet files, and print the yield table.

Formula (OS method, UAntwerp):
  N_bkg_SR = (1 - N_ZZ_3P1F / N_3P1F) * sum_3P1F[ f/(1-f) ]
             - sum_2P2F[ f1*f2 / ((1-f1)*(1-f2)) ]

ERROR PROPAGATION (matches ZZAnalysis OSmethod.C::PrintZXYields):
  stat      = sqrt(Sum w_i^2)
  syst      = net_zx * ((net_zx_up / net_zx) - 1)   (fake-rate Up variation)
  syst_comp = net_zx * 0.30                          (flat 30% comp. systematic)
  comb      = sqrt(stat^2 + syst^2 + syst_comp^2)

Output: for each era, writes
    <output>/<era>/zx_3p1f_<era>.parquet
    <output>/<era>/zx_2p2f_<era>.parquet
These are the exact input files expected by run_mva_inference.py.

Usage:
    python compute_zx_parquets.py \\
        --merged-input /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_full_merged \\
        --cr-input     /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_full \\
        --sr-base      /eos/user/p/pkatris/higgscharm/outputs/hplusc \\
        --fake-rates   /eos/user/p/pkatris/higgscharm/outputs/fake_rates \\
        --electron-fr-zll /path/to/fit_ZLL_2022preEE.pkl \\
        --year all \\
        --output /eos/user/p/pkatris/Analysis/zx_background
"""

import os
import glob
import argparse
import pickle
import numpy as np
import pandas as pd
import awkward as ak

# ============================================================================
# Constants / defaults
# ============================================================================

MERGED_BASE_DEFAULT = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR_full_merged"
CR_BASE_DEFAULT      = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR_full"
SR_BASE_DEFAULT       = "/eos/user/p/pkatris/higgscharm/outputs/hplusc"
HC_PATH_DEFAULT        = "/afs/cern.ch/user/p/pkatris/higgscharm"

_FILESET_YAML = {
    '2022preEE':    '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2022preEE_nanov12.yaml',
    '2022postEE':   '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2022postEE_nanov12.yaml',
    '2023preBPix':  '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2023preBPix_nanov12.yaml',
    '2023postBPix': '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2023postBPix_nanov12.yaml',
}
_LUMI_FB = {'2022preEE': 7.98, '2022postEE': 26.67, '2023preBPix': 17.79, '2023postBPix': 9.53}
BKG_COMPOSITION_SYST = 0.30
_FINAL_STATES = ['4e', '4mu', '2e2mu', '2mu2e']
_ELECTRON_BARREL_ETA_CUT = 1.479

# ----------------------------------------------------------------------------
# qqZZ sample naming.
#
# The qqZZ->4l sample is split across a base production and one or more
# "extension" campaigns, e.g. on-disk folders:
#     <sr_base>/<year>/ZZ4l/*.coffea
#     <sr_base>/<year>/ZZ4l_ext1/*.coffea
#     <cr_base>/<year>/ZZ4l/CR_3P1F/*.parquet
#     <cr_base>/<year>/ZZ4l_ext1/CR_3P1F/*.parquet
# and a single entry in the fileset yaml (same xsec for base + extensions).
#
# _get_sumw_from_coffea / _sum_wnominal_from_cr glob on "<prefix>*", which
# already matches the base folder itself (zero extra chars) *and* every
# "<prefix>_ext*" folder, summing their sumw / raw weighted yields together
# before a single normalization factor is applied -- mirroring how
# get_lumi_weight() in the ranker-training script sums sumw across every
# .coffea file for a sample before computing one lumi_weight.
#
# QQZZ_FOLDER_PREFIX must match the on-disk directory prefix; QQZZ_YAML_KEY
# must match the key used for this sample in the fileset yaml. They are
# usually the same string but are kept separate in case the yaml uses a
# different naming convention than the output directories.
# ----------------------------------------------------------------------------
QQZZ_FOLDER_PREFIX = "ZZto4L"
QQZZ_YAML_KEY       = "ZZto4L"


# ============================================================================
# Fake rate loading + lookup
# ============================================================================

def load_fake_rates(path):
    npz_path = path.replace('.pkl', '.npz')
    if os.path.exists(npz_path):
        raw = np.load(npz_path, allow_pickle=False)
        fr = {}
        for key in raw.files:
            parts = key.split('__')
            flavor, subkey, field = parts[0], parts[1], parts[2]
            fr.setdefault(flavor, {})
            if subkey == 'top':
                fr[flavor][field] = raw[key]
            else:
                fr[flavor].setdefault(subkey, {})
                fr[flavor][subkey][field] = raw[key]
        return fr
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_electron_fr_graphs(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def graph_lookup(graph, x_query):
    x, y, ey = graph['x'], graph['y'], graph['ey']
    if len(x) == 0:
        return 0.0, 0.0
    above = np.nonzero(x_query < x)[0]
    idx = int(above[0]) if len(above) else len(x) - 1
    return float(np.clip(y[idx], 0.001, 0.999)), float(ey[idx])


def get_fake_rate(pt, eta, pdgid, fake_rates, electron_fr_graphs=None):
    """Return (rate, err). Muons always from fake_rates.npz; electrons use
    electron_fr_graphs if given, else also fake_rates.npz."""
    is_electron = abs(int(pdgid)) == 11
    if is_electron and electron_fr_graphs is not None:
        graph = (electron_fr_graphs['corrected_fr_EB'] if abs(eta) < _ELECTRON_BARREL_ETA_CUT
                 else electron_fr_graphs['corrected_fr_EE'])
        return graph_lookup(graph, pt)

    flavor = 'electron' if is_electron else 'muon'
    fr = fake_rates[flavor]
    pt_bin  = int(np.clip(np.searchsorted(fr['pt_edges'],  pt)  - 1, 0, len(fr['pt_edges'])  - 2))
    eta_bin = int(np.clip(np.searchsorted(fr['eta_edges'], abs(eta)) - 1, 0, len(fr['eta_edges']) - 2))
    rate = float(fr['fake_rate'][pt_bin, eta_bin])
    err  = float(fr['fake_rate_err'][pt_bin, eta_bin])
    if err >= 1.0:
        err = 0.0
    return rate, err


def apply_variant(rate, err, variant):
    if variant == 'up':
        return min(rate + err, 0.999999)
    if variant == 'dn':
        return max(rate - err, 0.0)
    return rate


def compute_weights_3p1f(df, fake_rates, variant='nominal', electron_fr_graphs=None):
    w = np.zeros(len(df))
    for i in range(len(df)):
        if not df['l3_istight_3p1f'].iloc[i]:
            rate, err = get_fake_rate(df['l3_pt_3p1f'].iloc[i], df['l3_eta_3p1f'].iloc[i],
                                      df['l3_pdgid_3p1f'].iloc[i], fake_rates, electron_fr_graphs)
        elif not df['l4_istight_3p1f'].iloc[i]:
            rate, err = get_fake_rate(df['l4_pt_3p1f'].iloc[i], df['l4_eta_3p1f'].iloc[i],
                                      df['l4_pdgid_3p1f'].iloc[i], fake_rates, electron_fr_graphs)
        else:
            rate, err = 0.0, 0.0
        f = apply_variant(rate, err, variant)
        w[i] = f / (1.0 - f) if f < 1.0 else 0.0
    return w


def compute_weights_2p2f(df, fake_rates, variant='nominal', electron_fr_graphs=None):
    w = np.zeros(len(df))
    for i in range(len(df)):
        r1, e1 = get_fake_rate(df['l3_pt_2p2f'].iloc[i], df['l3_eta_2p2f'].iloc[i],
                               df['l3_pdgid_2p2f'].iloc[i], fake_rates, electron_fr_graphs)
        r2, e2 = get_fake_rate(df['l4_pt_2p2f'].iloc[i], df['l4_eta_2p2f'].iloc[i],
                               df['l4_pdgid_2p2f'].iloc[i], fake_rates, electron_fr_graphs)
        f1 = apply_variant(r1, e1, variant)
        f2 = apply_variant(r2, e2, variant)
        w[i] = (f1 * f2) / ((1.0 - f1) * (1.0 - f2)) if f1 < 1.0 and f2 < 1.0 else 0.0
    return w


def get_final_state(z1_pdgid, z2_pdgid):
    mapping = {(13, 13): '4mu', (11, 11): '4e', (11, 13): '2e2mu', (13, 11): '2mu2e'}
    return mapping.get((int(z1_pdgid), int(z2_pdgid)), 'unknown')


# ============================================================================
# Merged-parquet loading (DATA-only, SR mass window applied)
# ============================================================================

def _load_flat(arr, fields):
    n_rows = len(arr)
    flat = {}
    for f in fields:
        if f not in arr.fields:
            continue
        col = arr[f]
        if col.ndim == 1:
            flat[f] = ak.to_numpy(ak.fill_none(col, np.nan))
        else:
            try:
                flat_col = ak.to_numpy(ak.flatten(ak.fill_none(col, np.nan), axis=1))
            except Exception:
                flat_col = None
            if flat_col is not None and len(flat_col) == n_rows:
                flat[f] = flat_col
            else:
                padded = ak.pad_none(col, 1, axis=1)
                flat[f] = ak.to_numpy(ak.fill_none(padded[:, 0], np.nan))
                padded3 = ak.pad_none(col, 3, axis=1)
                for j in range(3):
                    flat[f'{f}_{j}'] = ak.to_numpy(ak.fill_none(padded3[:, j], np.nan))
    return flat


def load_cr_data_merged(merged_base, year, category, sr_m4l_min, sr_m4l_max):
    merged = os.path.join(merged_base, year, f"{category}.parquet")
    if not os.path.exists(merged):
        print(f"  Merged file not found: {merged}")
        return pd.DataFrame()

    arr = ak.from_parquet(merged)
    other_suffix = "_2p2f" if category == "CR_3P1F" else "_3p1f"
    needed = [f for f in arr.fields if not f.endswith(other_suffix)]
    flat = _load_flat(arr, needed)
    df = pd.DataFrame(flat)
    print(f"  Loaded {len(df):,} events for {category} (before SR cuts)")

    m4l_col = "m4l_3p1f" if category == "CR_3P1F" else "m4l_2p2f"
    before = len(df)
    if sr_m4l_min is not None and sr_m4l_max is not None:
        mask = (df[m4l_col] >= sr_m4l_min) & (df[m4l_col] <= sr_m4l_max)
        df = df[mask].reset_index(drop=True)
        print(f"  After m4l [{sr_m4l_min},{sr_m4l_max}] GeV: {len(df):,} events ({len(df)/before*100:.1f}%)")
    else:
        print("  m4l window: NONE (inclusive)")

    if "weight_nominal" in df.columns:
        before = len(df)
        df = df[np.isnan(df["weight_nominal"])].reset_index(drop=True)
        print(f"  After DATA-only filter:         {len(df):,} events ({len(df)/before*100:.1f}% were DATA)")
    return df


# ============================================================================
# ZZ correction (reads raw per-dataset parquets + SR coffea metadata)
# ============================================================================

def _load_xsec(year, sample_key):
    import yaml
    ypath = _FILESET_YAML.get(year, '')
    if not os.path.exists(ypath):
        return 0.0
    with open(ypath) as f:
        fs = yaml.safe_load(f)
    return float(fs.get(sample_key, {}).get('xsec', 0))


def _get_sumw_from_coffea(sr_base, year, dataset_prefix, hc_path):
    try:
        import sys
        if hc_path not in sys.path:
            sys.path.insert(0, hc_path)
        from coffea.util import load as coffea_load
    except ImportError:
        return 0.0
    total = 0.0
    matched_dirs = sorted(glob.glob(os.path.join(sr_base, year, f"{dataset_prefix}*")))
    if not matched_dirs:
        print(f"    [_get_sumw_from_coffea] no folders matched "
              f"'{dataset_prefix}*' under {os.path.join(sr_base, year)}")
    for d in matched_dirs:
        n_files = 0
        for cf in glob.glob(os.path.join(d, "*.coffea")):
            try:
                out = coffea_load(cf)
                if isinstance(out, dict) and 'metadata' in out:
                    total += float(out['metadata'].get('sumw', 0))
                    n_files += 1
            except Exception:
                pass
        print(f"    [_get_sumw_from_coffea] {os.path.basename(d)}: {n_files} .coffea file(s)")
    return total


def _sum_wnominal_from_cr(cr_base, year, dataset_prefix, category, m4l_min=None, m4l_max=None):
    cr_dir  = os.path.join(cr_base, year)
    m4l_col = 'm4l_3p1f' if 'CR_3P1F' in category else 'm4l_2p2f'
    total   = 0.0
    if not os.path.isdir(cr_dir):
        return total
    matched_folders = [d for d in sorted(os.listdir(cr_dir)) if d.startswith(dataset_prefix)]
    if not matched_folders:
        print(f"    [_sum_wnominal_from_cr] no folders matched "
              f"'{dataset_prefix}*' under {cr_dir}")
    for dataset_folder in matched_folders:
        for pfile in glob.glob(os.path.join(cr_dir, dataset_folder, category, "*.parquet")):
            try:
                arr = ak.from_parquet(pfile)
                if 'weight_nominal' not in arr.fields:
                    continue
                w = arr['weight_nominal']
                w_np = ak.to_numpy(ak.fill_none(ak.flatten(w, axis=1) if w.ndim > 1 else w, np.nan))
                mask = ~np.isnan(w_np)
                if m4l_min is not None and m4l_col in arr.fields:
                    m4l = arr[m4l_col]
                    m4l_np = ak.to_numpy(ak.fill_none(ak.flatten(m4l, axis=1) if m4l.ndim > 1 else m4l, np.nan))
                    mask = mask & (m4l_np >= m4l_min) & (m4l_np <= m4l_max)
                total += float(w_np[mask].sum())
            except Exception as e:
                print(f"    Warning reading {pfile}: {e}")
    return total


def compute_zz_correction(cr_base, sr_base, hc_path, year, category, df_data):
    lumi = _LUMI_FB.get(year, 0)
    if lumi == 0:
        print(f"  Unknown era {year}, skipping ZZ correction")
        return 1.0, 0, 0.0

    # qqZZ: base folder ("ZZ4l") + extension folders ("ZZ4l_ext1", ...) are
    # aggregated automatically because _get_sumw_from_coffea / 
    # _sum_wnominal_from_cr glob on "<prefix>*", which matches the base
    # folder itself as well as every extension. One xsec/normalization is
    # applied to the combined sumw + combined raw weighted yield, matching
    # the way get_lumi_weight() in the ranker-training script sums sumw
    # across every .coffea file for a sample before deriving one lumi_weight.
    xs_qq   = _load_xsec(year, QQZZ_YAML_KEY)
    # sumw metadata lives alongside the CR parquet output (hplus_FR_full),
    # not under the SR coffea output (hplusc) -- so look in cr_base here.
    sumw_qq = _get_sumw_from_coffea(cr_base, year, QQZZ_FOLDER_PREFIX, hc_path)
    if sumw_qq > 0 and xs_qq > 0:
        xs_norm_qq = xs_qq * 1000 * lumi / sumw_qq
        raw_qq     = _sum_wnominal_from_cr(cr_base, year, QQZZ_FOLDER_PREFIX, category)
        n_zz_qq    = raw_qq * xs_norm_qq
    else:
        n_zz_qq = 0.0
        print(f"  Warning: could not compute qqZZ correction for {year} "
              f"(yaml_key='{QQZZ_YAML_KEY}' xs={xs_qq}, folder_prefix='{QQZZ_FOLDER_PREFIX}' sumw={sumw_qq})")

    n_zz_gg = 0.0
    for sample_key, folder_prefix in [
        ('GluGlutoContinto2Zto4E',    'GluGlutoContinto2Zto4E'),
        ('GluGlutoContinto2Zto4Mu',   'GluGlutoContinto2Zto4Mu'),
        ('GluGluToContinto2Zto2E2Mu', 'GluGluToContinto2Zto2E2Mu'),
    ]:
        xs_gg   = _load_xsec(year, sample_key)
        sumw_gg = _get_sumw_from_coffea(cr_base, year, folder_prefix, hc_path)
        if sumw_gg > 0 and xs_gg > 0:
            xs_norm  = xs_gg * 1000 * lumi / sumw_gg
            raw      = _sum_wnominal_from_cr(cr_base, year, folder_prefix, category)
            n_zz_gg += raw * xs_norm

    n_zz_mc = n_zz_qq + n_zz_gg
    n_data = len(df_data) if df_data is not None else 0
    if n_data == 0:
        print(f"  Warning: no DATA events found in {category} for {year}")
        return 1.0, 0, 0.0

    correction = 1.0 - n_zz_mc / n_data
    print(f"  ZZ correction ({category}): N_data={n_data:,}  N_qqZZ={n_zz_qq:.2f}  "
          f"N_ggZZ={n_zz_gg:.2f}  N_ZZ_total={n_zz_mc:.2f}  factor={correction:.4f}")
    return correction, n_data, n_zz_mc


# ============================================================================
# Per-era OS-method calculation
# ============================================================================

def estimate_zx_background(year, merged_base, cr_base, sr_base, hc_path, fake_rates,
                            sr_m4l_min, sr_m4l_max, output_dir, electron_fr_graphs=None):
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n{'='*60}\n  Year: {year}\n{'='*60}")
    print("\nLoading 3P1F events (merged, DATA-only)...")
    df_3p1f = load_cr_data_merged(merged_base, year, "CR_3P1F", sr_m4l_min, sr_m4l_max)
    print("\nLoading 2P2F events (merged, DATA-only)...")
    df_2p2f = load_cr_data_merged(merged_base, year, "CR_2P2F", sr_m4l_min, sr_m4l_max)

    if len(df_3p1f) == 0:
        print("No 3P1F events — skipping.")
        return None

    print("\nComputing ZZ correction factor...")
    zz_correction, n_3p1f_data, n_zz_mc = compute_zz_correction(
        cr_base, sr_base, hc_path, year, "CR_3P1F", df_3p1f
    )

    print("\nComputing fake rate weights (nominal + Up variation)...")
    w3    = compute_weights_3p1f(df_3p1f, fake_rates, 'nominal', electron_fr_graphs)
    w3_up = compute_weights_3p1f(df_3p1f, fake_rates, 'up', electron_fr_graphs)
    w2    = compute_weights_2p2f(df_2p2f, fake_rates, 'nominal', electron_fr_graphs) if len(df_2p2f) > 0 else np.array([])
    w2_up = compute_weights_2p2f(df_2p2f, fake_rates, 'up', electron_fr_graphs)      if len(df_2p2f) > 0 else np.array([])

    cw3, cw3_up = zz_correction * w3, zz_correction * w3_up
    df_3p1f['zx_weight']    = cw3
    df_3p1f['zx_weight_sq'] = cw3 ** 2
    df_3p1f['zx_weight_up'] = cw3_up
    df_3p1f['zx_sign']      = 1
    df_2p2f['zx_weight']    = -w2
    df_2p2f['zx_weight_sq'] = w2 ** 2
    df_2p2f['zx_weight_up'] = -w2_up
    df_2p2f['zx_sign']      = -1

    yield_3p1f_raw       = float(np.sum(w3))
    yield_3p1f_corrected = zz_correction * yield_3p1f_raw
    yield_2p2f            = float(np.sum(w2))
    net_zx                = yield_3p1f_corrected - yield_2p2f

    yield_3p1f_corrected_up = zz_correction * float(np.sum(w3_up))
    yield_2p2f_up            = float(np.sum(w2_up))
    net_zx_up                = yield_3p1f_corrected_up - yield_2p2f_up

    stat_net = float(np.sqrt(np.sum(cw3 ** 2) + np.sum(w2 ** 2)))
    syst_net = net_zx * ((net_zx_up / net_zx) - 1.0) if net_zx != 0 else 0.0
    syst_comp_net = net_zx * BKG_COMPOSITION_SYST
    comb_net = float(np.sqrt(stat_net ** 2 + syst_net ** 2 + syst_comp_net ** 2))

    has_z1_pdgid = 'z1_l1_pdgid_3p1f' in df_3p1f.columns
    fs_yields    = {fs: 0.0 for fs in _FINAL_STATES}
    fs_yields_up = {fs: 0.0 for fs in _FINAL_STATES}
    fs_stat      = {fs: 0.0 for fs in _FINAL_STATES}
    fs_syst      = {fs: 0.0 for fs in _FINAL_STATES}
    fs_syst_comp = {fs: 0.0 for fs in _FINAL_STATES}
    fs_comb      = {fs: 0.0 for fs in _FINAL_STATES}

    if has_z1_pdgid:
        df_3p1f['final_state'] = [get_final_state(z1, z2) for z1, z2 in
                                  zip(df_3p1f['z1_l1_pdgid_3p1f'].values, df_3p1f['l3_pdgid_3p1f'].values)]
        for fs in fs_yields:
            mask = df_3p1f['final_state'] == fs
            fs_yields[fs]    += float(np.sum(df_3p1f.loc[mask, 'zx_weight']))
            fs_yields_up[fs] += float(np.sum(df_3p1f.loc[mask, 'zx_weight_up']))
            fs_stat[fs]       = float(np.sqrt(np.sum(df_3p1f.loc[mask, 'zx_weight_sq'])))

        if 'z1_l1_pdgid_2p2f' in df_2p2f.columns and len(df_2p2f) > 0:
            df_2p2f['final_state'] = [get_final_state(z1, z2) for z1, z2 in
                                      zip(df_2p2f['z1_l1_pdgid_2p2f'].values, df_2p2f['l3_pdgid_2p2f'].values)]
            for fs in fs_yields:
                mask = df_2p2f['final_state'] == fs
                fs_yields[fs]    += float(np.sum(df_2p2f.loc[mask, 'zx_weight']))
                fs_yields_up[fs] += float(np.sum(df_2p2f.loc[mask, 'zx_weight_up']))
                fs_stat[fs] = float(np.sqrt(fs_stat[fs] ** 2 + np.sum(df_2p2f.loc[mask, 'zx_weight_sq'])))

        for fs in fs_yields:
            y, y_up = fs_yields[fs], fs_yields_up[fs]
            fs_syst[fs]      = y * ((y_up / y) - 1.0) if y != 0 else 0.0
            fs_syst_comp[fs] = y * BKG_COMPOSITION_SYST
            fs_comb[fs]      = float(np.sqrt(fs_stat[fs] ** 2 + fs_syst[fs] ** 2 + fs_syst_comp[fs] ** 2))
    else:
        print("  Note: z1_l1_pdgid not available — rerun CR jobs for final-state breakdown.")

    # ── Save output parquets (input files for run_mva_inference.py) ────────
    df_3p1f['m4l'] = df_3p1f['m4l_3p1f']
    df_3p1f.to_parquet(os.path.join(output_dir, f'zx_3p1f_{year}.parquet'))
    if len(df_2p2f) > 0:
        df_2p2f['m4l'] = df_2p2f['m4l_2p2f']
        df_2p2f.to_parquet(os.path.join(output_dir, f'zx_2p2f_{year}.parquet'))

    print(f"\n{'='*60}\n  Z+X Yield Summary — {year}\n{'='*60}")
    mw_str = f"[{sr_m4l_min},{sr_m4l_max}] GeV" if sr_m4l_min is not None else "inclusive"
    print(f"  m4l window:                  {mw_str:>10}")
    print(f"  N_3P1F (DATA events):        {n_3p1f_data:>10,}")
    print(f"  N_ZZ_MC in 3P1F:             {n_zz_mc:>10.2f}")
    print(f"  ZZ correction factor:        {zz_correction:>10.4f}")
    print(f"  Sum 3P1F [corrected]:        {yield_3p1f_corrected:>10.2f}")
    print(f"  Sum 2P2F [subtracted]:       {yield_2p2f:>10.2f}")
    print(f"  Net Z+X yield:               {net_zx:>10.2f}")
    print("  ── error breakdown ──")
    print(f"  stat (CR, sqrt(Sum w^2)):    {stat_net:>10.4f}")
    print(f"  syst (fake-rate Up var.):    {syst_net:>10.4f}")
    print(f"  syst_comp (flat 30%):        {syst_comp_net:>10.4f}")
    print(f"  >>> comb (total):            {comb_net:>10.4f}  <<<")
    if has_z1_pdgid:
        print("\n  Breakdown by final state:")
        print(f"  {'Final state':<10}  {'Yield':>8}  {'stat':>8}  {'syst':>8}  {'syst_comp':>9}  {'comb':>8}")
        print("  " + "-" * 62)
        for fs in _FINAL_STATES:
            print(f"  {fs:<10}  {fs_yields[fs]:>8.2f}  {fs_stat[fs]:>8.2f}  "
                  f"{fs_syst[fs]:>8.2f}  {fs_syst_comp[fs]:>9.2f}  {fs_comb[fs]:>8.2f}")
    print(f"{'='*60}\n")

    return {
        'year': year, 'n_3p1f_data': n_3p1f_data, 'n_zz_mc': n_zz_mc,
        'zz_correction': zz_correction, 'yield_3p1f_corrected': yield_3p1f_corrected,
        'yield_2p2f': yield_2p2f, 'net_zx': net_zx, 'stat_net': stat_net,
        'syst_net': syst_net, 'syst_comp_net': syst_comp_net, 'comb_net': comb_net,
        'fs_yields': fs_yields, 'fs_comb': fs_comb, 'has_fs': has_z1_pdgid,
    }


def print_era_table(results):
    has_fs = any(r['has_fs'] for r in results)
    print("\n" + "=" * 70)
    print("  Z+X Background Yield — OS Method (opposite-sign)")
    print("  errors: comb = sqrt(stat^2 + syst^2 + syst_comp^2)")
    print("=" * 70)

    if has_fs:
        col = 18
        print(f"  {'Era':<16}  {'4mu':>{col}}  {'4e':>{col}}  {'2e2mu':>{col}}  {'2mu2e':>{col}}  {'Total':>12}")
        print("  " + "-" * 90)

        def fmt(y, e):
            return f"{y:.1f} ± {e:.1f}"

        for r in results:
            fs, comb = r['fs_yields'], r['fs_comb']
            print(f"  {r['year']:<16}  {fmt(fs['4mu'], comb['4mu']):>{col}}  {fmt(fs['4e'], comb['4e']):>{col}}  "
                  f"{fmt(fs['2e2mu'], comb['2e2mu']):>{col}}  {fmt(fs['2mu2e'], comb['2mu2e']):>{col}}  "
                  f"{r['net_zx']:>6.1f} ± {r['comb_net']:.1f}")
        total_row  = {fs: sum(r['fs_yields'][fs] for r in results) for fs in _FINAL_STATES}
        total_comb = {fs: float(np.sqrt(sum(r['fs_comb'][fs] ** 2 for r in results))) for fs in _FINAL_STATES}
        grand_total = sum(r['net_zx'] for r in results)
        grand_comb  = float(np.sqrt(sum(r['comb_net'] ** 2 for r in results)))
        print("  " + "-" * 90)
        print(f"  {'Total':<16}  {fmt(total_row['4mu'], total_comb['4mu']):>{col}}  "
              f"{fmt(total_row['4e'], total_comb['4e']):>{col}}  "
              f"{fmt(total_row['2e2mu'], total_comb['2e2mu']):>{col}}  "
              f"{fmt(total_row['2mu2e'], total_comb['2mu2e']):>{col}}  "
              f"{grand_total:>6.1f} ± {grand_comb:.1f}")
    else:
        print("  Note: final-state breakdown not available.")
        print(f"  {'Era':<16}  {'3P1F corr':>12}  {'2P2F sub':>10}  {'Net Z+X':>10}  {'comb':>8}")
        print("  " + "-" * 60)
        for r in results:
            print(f"  {r['year']:<16}  {r['yield_3p1f_corrected']:>12.1f}  {r['yield_2p2f']:>10.1f}  "
                  f"{r['net_zx']:>10.1f}  {r['comb_net']:>8.1f}")
        grand = sum(r['net_zx'] for r in results)
        grand_comb = float(np.sqrt(sum(r['comb_net'] ** 2 for r in results)))
        print("  " + "-" * 60)
        print(f"  {'Total':<16}  {'':>12}  {'':>10}  {grand:>10.1f}  {grand_comb:>8.1f}")
    print("=" * 70 + "\n")


def main():
    global QQZZ_FOLDER_PREFIX, QQZZ_YAML_KEY

    parser = argparse.ArgumentParser()
    parser.add_argument('--merged-input', default=MERGED_BASE_DEFAULT,
                        help='hplus_FR_full_merged dir (from merge_cr_parquets_full.py)')
    parser.add_argument('--cr-input', default=CR_BASE_DEFAULT,
                        help='hplus_FR_full raw per-dataset output dir (for ZZ correction)')
    parser.add_argument('--sr-base', default=SR_BASE_DEFAULT,
                        help='SR coffea output dir (for sumw metadata)')
    parser.add_argument('--hc-path', default=HC_PATH_DEFAULT,
                        help='higgscharm repo path (for coffea util import)')
    parser.add_argument('--fake-rates', required=True)
    parser.add_argument('--electron-fr-zll', default=None,
                        help='[optional] fit_ZLL.py output pkl for corrected electron fake rates')
    parser.add_argument('--year', default='all', help="Era or 'all'")
    parser.add_argument('--output', required=True)
    parser.add_argument('--no-mass-window', action='store_true')
    parser.add_argument('--mass-window', type=float, nargs=2, default=None, metavar=('MIN', 'MAX'))
    parser.add_argument('--qqzz-folder-prefix', default=QQZZ_FOLDER_PREFIX,
                        help="On-disk folder prefix for the qqZZ sample "
                             "(matches '<prefix>' base folder + '<prefix>_ext*' extensions)")
    parser.add_argument('--qqzz-yaml-key', default=QQZZ_YAML_KEY,
                        help="Key of the qqZZ sample in the fileset yaml (xsec lookup)")
    args = parser.parse_args()

    QQZZ_FOLDER_PREFIX = args.qqzz_folder_prefix
    QQZZ_YAML_KEY       = args.qqzz_yaml_key

    sr_m4l_min, sr_m4l_max = 70.0, None
    if args.no_mass_window:
        sr_m4l_min, sr_m4l_max = None, None
        print("Mass window: DISABLED (inclusive m4l)")
    elif args.mass_window is not None:
        sr_m4l_min, sr_m4l_max = args.mass_window
        print(f"Mass window: [{sr_m4l_min}, {sr_m4l_max}] GeV")
    else:
        print(f"Mass window: [{sr_m4l_min}, {sr_m4l_max}] GeV (default)")

    fake_rates = load_fake_rates(args.fake_rates)
    electron_fr_graphs = None
    if args.electron_fr_zll is not None:
        electron_fr_graphs = load_electron_fr_graphs(args.electron_fr_zll)
        print(f"Electron FR source: {args.electron_fr_zll}")
    else:
        print("Electron FR source: fake_rates.npz (default)")

    eras = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"] if args.year == 'all' else [args.year]

    all_results = []
    for era in eras:
        fr_path = args.fake_rates
        if args.year == 'all':
            fr_dir = os.path.dirname(os.path.dirname(args.fake_rates)) or '.'
            candidate = os.path.join(fr_dir, era, 'fake_rates.pkl')
            if os.path.exists(candidate) or os.path.exists(candidate.replace('.pkl', '.npz')):
                fr_path = candidate
        fr = load_fake_rates(fr_path) if args.year == 'all' else fake_rates

        out = os.path.join(args.output, era) if args.year == 'all' else args.output
        result = estimate_zx_background(era, args.merged_input, args.cr_input, args.sr_base,
                                         args.hc_path, fr, sr_m4l_min, sr_m4l_max, out,
                                         electron_fr_graphs)
        if result is not None:
            all_results.append(result)

    if all_results:
        print_era_table(all_results)


if __name__ == '__main__':
    main()