#!/usr/bin/env python3
"""
Estimate Z+X (reducible) background using fake rate method.

Formula (OS method, UAntwerp):
  N_bkg_SR = (1 - N_ZZ_3P1F / N_3P1F) * sum_3P1F[ f/(1-f) ]
             - sum_2P2F[ f1*f2 / ((1-f1)*(1-f2)) ]

where N_ZZ_3P1F is the ZZ MC (qqZZ + ggZZ) contribution in the 3P1F CR,
and N_3P1F is the total number of observed DATA events in the 3P1F CR.

Final states (determined from Z1 + Z2 lepton flavors):
  4mu  : Z1->mumu, Z2->mumu
  4e   : Z1->ee,   Z2->ee
  2e2mu: Z1->ee,   Z2->mumu
  2mu2e: Z1->mumu, Z2->ee

ERROR PROPAGATION (matches ZZAnalysis OSmethod.C::PrintZXYields):
  stat      = sqrt(Sum w_i^2) over all contributing CR events (ROOT
              TH1::IntegralAndError-equivalent with Sumw2 enabled) — i.e.
              exactly what was already computed here as err_net_total.
  syst      = net_zx * ((net_zx_up / net_zx) - 1), where net_zx_up is the
              SAME calculation redone with every fake rate shifted to its
              upper 1-sigma bound (f -> f + fake_rate_err). Single-sided:
              the reference C++ code only ever computes the Up variation
              for this method and applies it symmetrically.
  syst_comp = net_zx * 0.30  (flat 30% background-composition systematic,
              measured in Run I, applied identically in the reference code)
  comb      = sqrt(stat^2 + syst^2 + syst_comp^2)   (symmetric total)

Usage:
    python estimate_zx_background.py \\
        --cr-input /eos/user/p/pkatris/higgscharm/outputs \\
        --fake-rates /eos/user/p/pkatris/higgscharm/outputs/fake_rates/2022postEE/fake_rates.pkl \\
        --year 2022postEE \\
        --output /eos/user/p/pkatris/Analysis/zx_background/2022postEE
"""

import os
import argparse
import pickle
import numpy as np
import pandas as pd
import awkward as ak
from glob import glob

MERGED_BASE = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR_full_merged"
CR_BASE     = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged"

# Signal region selection cuts applied to CR events (must match hplusc_mva_4class.yaml)
# These are module-level defaults; the CLI --mass-window / --no-mass-window overrides them.
SR_M4L_MIN  = 70   # GeV  (mva_config.mass_window.min); set to None to disable
SR_M4L_MAX  = None   # GeV  (mva_config.mass_window.max); set to None to disable
SR_CJET_REQ = False   # Do NOT apply c-jet cut to CR events.
                      # Standard CMS H→ZZ fake rate method (AN-18-340, UAntwerp iBOF)
                      # uses inclusive CR events within the m4l window.  Applying the
                      # SR c-jet requirement here incorrectly reduces N_3P1F (fewer
                      # events relative to N_2P2F) and causes near-cancellation in the
                      # Z+X estimate.  The c-jet efficiency is accounted for separately
                      # via the SR selection when the final MVA score is applied.

# Background-composition flat systematic (Run I measurement), matches OSmethod.C
BKG_COMPOSITION_SYST = 0.30

# Dataset folder name keywords to identify process type
_DATA_KW   = ('Muon', 'EGamma', 'DoubleEG', 'DoubleMuon', 'MuonEG',
               'SingleMuon', 'SingleElectron', 'SinglePhoton')
_QQZZ_KW   = ('ZZto4L',)
# GluGluToContinto / GluGlutoContinto = continuum gg→ZZ (not H→ZZ signal)
_GGZZ_KW   = ('GluGlutoContinto', 'GluGluToContinto')


def _classify_dataset(folder_name):
    """Return 'data', 'qqzz', 'ggzz', or 'other' from the dataset folder name."""
    if any(kw in folder_name for kw in _DATA_KW):
        return 'data'
    if any(kw in folder_name for kw in _QQZZ_KW):
        return 'qqzz'
    if any(kw in folder_name for kw in _GGZZ_KW):
        return 'ggzz'
    return 'other'


def load_fake_rates(path):
    """Load from .npz (numpy-version agnostic) or fall back to .pkl."""
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


def get_fake_rate(pt, eta, pdgid, fake_rates, variant='nominal'):
    """
    Return the fake rate for a single lepton.

    variant:
      'nominal' -> f
      'up'      -> f + fake_rate_err   (matches FakeRates::GetFakeRate_Up)
      'dn'      -> max(0, f - fake_rate_err)  (matches FakeRates::GetFakeRate_Dn)
    """
    flavor = 'electron' if abs(pdgid) == 11 else 'muon'
    fr = fake_rates[flavor]
    pt_bin  = np.clip(np.searchsorted(fr['pt_edges'],  pt)  - 1, 0, len(fr['pt_edges'])  - 2)
    eta_bin = np.clip(np.searchsorted(fr['eta_edges'], np.abs(eta)) - 1, 0, len(fr['eta_edges']) - 2)
    rate = fr['fake_rate'][pt_bin, eta_bin]
    err  = fr['fake_rate_err'][pt_bin, eta_bin]
    # sentinel value 1.0 marks empty/undefined bins — treat uncertainty as unknown (0)
    if err >= 1.0:
        err = 0.0
    if variant == 'up':
        return min(rate + err, 0.999999)
    if variant == 'dn':
        return max(rate - err, 0.0)
    return rate


def compute_weights_3p1f(df, fake_rates, variant='nominal'):
    """
    f/(1-f) for the failing lepton in each 3P1F event.
    variant selects nominal / up / dn fake rate (see get_fake_rate).
    """
    w = np.zeros(len(df))
    for i in range(len(df)):
        if not df['l3_istight_3p1f'].iloc[i]:
            f = get_fake_rate(df['l3_pt_3p1f'].iloc[i], df['l3_eta_3p1f'].iloc[i],
                              df['l3_pdgid_3p1f'].iloc[i], fake_rates, variant)
        elif not df['l4_istight_3p1f'].iloc[i]:
            f = get_fake_rate(df['l4_pt_3p1f'].iloc[i], df['l4_eta_3p1f'].iloc[i],
                              df['l4_pdgid_3p1f'].iloc[i], fake_rates, variant)
        else:
            f = 0.0
        w[i] = f / (1.0 - f) if f < 1.0 else 0.0
    return w


def compute_weights_2p2f(df, fake_rates, variant='nominal'):
    """f1*f2/((1-f1)(1-f2)) for each 2P2F event. variant applied to BOTH legs."""
    w = np.zeros(len(df))
    for i in range(len(df)):
        f1 = get_fake_rate(df['l3_pt_2p2f'].iloc[i], df['l3_eta_2p2f'].iloc[i],
                           df['l3_pdgid_2p2f'].iloc[i], fake_rates, variant)
        f2 = get_fake_rate(df['l4_pt_2p2f'].iloc[i], df['l4_eta_2p2f'].iloc[i],
                           df['l4_pdgid_2p2f'].iloc[i], fake_rates, variant)
        w[i] = (f1 * f2) / ((1.0 - f1) * (1.0 - f2)) if f1 < 1.0 and f2 < 1.0 else 0.0
    return w


def get_final_state(z1_pdgid, z2_pdgid):
    """
    Return final state label from Z1 and Z2 lepton |pdgId|.
    CMS convention: label is Z1 leptons first, then Z2 leptons.
      Z1->mumu, Z2->mumu -> 4mu
      Z1->ee,   Z2->ee   -> 4e
      Z1->ee,   Z2->mumu -> 2e2mu
      Z1->mumu, Z2->ee   -> 2mu2e
    """
    mapping = {
        (13, 13): '4mu',
        (11, 11): '4e',
        (11, 13): '2e2mu',
        (13, 11): '2mu2e',
    }
    return mapping.get((int(z1_pdgid), int(z2_pdgid)), 'unknown')


def _load_flat(arr, fields):
    """Flatten large_list columns from awkward array into a dict of numpy arrays.

    Three column types handled:
    1. 1D (per-row scalars): fill_none → NaN
    2. 2D where flattened_len == n_rows (per-event lists of length 1): flatten
    3. 2D variable-length (per-jet / per-cjet): extract first element as per-event
       scalar (NaN when list is empty); also create _0/_1/_2 indexed columns for
       jet arrays so the MVA can access leading/subleading jets by index.

    Nullable 1D columns (e.g. weight_nominal, None for DATA rows) become NaN so
    callers can distinguish DATA (NaN) from MC (finite value).
    """
    n_rows = len(arr)
    flat = {}
    for f in fields:
        if f not in arr.fields:
            continue
        col = arr[f]
        if col.ndim == 1:
            flat[f] = ak.to_numpy(ak.fill_none(col, np.nan))
        else:
            # Try to flatten: if every row contributes exactly 1 value, keep as-is
            try:
                flat_col = ak.to_numpy(ak.flatten(ak.fill_none(col, np.nan), axis=1))
            except Exception:
                flat_col = None
            if flat_col is not None and len(flat_col) == n_rows:
                # Per-event scalar stored as single-element list
                flat[f] = flat_col
            else:
                # Variable-length per-jet / per-cjet list
                # Extract first element as per-event scalar (NaN if empty)
                padded = ak.pad_none(col, 1, axis=1)
                flat[f] = ak.to_numpy(ak.fill_none(padded[:, 0], np.nan))
                # Also emit indexed columns _0, _1, _2 for jet arrays
                max_idx = 3
                padded3 = ak.pad_none(col, max_idx, axis=1)
                for j in range(max_idx):
                    flat[f'{f}_{j}'] = ak.to_numpy(ak.fill_none(padded3[:, j], np.nan))
    return flat


def load_cr_data(year, category):
    """
    Load merged CR parquet (DATA only, based on weight_nominal == 1 heuristic
    won't work — instead load merged and return all events; caller uses dataset
    info computed separately for the ZZ correction).
    Returns a DataFrame with one row per event.
    """
    merged = os.path.join(MERGED_BASE, year, f"{category}.parquet")
    if not os.path.exists(merged):
        print(f"  Merged file not found: {merged}")
        return pd.DataFrame()

    arr = ak.from_parquet(merged)

    # Load all fields relevant to this CR category:
    #   - category-specific columns (end with _3p1f or _2p2f)
    #   - shared per-event scalar columns (1D, no CR suffix)
    # Skip columns belonging to the OTHER CR type (they have a different event count).
    other_suffix = "_2p2f" if category == "CR_3P1F" else "_3p1f"
    needed = [f for f in arr.fields if not f.endswith(other_suffix)]

    flat = _load_flat(arr, needed)
    df = pd.DataFrame(flat)
    print(f"  Loaded {len(df):,} events for {category} (before SR cuts)")

    # ── Apply SR selection cuts to CR events ────────────────────────────────
    m4l_col = "m4l_3p1f" if category == "CR_3P1F" else "m4l_2p2f"
    before = len(df)
    if SR_M4L_MIN is not None and SR_M4L_MAX is not None:
        mask = (df[m4l_col] >= SR_M4L_MIN) & (df[m4l_col] <= SR_M4L_MAX)
        df = df[mask].reset_index(drop=True)
        print(f"  After m4l [{SR_M4L_MIN},{SR_M4L_MAX}] GeV: {len(df):,} events ({len(df)/before*100:.1f}%)")
    else:
        print(f"  m4l window: NONE (inclusive)")

    if SR_CJET_REQ and "cjet_HT" in df.columns:
        before = len(df)
        df = df[df["cjet_HT"] > 0].reset_index(drop=True)
        print(f"  After cjet_HT > 0:              {len(df):,} events ({len(df)/before*100:.1f}%)")

    # ── Keep DATA events only ───────────────────────────────────────────────
    # DATA rows have weight_nominal = NaN (no MC weight); MC rows have finite values.
    if "weight_nominal" in df.columns:
        before = len(df)
        df = df[np.isnan(df["weight_nominal"])].reset_index(drop=True)
        print(f"  After DATA-only filter:         {len(df):,} events ({len(df)/before*100:.1f}% were DATA)")
    return df


_FILESET_YAML = {
    '2022preEE':    '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2022preEE_nanov12.yaml',
    '2022postEE':   '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2022postEE_nanov12.yaml',
    '2023preBPix':  '/afs/cern.ch/user/p/pkatris//higgscharm/analysis/filesets/2023preBPix_nanov12.yaml',
    '2023postBPix': '/afs/cern.ch/user/p/pkatris/higgscharm/analysis/filesets/2023postBPix_nanov12.yaml',
}
_LUMI_FB = {'2022preEE': 7.98, '2022postEE': 26.67, '2023preBPix': 17.79, '2023postBPix': 9.53}
_SR_BASE  = "/eos/user/p/pkatris/higgscharm/outputs/hplusc"


def _load_xsec(year, sample_key):
    """Return cross-section in pb from the era fileset YAML."""
    import yaml
    ypath = _FILESET_YAML.get(year, '')
    if not os.path.exists(ypath):
        return 0.0
    with open(ypath) as f:
        fs = yaml.safe_load(f)
    return float(fs.get(sample_key, {}).get('xsec', 0))


def _get_sumw_from_coffea(year, dataset_prefix):
    """Sum 'sumw' metadata from SR coffea files for dataset_prefix* in given year."""
    try:
        import sys
        hc_path = '/afs/cern.ch/user/p/pkatris/higgscharm'
        if hc_path not in sys.path:
            sys.path.insert(0, hc_path)
        from coffea.util import load as coffea_load
    except ImportError:
        return 0.0

    total = 0.0
    for cf in glob(os.path.join(_SR_BASE, year, f"{dataset_prefix}*", "*.coffea")):
        try:
            out = coffea_load(cf)
            if isinstance(out, dict) and 'metadata' in out:
                total += float(out['metadata'].get('sumw', 0))
        except Exception:
            pass
    return total


def _sum_wnominal_from_cr(year, dataset_prefix, category, m4l_min=None, m4l_max=None):
    """Sum weight_nominal (genWeight × SFs, NOT xs-normalized) from per-dataset CR parquets."""
    cr_dir   = os.path.join(CR_BASE, year)
    m4l_col  = 'm4l_3p1f' if 'CR_3P1F' in category else 'm4l_2p2f'
    total    = 0.0
    for dataset_folder in sorted(os.listdir(cr_dir)):
        if not dataset_folder.startswith(dataset_prefix):
            continue
        for pfile in glob(os.path.join(cr_dir, dataset_folder, category, "*.parquet")):
            try:
                arr = ak.from_parquet(pfile)
                if 'weight_nominal' not in arr.fields:
                    continue
                w = arr['weight_nominal']
                w_np = ak.to_numpy(
                    ak.fill_none(ak.flatten(w, axis=1) if w.ndim > 1 else w, np.nan))
                mask = ~np.isnan(w_np)
                if m4l_min is not None and m4l_col in arr.fields:
                    m4l = arr[m4l_col]
                    m4l_np = ak.to_numpy(
                        ak.fill_none(ak.flatten(m4l, axis=1) if m4l.ndim > 1 else m4l, np.nan))
                    mask = mask & (m4l_np >= m4l_min) & (m4l_np <= m4l_max)
                total += float(w_np[mask].sum())
            except Exception as e:
                print(f"    Warning reading {pfile}: {e}")
    return total


def compute_zz_correction(year, category="CR_3P1F", df_data=None):
    """
    Compute ZZ correction factor: (1 - N_ZZ_3P1F / N_3P1F)

    N_ZZ_3P1F = expected yield of qqZZ events in the 3P1F CR, computed as:
        N_ZZ = Σ weight_nominal × (xs × lumi / sumw_total)
    where weight_nominal = genWeight × SFs (NOT xs-normalized in parquets),
    sumw_total = sum of all genWeights from the full NanoAOD sample (read from
    SR coffea metadata), and xs/lumi are from the fileset YAML.

    N_3P1F = DATA events in CR passing the same m4l window (from df_data).
    """
    lumi = _LUMI_FB.get(year, 0)
    if lumi == 0:
        print(f"  Unknown era {year}, skipping ZZ correction")
        return 1.0, 0, 0.0

    m4l_min = SR_M4L_MIN
    m4l_max = SR_M4L_MAX

    # ── qqZZ contribution (ZZto4L) ─────────────────────────────────────────
    xs_qq   = _load_xsec(year, 'ZZto4L')   # pb
    sumw_qq = _get_sumw_from_coffea(year, 'ZZto4L')
    if sumw_qq > 0 and xs_qq > 0:
        xs_norm_qq  = xs_qq * 1000 * lumi / sumw_qq   # fb → expected yield per unit weight
        raw_qq      = _sum_wnominal_from_cr(year, 'ZZto4L', category, m4l_min, m4l_max)
        n_zz_qq     = raw_qq * xs_norm_qq
    else:
        n_zz_qq = 0.0
        print(f"  Warning: could not compute qqZZ correction for {year} (xs={xs_qq}, sumw={sumw_qq})")

    # ── ggZZ contribution (GluGluto/GluGluToContinto) ──────────────────────
    # Sum over all relevant gg→ZZ samples (4e, 4mu, 2e2mu)
    n_zz_gg = 0.0
    for sample_key, folder_prefix in [
        ('GluGlutoContinto2Zto4E',       'GluGlutoContinto2Zto4E'),
        ('GluGlutoContinto2Zto4Mu',      'GluGlutoContinto2Zto4Mu'),
        ('GluGluToContinto2Zto2E2Mu',    'GluGluToContinto2Zto2E2Mu'),
    ]:
        xs_gg   = _load_xsec(year, sample_key)
        sumw_gg = _get_sumw_from_coffea(year, folder_prefix)
        if sumw_gg > 0 and xs_gg > 0:
            xs_norm = xs_gg * 1000 * lumi / sumw_gg
            raw     = _sum_wnominal_from_cr(year, folder_prefix, category, m4l_min, m4l_max)
            n_zz_gg += raw * xs_norm

    n_zz_mc = n_zz_qq + n_zz_gg

    # ── N_DATA ─────────────────────────────────────────────────────────────
    n_data = len(df_data) if df_data is not None else 0
    if n_data == 0:
        print(f"  Warning: no DATA events found in {category} for {year}")
        return 1.0, 0, 0.0

    correction = 1.0 - n_zz_mc / n_data
    mw_str = f"[{m4l_min},{m4l_max}] GeV" if m4l_min is not None else "inclusive"
    print(f"  ZZ correction ({category}, m4l {mw_str}): "
          f"N_data={n_data:,}  N_qqZZ={n_zz_qq:.2f}  N_ggZZ={n_zz_gg:.2f}  "
          f"N_ZZ_total={n_zz_mc:.2f}  factor={correction:.4f}")
    return correction, n_data, n_zz_mc


def estimate_zx_background(year, fake_rates, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # ── Load CR events ──────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Year: {year}")
    print(f"{'='*60}")
    print("\nLoading 3P1F events...")
    df_3p1f = load_cr_data(year, "CR_3P1F")
    print("\nLoading 2P2F events...")
    df_2p2f = load_cr_data(year, "CR_2P2F")

    if len(df_3p1f) == 0:
        print("No 3P1F events — skipping.")
        return None, None

    # ── ZZ correction factor ────────────────────────────────────────────────
    # weight_nominal in per-dataset parquets = genWeight × SFs × (xs × lumi / sumw),
    # i.e. the properly normalized expected yield per event.
    # N_3P1F (DATA) is taken from df_3p1f which already has the m4l window applied.
    print("\nComputing ZZ correction factor...")
    zz_correction, n_3p1f_data, n_zz_mc = compute_zz_correction(
        year, category="CR_3P1F", df_data=df_3p1f
    )

    # ── Fake rate weights (nominal AND Up variant, for the syst.) ───────────
    print("\nComputing fake rate weights (nominal + Up variation)...")
    w3        = compute_weights_3p1f(df_3p1f, fake_rates, variant='nominal')
    w3_up     = compute_weights_3p1f(df_3p1f, fake_rates, variant='up')
    w2        = compute_weights_2p2f(df_2p2f, fake_rates, variant='nominal') if len(df_2p2f) > 0 else np.array([])
    w2_up     = compute_weights_2p2f(df_2p2f, fake_rates, variant='up')      if len(df_2p2f) > 0 else np.array([])

    # Corrected weights; CR stat uncertainty = sqrt(Σ w²) per the weighted-sum variance
    # (matches ROOT TH1::IntegralAndError with Sumw2 enabled, as used in OSmethod.C)
    cw3      = zz_correction * w3       # corrected positive contribution (nominal)
    cw3_up   = zz_correction * w3_up    # corrected positive contribution (Up variant)
    df_3p1f['zx_weight']       = cw3
    df_3p1f['zx_weight_sq']    = cw3 ** 2
    df_3p1f['zx_weight_up']    = cw3_up
    df_2p2f['zx_weight']       = -w2                 # subtracted
    df_2p2f['zx_weight_sq']    = w2 ** 2
    df_2p2f['zx_weight_up']    = -w2_up

    yield_3p1f_raw        = float(np.sum(w3))
    yield_3p1f_corrected  = zz_correction * yield_3p1f_raw
    yield_2p2f            = float(np.sum(w2))
    net_zx                = yield_3p1f_corrected - yield_2p2f

    yield_3p1f_raw_up       = float(np.sum(w3_up))
    yield_3p1f_corrected_up = zz_correction * yield_3p1f_raw_up
    yield_2p2f_up           = float(np.sum(w2_up))
    net_zx_up               = yield_3p1f_corrected_up - yield_2p2f_up

    # ── CR stat uncertainty: σ = sqrt(Σ w_3p1f² + Σ w_2p2f²)  [matches OSmethod.C] ──
    err_3p1f_total = float(np.sqrt(np.sum(cw3 ** 2)))
    err_2p2f_total = float(np.sqrt(np.sum(w2 ** 2)))
    stat_net       = float(np.sqrt(err_3p1f_total ** 2 + err_2p2f_total ** 2))

    # ── Fake-rate systematic: single-sided "Up" variation, applied symmetrically ──
    # syst = net_zx * ((net_zx_up / net_zx) - 1)   [matches OSmethod.C::PrintZXYields]
    syst_net = net_zx * ((net_zx_up / net_zx) - 1.0) if net_zx != 0 else 0.0

    # ── Flat 30% background-composition systematic [matches OSmethod.C] ──
    syst_comp_net = net_zx * BKG_COMPOSITION_SYST

    # ── Combined total: comb = sqrt(stat^2 + syst^2 + syst_comp^2) ──
    comb_net = float(np.sqrt(stat_net ** 2 + syst_net ** 2 + syst_comp_net ** 2))

    # ── Final state breakdown ───────────────────────────────────────────────
    has_z1_pdgid = 'z1_l1_pdgid_3p1f' in df_3p1f.columns

    fs_yields    = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}
    fs_yields_up = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}
    fs_stat      = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}
    fs_syst      = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}
    fs_syst_comp = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}
    fs_comb      = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}

    if has_z1_pdgid:
        # Z2 flavor: both Z2 leptons are OSSF -> same |pdgId|, use l3
        z1_flav_3p1f = df_3p1f['z1_l1_pdgid_3p1f'].values
        z2_flav_3p1f = df_3p1f['l3_pdgid_3p1f'].values
        df_3p1f['final_state'] = [get_final_state(z1, z2)
                                  for z1, z2 in zip(z1_flav_3p1f, z2_flav_3p1f)]

        for fs in fs_yields:
            mask = df_3p1f['final_state'] == fs
            fs_yields[fs]    += float(np.sum(df_3p1f.loc[mask, 'zx_weight']))
            fs_yields_up[fs] += float(np.sum(df_3p1f.loc[mask, 'zx_weight_up']))
            fs_stat[fs]       = float(np.sqrt(np.sum(df_3p1f.loc[mask, 'zx_weight_sq'])))

        if 'z1_l1_pdgid_2p2f' in df_2p2f.columns and len(df_2p2f) > 0:
            z1_flav_2p2f = df_2p2f['z1_l1_pdgid_2p2f'].values
            z2_flav_2p2f = df_2p2f['l3_pdgid_2p2f'].values
            df_2p2f['final_state'] = [get_final_state(z1, z2)
                                      for z1, z2 in zip(z1_flav_2p2f, z2_flav_2p2f)]
            for fs in fs_yields:
                mask = df_2p2f['final_state'] == fs
                fs_yields[fs]    += float(np.sum(df_2p2f.loc[mask, 'zx_weight']))
                fs_yields_up[fs] += float(np.sum(df_2p2f.loc[mask, 'zx_weight_up']))
                # 2P2F is subtracted; stat errors add in quadrature
                fs_stat[fs] = float(np.sqrt(
                    fs_stat[fs] ** 2 +
                    np.sum(df_2p2f.loc[mask, 'zx_weight_sq'])
                ))

        # syst + syst_comp + comb per final state
        for fs in fs_yields:
            y, y_up = fs_yields[fs], fs_yields_up[fs]
            fs_syst[fs]      = y * ((y_up / y) - 1.0) if y != 0 else 0.0
            fs_syst_comp[fs] = y * BKG_COMPOSITION_SYST
            fs_comb[fs]      = float(np.sqrt(fs_stat[fs]**2 + fs_syst[fs]**2 + fs_syst_comp[fs]**2))
    else:
        print("  Note: z1_l1_pdgid not yet available — rerun CR condor jobs to get final-state breakdown.")

    # ── Save output parquets ────────────────────────────────────────────────
    df_3p1f['m4l'] = df_3p1f['m4l_3p1f']
    df_3p1f.to_parquet(os.path.join(output_dir, f'zx_3p1f_{year}.parquet'))
    if len(df_2p2f) > 0:
        df_2p2f['m4l'] = df_2p2f['m4l_2p2f']
        df_2p2f.to_parquet(os.path.join(output_dir, f'zx_2p2f_{year}.parquet'))

    # ── Print summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Z+X Yield Summary — {year}")
    print(f"{'='*60}")
    mw_str = f"[{SR_M4L_MIN},{SR_M4L_MAX}] GeV" if SR_M4L_MIN is not None else "inclusive"
    print(f"  m4l window:                  {mw_str:>10}")
    print(f"  N_3P1F (DATA events):        {n_3p1f_data:>10,}")
    print(f"  N_ZZ_MC in 3P1F:             {n_zz_mc:>10.2f}")
    print(f"  ZZ correction factor:        {zz_correction:>10.4f}")
    print(f"  Sum 3P1F f/(1-f) [raw]:      {yield_3p1f_raw:>10.2f}")
    print(f"  Sum 3P1F [corrected]:        {yield_3p1f_corrected:>10.2f}")
    print(f"  Sum 2P2F [subtracted]:       {yield_2p2f:>10.2f}")
    print(f"  Net Z+X yield:               {net_zx:>10.2f}")
    print(f"  ── error breakdown ──")
    print(f"  stat (CR, sqrt(Sum w^2)):    {stat_net:>10.4f}")
    print(f"  syst (fake-rate Up var.):    {syst_net:>10.4f}")
    print(f"  syst_comp (flat 30%):        {syst_comp_net:>10.4f}")
    print(f"  >>> comb (total):            {comb_net:>10.4f}  <<<")
    if has_z1_pdgid:
        print(f"\n  Breakdown by final state:")
        print(f"  {'Final state':<10}  {'Yield':>8}  {'stat':>8}  {'syst':>8}  {'syst_comp':>9}  {'comb':>8}")
        print(f"  {'-'*62}")
        for fs in ['4mu', '4e', '2e2mu', '2mu2e']:
            print(f"  {fs:<10}  {fs_yields[fs]:>8.2f}  {fs_stat[fs]:>8.2f}  "
                  f"{fs_syst[fs]:>8.2f}  {fs_syst_comp[fs]:>9.2f}  {fs_comb[fs]:>8.2f}")
    print(f"{'='*60}\n")

    return df_3p1f, df_2p2f, {
        'year': year,
        'n_3p1f_data': n_3p1f_data,
        'n_zz_mc': n_zz_mc,
        'zz_correction': zz_correction,
        'yield_3p1f_raw': yield_3p1f_raw,
        'yield_3p1f_corrected': yield_3p1f_corrected,
        'yield_2p2f': yield_2p2f,
        'net_zx': net_zx,
        'stat_net': stat_net,
        'syst_net': syst_net,
        'syst_comp_net': syst_comp_net,
        'comb_net': comb_net,
        'fs_yields': fs_yields,
        'fs_stat': fs_stat,
        'fs_syst': fs_syst,
        'fs_syst_comp': fs_syst_comp,
        'fs_comb': fs_comb,
        'has_fs': has_z1_pdgid,
    }


def print_era_table(results):
    """Print the per-era per-final-state table matching iBOF slide page 12."""
    has_fs = any(r['has_fs'] for r in results)

    print("\n" + "="*70)
    print("  Z+X Background Yield — OS Method (UAntwerp-style table)")
    print("  errors: comb = sqrt(stat^2 + syst^2 + syst_comp^2)")
    print("="*70)

    if has_fs:
        col = 18
        header = f"  {'Era':<16}  {'4mu':>{col}}  {'4e':>{col}}  {'2e2mu':>{col}}  {'2mu2e':>{col}}  {'Total':>12}"
        print(header)
        print("  " + "-"*90)
        for r in results:
            fs   = r['fs_yields']
            comb = r['fs_comb']
            def fmt(y, e): return f"{y:.1f} ± {e:.1f}"
            print(f"  {r['year']:<16}  "
                  f"{fmt(fs['4mu'],   comb['4mu']):>{col}}  "
                  f"{fmt(fs['4e'],    comb['4e']):>{col}}  "
                  f"{fmt(fs['2e2mu'], comb['2e2mu']):>{col}}  "
                  f"{fmt(fs['2mu2e'], comb['2mu2e']):>{col}}  "
                  f"{r['net_zx']:>6.1f} ± {r['comb_net']:.1f}")
        total_row  = {fs: sum(r['fs_yields'][fs] for r in results) for fs in ['4mu','4e','2e2mu','2mu2e']}
        total_comb = {fs: float(np.sqrt(sum(r['fs_comb'][fs]**2 for r in results))) for fs in ['4mu','4e','2e2mu','2mu2e']}
        grand_total = sum(r['net_zx'] for r in results)
        grand_comb  = float(np.sqrt(sum(r['comb_net']**2 for r in results)))
        print("  " + "-"*90)
        print(f"  {'Total':<16}  "
              f"{fmt(total_row['4mu'],   total_comb['4mu']):>{col}}  "
              f"{fmt(total_row['4e'],    total_comb['4e']):>{col}}  "
              f"{fmt(total_row['2e2mu'], total_comb['2e2mu']):>{col}}  "
              f"{fmt(total_row['2mu2e'], total_comb['2mu2e']):>{col}}  "
              f"{grand_total:>6.1f} ± {grand_comb:.1f}")
    else:
        print("  Note: final-state breakdown not available (z1_l1_pdgid missing).")
        print(f"  {'Era':<16}  {'3P1F corr':>12}  {'2P2F sub':>10}  {'Net Z+X':>10}  {'comb':>8}")
        print("  " + "-"*60)
        for r in results:
            print(f"  {r['year']:<16}  "
                  f"{r['yield_3p1f_corrected']:>12.1f}  "
                  f"{r['yield_2p2f']:>10.1f}  "
                  f"{r['net_zx']:>10.1f}  "
                  f"{r['comb_net']:>8.1f}")
        grand = sum(r['net_zx'] for r in results)
        grand_comb = float(np.sqrt(sum(r['comb_net']**2 for r in results)))
        print("  " + "-"*60)
        print(f"  {'Total':<16}  {'':>12}  {'':>10}  {grand:>10.1f}  {grand_comb:>8.1f}")
    print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cr-input', type=str, required=True)
    parser.add_argument('--fake-rates', type=str, required=True)
    parser.add_argument('--year', type=str, default='all',
                        help="Era or 'all'")
    parser.add_argument('--output', type=str, required=True)
    parser.add_argument('--no-mass-window', action='store_true',
                        help='Disable m4l mass window (inclusive CR; for comparison with UAntwerp)')
    parser.add_argument('--mass-window', type=float, nargs=2, default=None,
                        metavar=('MIN', 'MAX'),
                        help='Override mass window in GeV (default: 100 150)')
    args = parser.parse_args()

    # Apply mass window settings to module globals before running
    global SR_M4L_MIN, SR_M4L_MAX
    if args.no_mass_window:
        SR_M4L_MIN = None
        SR_M4L_MAX = None
        print("Mass window: DISABLED (inclusive m4l)")
    elif args.mass_window is not None:
        SR_M4L_MIN, SR_M4L_MAX = args.mass_window
        print(f"Mass window: [{SR_M4L_MIN}, {SR_M4L_MAX}] GeV")
    else:
        print(f"Mass window: [{SR_M4L_MIN}, {SR_M4L_MAX}] GeV (default)")

    fake_rates = load_fake_rates(args.fake_rates)

    eras = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"] \
           if args.year == 'all' else [args.year]

    all_results = []
    for era in eras:
        fr_path = args.fake_rates.replace(
            os.path.basename(args.fake_rates),
            ''
        ).rstrip('/') + f"/../{era}/fake_rates.pkl" \
            if args.year == 'all' else args.fake_rates

        # For --year all, load per-era fake rates
        if args.year == 'all':
            fr_dir = os.path.dirname(os.path.dirname(args.fake_rates))
            fr_path = os.path.join(fr_dir, era, 'fake_rates.pkl')
            if not os.path.exists(fr_path):
                print(f"Fake rates not found for {era}: {fr_path}")
                continue
            fr = load_fake_rates(fr_path)
        else:
            fr = fake_rates

        out = os.path.join(args.output, era) if args.year == 'all' else args.output
        result = estimate_zx_background(era, fr, out)
        if result[2] is not None:
            all_results.append(result[2])

    if all_results:
        print_era_table(all_results)


if __name__ == '__main__':
    main()