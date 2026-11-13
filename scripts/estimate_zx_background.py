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

Usage:
    python estimate_zx_background.py \\
        --cr-input /eos/user/s/snandaku/higgscharm/outputs \\
        --fake-rates /eos/user/s/snandaku/higgscharm/outputs/fake_rates/2022postEE/fake_rates.pkl \\
        --year 2022postEE \\
        --output /eos/user/s/snandaku/Analysis/zx_background/2022postEE
"""

import os
import argparse
import pickle
import numpy as np
import pandas as pd
import awkward as ak
from glob import glob

MERGED_BASE = "/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_CR_merged"
CR_BASE     = "/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_CR"
# These are overridden in main() via --workflow

# Signal region selection cuts applied to CR events (must match hplusc_mva_4class.yaml)
# These are module-level defaults; the CLI --mass-window / --no-mass-window overrides them.
SR_M4L_MIN  = 100.0   # GeV  (mva_config.mass_window.min); set to None to disable
SR_M4L_MAX  = 150.0   # GeV  (mva_config.mass_window.max); set to None to disable
SR_CJET_REQ = False   # Default: no c-jet cut on CR events (inclusive CR within the m4l window, as the standard CMS H→ZZ fake rate method).
                      # 2026-11-12: --require-cjet sets this True -> n_cjet >= 1 (same loose c-tag WP and jet cleaning as the SR atleast_one_cjet) on the
                      # 3P1F/2P2F data AND on the ZZ MC subtracted in 3P1F. Without it the card Z+X is 2.6x too large: the CR has no jet requirement, so
                      # 0-jet / non-c-tagged Z+X events (28.9 + 0.8 of 47.7 in 2022/23 105-160) enter the card, while all MC needs >= 1 c-jet. The old claim
                      # that the c-jet efficiency is applied later via the MVA score does not hold (the scorer does not reject them); the near-cancellation
                      # seen earlier does not occur with the current inputs (net 18.1 events with the cut).

# Dataset folder name keywords to identify process type
_DATA_KW   = ('Muon', 'EGamma', 'DoubleEG', 'DoubleMuon', 'MuonEG',
               'SingleMuon', 'SingleElectron', 'SinglePhoton')
_QQZZ_KW   = ('ZZto4L',)
# GluGluToContinto / GluGlutoContinto = continuum gg→ZZ (not H→ZZ signal)
_GGZZ_KW   = ('GluGluToContinto', 'GluGlutoContinto', 'GluGlu2Zto')   # 2026-11-10: 2024 (nanov15) names GluGlu2Zto*


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


def get_fake_rate(pt, eta, pdgid, fake_rates):
    flavor = 'electron' if abs(pdgid) == 11 else 'muon'
    fr = fake_rates[flavor]
    pt_bin  = np.clip(np.searchsorted(fr['pt_edges'],  pt)  - 1, 0, len(fr['pt_edges'])  - 2)
    eta_bin = np.clip(np.searchsorted(fr['eta_edges'], np.abs(eta)) - 1, 0, len(fr['eta_edges']) - 2)
    rate = fr['fake_rate'][pt_bin, eta_bin]
    err  = fr['fake_rate_err'][pt_bin, eta_bin]
    # sentinel value 1.0 marks empty/undefined bins — treat uncertainty as unknown (0)
    if err >= 1.0:
        err = 0.0
    return rate, err


def compute_weights_3p1f(df, fake_rates):
    """
    f/(1-f) for the failing lepton in each 3P1F event.
    Also returns per-event weight for CR stat uncertainty: σ_CR = sqrt(Σ w²).
    """
    w = np.zeros(len(df))
    for i in range(len(df)):
        if not df['l3_istight_3p1f'].iloc[i]:
            f, _ = get_fake_rate(df['l3_pt_3p1f'].iloc[i], df['l3_eta_3p1f'].iloc[i],
                                 df['l3_pdgid_3p1f'].iloc[i], fake_rates)
        elif not df['l4_istight_3p1f'].iloc[i]:
            f, _ = get_fake_rate(df['l4_pt_3p1f'].iloc[i], df['l4_eta_3p1f'].iloc[i],
                                 df['l4_pdgid_3p1f'].iloc[i], fake_rates)
        else:
            f = 0.0
        w[i] = f / (1.0 - f) if f < 1.0 else 0.0
    return w


def compute_weights_2p2f(df, fake_rates):
    """f1*f2/((1-f1)(1-f2)) for each 2P2F event."""
    w = np.zeros(len(df))
    for i in range(len(df)):
        f1, _ = get_fake_rate(df['l3_pt_2p2f'].iloc[i], df['l3_eta_2p2f'].iloc[i],
                              df['l3_pdgid_2p2f'].iloc[i], fake_rates)
        f2, _ = get_fake_rate(df['l4_pt_2p2f'].iloc[i], df['l4_eta_2p2f'].iloc[i],
                              df['l4_pdgid_2p2f'].iloc[i], fake_rates)
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
            # Per-event scalar stored as a single-element list: EVERY row has exactly one entry (10-29: was
            # "total length == n_rows", which a per-jet column can satisfy by coincidence)
            counts = ak.to_numpy(ak.fill_none(ak.num(col, axis=1), 0))
            if np.all(counts == 1):
                flat[f] = np.asarray(ak.to_numpy(ak.flatten(col, axis=1)), dtype=np.float64)
            else:
                # Variable-length per-jet / per-cjet list: first element as the per-event scalar (NaN if empty)
                # plus _0/_1/_2 indexed columns. 2026-10-29: the old ak.fill_none(col, [], axis=0) turned the column
                # into a union type, every later call raised and the except blocks filled ALL jets with NaN (Z+X MVA
                # saw no jet at all). Index the option[var] column directly; no silent fallback.
                padded3 = ak.pad_none(col, 3, axis=1, clip=True)
                for jj in range(3):
                    flat[f'{f}_{jj}'] = np.asarray(ak.to_numpy(ak.fill_none(padded3[:, jj], np.nan)), dtype=np.float64)
                flat[f] = flat[f'{f}_0'].copy()
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

    if SR_CJET_REQ:
        if "n_cjet" not in df.columns:
            raise RuntimeError(f"--require-cjet: no n_cjet column in {merged}")
        before = len(df)
        df = df[df["n_cjet"] >= 1].reset_index(drop=True)
        print(f"  After n_cjet >= 1:              {len(df):,} events ({len(df)/before*100:.1f}%)")

    # ── Keep DATA events only ───────────────────────────────────────────────
    # DATA rows have weight_nominal = NaN (no MC weight); MC rows have finite values.
    if "weight_nominal" in df.columns:
        before = len(df)
        df = df[np.isnan(df["weight_nominal"])].reset_index(drop=True)
        print(f"  After DATA-only filter:         {len(df):,} events ({len(df)/before*100:.1f}% were DATA)")
    return df


_FILESET_YAML = {
    '2022preEE':    '/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/2022preEE_nanov12.yaml',
    '2022postEE':   '/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/2022postEE_nanov12.yaml',
    '2023preBPix':  '/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/2023preBPix_nanov12.yaml',
    '2023postBPix': '/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/2023postBPix_nanov12.yaml',
    '2024':         '/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/2024_nanov15.yaml',
}
# 2026-11-10: 2024 added (was missing -> "Unknown era 2024, skipping ZZ correction" = factor 1.0, no ZZ subtraction in 3P1F);
# lumi = create_datacards_2024.py LUMI_2024_PB 108960 pb^-1
_LUMI_FB = {'2022preEE': 7.98, '2022postEE': 26.67, '2023preBPix': 17.79, '2023postBPix': 9.53, '2024': 108.96}


def _ggzz_samples(year):
    """gg->ZZ continuum samples (4e, 4mu, 2e2mu) entering the ZZ-in-CR correction; 2024 (nanov15) renamed them GluGlu2Zto*."""
    if year in ('2022preEE', '2022postEE', '2023preBPix', '2023postBPix'):
        return ['GluGlutoContinto2Zto4E', 'GluGlutoContinto2Zto4Mu', 'GluGluToContinto2Zto2E2Mu']
    return ['GluGlu2Zto4E', 'GluGlu2Zto4Mu', 'GluGlu2Zto2E2Mu']
# Generated-sumw source for the ZZ-in-3P1F correction: the dedup'd per-chunk
# sumw/*.json sidecars of the ctag2d production (the same denominator the
# [100,150] datacard uses for its MC). Was outputs/hplusc/*.coffea until that
# directory was deleted 2026-09-23 -- the lookup then silently returned 0 and
# the ZZ correction became 1.0 in every era. Same input files as the CR
# productions (e.g. ZZto4L 82/208/.../81 files). CR_merged's own .coffea were
# not used: 2022postEE ZZto4L has only 7/14 of them (silent xrdcp failures).
# 2026-11-03: hplusc_mva_4class_ctag2d was deleted (EOS space); HCZZ_ZX_SUMW_BASE points to another production
# with the same generated-sumw sidecars (e.g. hplusc_mva_4class_ctag2d_lepss_bkg).
_SUMW_BASE = os.environ.get("HCZZ_ZX_SUMW_BASE", "/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d")


def _load_xsec(year, sample_key):
    """Return cross-section in pb from the era fileset YAML."""
    import yaml
    ypath = _FILESET_YAML.get(year, '')
    if not os.path.exists(ypath):
        return 0.0
    with open(ypath) as f:
        fs = yaml.safe_load(f)
    return float(fs.get(sample_key, {}).get('xsec', 0))


def _load_coffea_file(path):
    """
    Decompress+unpickle a .coffea file directly (lz4 frame + pickle), the
    same format coffea.util.load() reads, WITHOUT importing coffea.util.

    Found 2026-09-18: `from coffea.util import load` unconditionally does
    `import numba` at coffea/util.py's top level, regardless of whether
    load() itself touches any numba-jitted code. In this environment
    (miniforge3 python, numpy 2.4.3) the only numba installed anywhere
    (~/.local/lib/python3.12/site-packages, pulled in via user-site
    auto-inclusion) requires numpy<=2.3 and raises ImportError at import
    time. The caller here used to catch exactly that ImportError and
    silently return 0.0 -- so EVERY call to _get_sumw_from_coffea has been
    silently returning 0.0 in this environment, which in turn made every
    era's qqZZ/ggZZ-in-3P1F correction factor silently default to a no-op
    1.0 (see estimate_zx_background()'s "could not compute qqZZ
    correction... sumw=0.0" warning -- previously assumed to mean "no MC
    present", actually an environment bug having nothing to do with the
    data). coffea's own save()/load() is just lz4.frame-compressed pickle
    (confirmed by reading a real file's raw bytes: LZ4 frame magic 0x18224D04
    at offset 0) -- reproducing that here avoids the numba import entirely.
    """
    import lz4.frame
    import pickle
    with open(path, 'rb') as f:
        return pickle.loads(lz4.frame.decompress(f.read()))


def _get_sumw_from_coffea(year, dataset_prefix):
    """
    Generated sum of genWeight for dataset_prefix* in given year, from the
    _SUMW_BASE sumw/*.json sidecars. One sidecar per processing chunk; the
    filename (<guid>_<chunk range>.json) identifies the chunk, so keep one
    copy per filename (resubmissions write the same chunk into several
    partition dirs), same dedup as create_datacards_ctag2d_100150.load_sumw.
    Raises instead of returning 0 -- a zero here used to silently disable the
    ZZ correction.
    """
    import json
    latest = {}
    for jf in glob(os.path.join(_SUMW_BASE, year, f"{dataset_prefix}*", "sumw", "*.json")):
        name = os.path.basename(jf)
        if name not in latest or os.path.getmtime(jf) > os.path.getmtime(latest[name]):
            latest[name] = jf
    total = 0.0
    for jf in latest.values():
        with open(jf) as f:
            total += float(json.load(f)['sumw'])
    if total <= 0:
        raise RuntimeError(f"No generated sumw for {dataset_prefix}* in {year} under "
                           f"{_SUMW_BASE} -- refusing to run with the ZZ correction disabled")
    return total


def _cjet_mask(arr, pfile):
    """2026-11-12: per-event n_cjet >= 1 mask for --require-cjet (None when the cut is off)."""
    if not SR_CJET_REQ:
        return None
    if 'n_cjet' not in arr.fields:
        raise RuntimeError(f"--require-cjet: no n_cjet column in {pfile}")
    n = arr['n_cjet']
    return ak.to_numpy(ak.fill_none(ak.flatten(n, axis=1) if n.ndim > 1 else n, 0)) >= 1


def _sum_wnominal_from_cr(year, dataset_prefix, category, m4l_min=None, m4l_max=None,
                          cr_base=None, m4l_col=None):
    """Sum weight_nominal (genWeight × SFs, NOT xs-normalized) from per-dataset CR parquets."""
    cr_dir   = os.path.join(cr_base if cr_base is not None else CR_BASE, year)
    if m4l_col is None:
        m4l_col = 'm4l_3p1f' if 'CR_3P1F' in category else 'm4l_2p2f'
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
                cm = _cjet_mask(arr, pfile)
                if cm is not None:
                    mask = mask & cm
                total += float(w_np[mask].sum())
            except RuntimeError:
                raise
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
    if xs_qq <= 0:
        raise RuntimeError(f"No ZZto4L cross section for {year} -- cannot compute the ZZ correction")
    xs_norm_qq  = xs_qq * 1000 * lumi / sumw_qq   # fb → expected yield per unit weight
    raw_qq      = _sum_wnominal_from_cr(year, 'ZZto4L', category, m4l_min, m4l_max)
    n_zz_qq     = raw_qq * xs_norm_qq

    # ── ggZZ contribution (GluGluto/GluGluToContinto) ──────────────────────
    # Sum over all relevant gg→ZZ samples (4e, 4mu, 2e2mu)
    n_zz_gg = 0.0
    for sample_key in _ggzz_samples(year):
        folder_prefix = sample_key
        xs_gg   = _load_xsec(year, sample_key)
        sumw_gg = _get_sumw_from_coffea(year, folder_prefix)
        if xs_gg <= 0:
            raise RuntimeError(f"No {sample_key} cross section for {year}")
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


# ────────────────────────────────────────────────────────────────────────
# Same-sign (SS) cross-check method (HIG-24-013 "2P2LSS" control region)
# ────────────────────────────────────────────────────────────────────────
# The official analysis (HIG-24-013) runs a second, independent Z+X estimate
# from a same-sign (SS) control region as a cross-check against the OS
# 3P1F/2P2F method above (see its "Different Z+ll control regions" section).
# Unlike the OS method, the SS method uses no fake rates: same-sign
# extra-lepton pairs are (to good approximation) not produced by genuine
# ZZ, so N_data(SS) directly estimates the reducible background after
# subtracting the small residual ZZ MC contamination in that region.
#
# The CR_2P2F_SS category is already produced (hplusc_mva_4class_CR_loose_z2
# workflow) but was never consumed anywhere before this. This adds it as a
# printed cross-check only -- it does NOT feed into the datacard/combine.

SS_WORKFLOW = 'hplusc_mva_4class_CR_loose_z2'
SS_CATEGORY = 'CR_2P2F_SS'
SS_M4L_COL  = 'm4l_2p2f_ss'


def load_cr_ss_data(year):
    """
    Load CR_2P2F_SS events directly from per-dataset parquets (no merged
    directory exists for this workflow, unlike the primary OS CRs).
    Returns (n_data, df_mc) with the SR mass window applied, where df_mc
    carries per-dataset xs-normalizable weight sums for 'qqzz'/'ggzz'.
    """
    cr_dir = f"/eos/user/s/snandaku/higgscharm/outputs/{SS_WORKFLOW}/{year}"
    if not os.path.isdir(cr_dir):
        print(f"  SS CR directory not found: {cr_dir}")
        return 0, pd.DataFrame(columns=['dataset_folder', 'dataset_class', 'weight_sum'])

    n_data = 0
    mc_rows = []
    for dataset_folder in sorted(os.listdir(cr_dir)):
        dclass = _classify_dataset(dataset_folder)
        if dclass not in ('data', 'qqzz', 'ggzz'):
            continue
        pfiles = glob(os.path.join(cr_dir, dataset_folder, SS_CATEGORY, "*.parquet"))
        if not pfiles:
            continue
        for pfile in pfiles:
            try:
                arr = ak.from_parquet(pfile)
            except Exception as e:
                print(f"    Warning reading {pfile}: {e}")
                continue
            if SS_M4L_COL not in arr.fields:
                continue
            m4l_col = arr[SS_M4L_COL]
            m4l = ak.to_numpy(ak.fill_none(
                ak.flatten(m4l_col, axis=1) if m4l_col.ndim > 1 else m4l_col, np.nan))
            if SR_M4L_MIN is not None and SR_M4L_MAX is not None:
                mask = (m4l >= SR_M4L_MIN) & (m4l <= SR_M4L_MAX)
            else:
                mask = np.ones(len(m4l), dtype=bool)
            cm = _cjet_mask(arr, pfile)
            if cm is not None:
                mask = mask & cm
            n_pass = int(np.nansum(mask))
            if n_pass == 0:
                continue
            if dclass == 'data':
                n_data += n_pass
            else:
                if 'weight_nominal' not in arr.fields:
                    continue
                w_col = arr['weight_nominal']
                w = ak.to_numpy(ak.fill_none(
                    ak.flatten(w_col, axis=1) if w_col.ndim > 1 else w_col, np.nan))
                mc_rows.append({'dataset_folder': dataset_folder, 'dataset_class': dclass,
                                 'weight_sum': float(np.nansum(w[mask]))})

    df_mc = pd.DataFrame(mc_rows) if mc_rows else pd.DataFrame(
        columns=['dataset_folder', 'dataset_class', 'weight_sum'])
    return n_data, df_mc


def estimate_zx_ss_crosscheck(year):
    """
    Compute the SS-method Z+X cross-check: N_data(SS) - N_ZZ_MC(SS), in the
    same SR mass window as the OS method. Printed only -- not written to any
    output file, not fed into combine.
    """
    lumi = _LUMI_FB.get(year, 0)
    if lumi == 0:
        print(f"  Unknown era {year}, skipping SS cross-check")
        return None

    n_data, df_mc = load_cr_ss_data(year)
    if n_data == 0 and len(df_mc) == 0:
        print(f"  No CR_2P2F_SS events found for {year} -- skipping SS cross-check.")
        return None

    # xs-normalize each MC dataset's weight sum the same way compute_zz_correction
    # does for the OS method's ZZ-in-3P1F correction -- same sample-key lists.
    n_zz_mc = 0.0
    for dclass, sample_keys in [
        ('qqzz', ['ZZto4L']),
        ('ggzz', _ggzz_samples(year)),
    ]:
        sub = df_mc[df_mc['dataset_class'] == dclass]
        if len(sub) == 0:
            continue
        for sample_key in sample_keys:
            xs = _load_xsec(year, sample_key)
            sumw = _get_sumw_from_coffea(year, sample_key)
            if sumw <= 0 or xs <= 0:
                continue
            xs_norm = xs * 1000 * lumi / sumw   # fb -> expected yield per unit weight
            raw = sub[sub['dataset_folder'].str.startswith(sample_key)]['weight_sum'].sum()
            n_zz_mc += raw * xs_norm

    net_ss = n_data - n_zz_mc
    mw_str = f"[{SR_M4L_MIN},{SR_M4L_MAX}] GeV" if SR_M4L_MIN is not None else "inclusive"
    print(f"\n  SS-method cross-check (CR_2P2F_SS, m4l {mw_str}):")
    print(f"    N_data(SS):     {n_data:>10,}")
    print(f"    N_ZZ_MC(SS):    {n_zz_mc:>10.2f}")
    print(f"    Net Z+X (SS):   {net_ss:>10.2f}   (cross-check only, not used in combine)")
    return {'year': year, 'n_data_ss': n_data, 'n_zz_mc_ss': n_zz_mc, 'net_zx_ss': net_ss}


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

    # ── Fake rate weights ───────────────────────────────────────────────────
    print("\nComputing fake rate weights...")
    w3 = compute_weights_3p1f(df_3p1f, fake_rates)
    w2 = compute_weights_2p2f(df_2p2f, fake_rates) if len(df_2p2f) > 0 else np.array([])

    # Corrected weights; CR stat uncertainty = sqrt(Σ w²) per the weighted-sum variance
    cw3 = zz_correction * w3    # corrected positive contribution
    df_3p1f['zx_weight']     = cw3
    df_3p1f['zx_weight_sq']  = cw3 ** 2
    df_2p2f['zx_weight']     = -w2                 # subtracted
    df_2p2f['zx_weight_sq']  = w2 ** 2

    yield_3p1f_raw       = float(np.sum(w3))
    yield_3p1f_corrected = zz_correction * yield_3p1f_raw
    yield_2p2f           = float(np.sum(w2))
    net_zx               = yield_3p1f_corrected - yield_2p2f

    # CR stat uncertainty: σ = sqrt(Σ w_3p1f² + Σ w_2p2f²)
    err_3p1f_total = float(np.sqrt(np.sum(cw3 ** 2)))
    err_2p2f_total = float(np.sqrt(np.sum(w2 ** 2)))
    err_net_total  = float(np.sqrt(err_3p1f_total ** 2 + err_2p2f_total ** 2))

    # ── Final state breakdown ───────────────────────────────────────────────
    has_z1_pdgid = 'z1_l1_pdgid_3p1f' in df_3p1f.columns

    fs_yields = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}
    fs_errors = {fs: 0.0 for fs in ['4mu', '4e', '2e2mu', '2mu2e']}

    if has_z1_pdgid:
        # Z2 flavor: both Z2 leptons are OSSF -> same |pdgId|, use l3
        z1_flav_3p1f = df_3p1f['z1_l1_pdgid_3p1f'].values
        z2_flav_3p1f = df_3p1f['l3_pdgid_3p1f'].values
        df_3p1f['final_state'] = [get_final_state(z1, z2)
                                  for z1, z2 in zip(z1_flav_3p1f, z2_flav_3p1f)]

        for fs in fs_yields:
            mask = df_3p1f['final_state'] == fs
            fs_yields[fs] += float(np.sum(df_3p1f.loc[mask, 'zx_weight']))
            fs_errors[fs]  = float(np.sqrt(np.sum(df_3p1f.loc[mask, 'zx_weight_sq'])))

        if 'z1_l1_pdgid_2p2f' in df_2p2f.columns and len(df_2p2f) > 0:
            z1_flav_2p2f = df_2p2f['z1_l1_pdgid_2p2f'].values
            z2_flav_2p2f = df_2p2f['l3_pdgid_2p2f'].values
            df_2p2f['final_state'] = [get_final_state(z1, z2)
                                      for z1, z2 in zip(z1_flav_2p2f, z2_flav_2p2f)]
            for fs in fs_yields:
                mask = df_2p2f['final_state'] == fs
                fs_yields[fs] += float(np.sum(df_2p2f.loc[mask, 'zx_weight']))
                # 2P2F is subtracted; errors add in quadrature
                fs_errors[fs] = float(np.sqrt(
                    fs_errors[fs] ** 2 +
                    np.sum(df_2p2f.loc[mask, 'zx_weight_sq'])
                ))
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
    print(f"  Sum 3P1F [corrected]:        {yield_3p1f_corrected:>10.2f}  ± {err_3p1f_total:.2f}")
    print(f"  Sum 2P2F [subtracted]:       {yield_2p2f:>10.2f}  ± {err_2p2f_total:.2f}")
    print(f"  Net Z+X yield:               {net_zx:>10.2f}  ± {err_net_total:.2f}")
    if has_z1_pdgid:
        print(f"\n  Breakdown by final state:")
        print(f"  {'Final state':<10}  {'Yield':>8}  {'Stat err':>8}")
        print(f"  {'-'*30}")
        for fs in ['4mu', '4e', '2e2mu', '2mu2e']:
            print(f"  {fs:<10}  {fs_yields[fs]:>8.2f}  ± {fs_errors[fs]:>7.2f}")
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
        'err_net': err_net_total,
        'fs_yields': fs_yields,
        'fs_errors': fs_errors,
        'has_fs': has_z1_pdgid,
    }


def print_era_table(results):
    """Print the per-era per-final-state table matching iBOF slide page 12."""
    eras = [r['year'] for r in results]
    has_fs = any(r['has_fs'] for r in results)

    print("\n" + "="*70)
    print("  Z+X Background Yield — OS Method (UAntwerp-style table)")
    print("="*70)

    if has_fs:
        col = 18
        header = f"  {'Era':<16}  {'4mu':>{col}}  {'4e':>{col}}  {'2e2mu':>{col}}  {'2mu2e':>{col}}  {'Total':>12}"
        print(header)
        print("  " + "-"*90)
        for r in results:
            fs  = r['fs_yields']
            fe  = r['fs_errors']
            def fmt(y, e): return f"{y:.1f} ± {e:.1f}"
            print(f"  {r['year']:<16}  "
                  f"{fmt(fs['4mu'],   fe['4mu']):>{col}}  "
                  f"{fmt(fs['4e'],    fe['4e']):>{col}}  "
                  f"{fmt(fs['2e2mu'], fe['2e2mu']):>{col}}  "
                  f"{fmt(fs['2mu2e'], fe['2mu2e']):>{col}}  "
                  f"{r['net_zx']:>6.1f} ± {r['err_net']:.1f}")
        total_row  = {fs: sum(r['fs_yields'][fs] for r in results) for fs in ['4mu','4e','2e2mu','2mu2e']}
        total_err  = {fs: float(np.sqrt(sum(r['fs_errors'][fs]**2 for r in results))) for fs in ['4mu','4e','2e2mu','2mu2e']}
        grand_total = sum(r['net_zx'] for r in results)
        grand_err   = float(np.sqrt(sum(r['err_net']**2 for r in results)))
        print("  " + "-"*90)
        print(f"  {'Total':<16}  "
              f"{fmt(total_row['4mu'],   total_err['4mu']):>{col}}  "
              f"{fmt(total_row['4e'],    total_err['4e']):>{col}}  "
              f"{fmt(total_row['2e2mu'], total_err['2e2mu']):>{col}}  "
              f"{fmt(total_row['2mu2e'], total_err['2mu2e']):>{col}}  "
              f"{grand_total:>6.1f} ± {grand_err:.1f}")
    else:
        print("  Note: final-state breakdown not available (z1_l1_pdgid missing).")
        print(f"  {'Era':<16}  {'3P1F corr':>12}  {'2P2F sub':>10}  {'Net Z+X':>10}")
        print("  " + "-"*52)
        for r in results:
            print(f"  {r['year']:<16}  "
                  f"{r['yield_3p1f_corrected']:>12.1f}  "
                  f"{r['yield_2p2f']:>10.1f}  "
                  f"{r['net_zx']:>10.1f}")
        grand = sum(r['net_zx'] for r in results)
        print("  " + "-"*52)
        print(f"  {'Total':<16}  {'':>12}  {'':>10}  {grand:>10.1f}")
    print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cr-input', type=str, default=None,
                        help='(Deprecated, ignored) CR input base directory')
    parser.add_argument('--workflow', type=str, default='hplusc_mva_4class_CR',
                        help='CR workflow name (default: hplusc_mva_4class_CR)')
    parser.add_argument('--fake-rates', type=str, required=True)
    parser.add_argument('--year', type=str, default='all',
                        help="Era or 'all'")
    parser.add_argument('--output', type=str, required=True)
    parser.add_argument('--merged-base', type=str, default=None,
                        help='2026-10-28: merged CR dir (default <workflow>_merged)')
    parser.add_argument('--no-mass-window', action='store_true',
                        help='Disable m4l mass window (inclusive CR; for comparison with UAntwerp)')
    parser.add_argument('--mass-window', type=float, nargs=2, default=None,
                        metavar=('MIN', 'MAX'),
                        help='Override mass window in GeV (default: 100 150)')
    parser.add_argument('--ss-crosscheck', action='store_true',
                        help='Also print the SS-method (CR_2P2F_SS) cross-check '
                             'per era. Printed only, not fed into combine.')
    parser.add_argument('--require-cjet', action='store_true',
                        help='2026-11-12: apply the SR c-jet requirement (n_cjet >= 1) to the CR data and the ZZ MC subtraction')
    args = parser.parse_args()

    # Set workflow-dependent globals before any estimation runs
    global MERGED_BASE, CR_BASE
    MERGED_BASE = args.merged_base or f"/eos/user/s/snandaku/higgscharm/outputs/{args.workflow}_merged"
    CR_BASE     = f"/eos/user/s/snandaku/higgscharm/outputs/{args.workflow}"
    print(f"Workflow:     {args.workflow}")
    print(f"Merged base:  {MERGED_BASE}")
    print(f"CR base:      {CR_BASE}")

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

    global SR_CJET_REQ
    if args.require_cjet:
        SR_CJET_REQ = True
    print(f"c-jet requirement (n_cjet >= 1): {'ON' if SR_CJET_REQ else 'OFF'}")

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

        if args.ss_crosscheck:
            estimate_zx_ss_crosscheck(era)

    if all_results:
        print_era_table(all_results)


if __name__ == '__main__':
    main()
