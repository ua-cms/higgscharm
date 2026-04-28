#!/usr/bin/env python3
"""
Validation (2b): Key MVA input feature distributions in the mass sideband.

Compares DATA vs prediction (MC irreducible + data-driven Z+X) in the CR sideband
(m4l outside [90, 160] GeV). Checks whether the data-driven Z+X has realistic
kinematics — if the stacked prediction matches data in the sideband, the
CR→SR kinematic extrapolation is supported.

Strategy:
  - DATA:           DATA events in the CR (3P1F category), m4l outside SR window
  - Data-driven Z+X: same DATA CR events with fake-rate weights (weight_zx)
  - MC irreducible: qqZZ + ggZZ CR events with weight_nominal, m4l outside SR window

Data and MC are loaded per-dataset from the CR output directory
(not from the merged parquet, which has no dataset label).

Usage:
    python scripts/validate_zx_sideband_features.py \\
        --cr-input  /eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_CR \\
        --fake-rates-dir /eos/user/s/snandaku/higgscharm/outputs/fake_rates \\
        --output    /eos/home-s/snandaku/b-hive_ttcc/docs/plots/zx_validation
"""

import os
import sys
import argparse
import pickle
import glob
import logging

import numpy as np
import pandas as pd
import awkward as ak
import matplotlib.pyplot as plt
import mplhep as hep

plt.style.use(hep.cms.style.CMS)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

ERAS = ['2022postEE', '2022preEE', '2023preBPix', '2023postBPix']

# Mass sideband: outside the SR window [90, 160]
M4L_SIDEBAND_LO = 70.0   # lower edge (avoid Z peak at 91 if needed)
M4L_SR_LO       = 90.0
M4L_SR_HI       = 160.0
M4L_SIDEBAND_HI = 200.0  # upper edge

# Dataset name patterns for each category
DATA_PREFIXES = ['Muon', 'DoubleMuon', 'EGamma', 'DoubleEGamma', 'MuonEG']
IRRED_PREFIXES = ['ZZto4L', 'GluGlutoContinto2Z', 'GluGluToContinto2Z']
REDUC_PREFIXES = ['DYJetsToLL_10to50', 'DYJetsToLL_50', 'WZto3LNu', 'TTto2L2Nu']

# Features to plot (use 3P1F column names as primary; script maps to sideband-agnostic names)
FEATURES = [
    ('m4l_3p1f',       r'$m_{4\ell}$ [GeV]',               np.linspace(70, 200, 27)),
    ('pT_4l_3p1f',     r'$p_T^{4\ell}$ [GeV]',             np.linspace(0, 200, 21)),
    ('z1_mass_3p1f',   r'$m_{Z1}$ [GeV]',                   np.linspace(40, 120, 21)),
    ('z2_mass_3p1f',   r'$m_{Z2}$ [GeV]',                   np.linspace(0, 120, 21)),
    ('z1_pt_3p1f',     r'$p_T^{Z1}$ [GeV]',                 np.linspace(0, 200, 21)),
    ('z2_pt_3p1f',     r'$p_T^{Z2}$ [GeV]',                 np.linspace(0, 150, 21)),
    ('deltaR_ZZ_3p1f', r'$\Delta R(Z_1, Z_2)$',             np.linspace(0, 5, 21)),
    ('l3_pt_3p1f',     r'Failing lepton $p_T$ [GeV]',        np.linspace(0, 80, 21)),
    ('jet_ht',         r'$H_T^{\rm jets}$ [GeV]',            np.linspace(0, 300, 21)),
    ('cjet_HT',        r'$H_T^{c\rm{-jet}}$ [GeV]',          np.linspace(0, 200, 21)),
]


# ---------------------------------------------------------------------------
# Fake-rate utilities
# ---------------------------------------------------------------------------

def load_fake_rates(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def get_fake_rate_array(pt, eta, pdgid, fake_rates):
    pt    = np.asarray(pt,    dtype=float)
    eta   = np.abs(np.asarray(eta,   dtype=float))
    pdgid = np.abs(np.asarray(pdgid, dtype=int))
    f = np.zeros(len(pt))
    for flav, mask_val in [('electron', 11), ('muon', 13)]:
        mask = pdgid == mask_val
        if not mask.any():
            continue
        fr = fake_rates[flav]
        pt_bin  = np.clip(np.searchsorted(fr['pt_edges'],  pt[mask])  - 1, 0, len(fr['pt_edges'])  - 2)
        eta_bin = np.clip(np.searchsorted(fr['eta_edges'], eta[mask]) - 1, 0, len(fr['eta_edges']) - 2)
        f[mask] = fr['fake_rate'][pt_bin, eta_bin]
    return f


def compute_fr_weight_3p1f(df, fake_rates):
    l3_tight = df['l3_istight_3p1f'].values.astype(bool)
    pt    = np.where(~l3_tight, df['l3_pt_3p1f'].values,     df['l4_pt_3p1f'].values)
    eta   = np.where(~l3_tight, df['l3_eta_3p1f'].values,    df['l4_eta_3p1f'].values)
    pdgid = np.where(~l3_tight, df['l3_pdgid_3p1f'].values,  df['l4_pdgid_3p1f'].values)
    f = np.clip(get_fake_rate_array(pt, eta, pdgid, fake_rates), 0.0, 0.999)
    return np.where(f > 0, f / (1.0 - f), 0.0)


# ---------------------------------------------------------------------------
# Parquet loading
# ---------------------------------------------------------------------------

NEEDED_3P1F = [
    'm4l_3p1f', 'pT_4l_3p1f', 'z1_mass_3p1f', 'z2_mass_3p1f',
    'z1_pt_3p1f', 'z1_eta_3p1f', 'z2_pt_3p1f', 'z2_eta_3p1f',
    'deltaR_ZZ_3p1f', 'l3_pt_3p1f', 'l3_eta_3p1f', 'l3_pdgid_3p1f', 'l3_istight_3p1f',
    'l4_pt_3p1f', 'l4_eta_3p1f', 'l4_pdgid_3p1f', 'l4_istight_3p1f',
    'jet_ht', 'cjet_HT', 'nSV', 'weight_nominal',
]


def _flatten_fields(arr, needed):
    flat = {}
    for field in needed:
        if field not in arr.fields:
            continue
        col = arr[field]
        flat[field] = ak.to_numpy(ak.flatten(col, axis=1) if col.ndim > 1 else col)
    return flat


def load_datasets_by_prefix(cr_input_dir, era, prefixes, category='CR_3P1F', max_files=None):
    """Load parquet files matching any of the given dataset-name prefixes."""
    frames = []
    for prefix in prefixes:
        pattern = os.path.join(cr_input_dir, era, f'{prefix}*', category, '*.parquet')
        files = sorted(glob.glob(pattern))
        if max_files:
            files = files[:max_files]
        if not files:
            continue
        logging.info(f'  {prefix}: {len(files)} files')
        chunks = []
        for fpath in files:
            try:
                arr = ak.from_parquet(fpath)
                flat = _flatten_fields(arr, NEEDED_3P1F)
                if flat and len(next(iter(flat.values()))) > 0:
                    chunks.append(pd.DataFrame(flat))
            except Exception as e:
                logging.debug(f'    Skip {fpath}: {e}')
        if chunks:
            frames.append(pd.concat(chunks, ignore_index=True))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def apply_sideband(df, m4l_col='m4l_3p1f'):
    if m4l_col not in df.columns:
        return df
    m4l = df[m4l_col].values
    mask = ((m4l >= M4L_SIDEBAND_LO) & (m4l < M4L_SR_LO)) | \
           ((m4l > M4L_SR_HI) & (m4l <= M4L_SIDEBAND_HI))
    return df[mask].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _fill_sideband_axes(ax, ax_r, df_data, w_data, x_zx, w_zx, x_irred, w_irred,
                         col, xlabel, bins, yield_data, yield_irred, yield_zx,
                         show_legend=True):
    """
    Fill one sideband feature's main panel (ax) and ratio panel (ax_r).
    Ratio = DATA / (MC irreducible + data-driven Z+X).
    """
    x_data = df_data[col].values if col in df_data.columns else np.array([])

    h_data,  _ = np.histogram(x_data,  bins=bins, weights=w_data[:len(x_data)])
    h_zx,    _ = np.histogram(x_zx,    bins=bins, weights=w_zx)    if len(x_zx)    else (np.zeros(len(bins)-1), None)
    h_irred, _ = np.histogram(x_irred, bins=bins, weights=w_irred) if len(x_irred) else (np.zeros(len(bins)-1), None)

    h_pred = h_irred + h_zx

    norm_d = h_data.sum() or 1.0
    norm_p = h_pred.sum() or 1.0
    h_data_n  = h_data  / norm_d
    h_irred_n = h_irred / norm_p
    h_pred_n  = h_pred  / norm_p

    cx    = 0.5 * (bins[:-1] + bins[1:])
    err_n = np.sqrt(h_data + 0.01) / norm_d

    ax.errorbar(cx, h_data_n, yerr=err_n,
                fmt='ko', markersize=4,
                label=f'Data CR sideband (N={yield_data:.0f})', zorder=3)
    ax.stairs(h_irred_n, bins, fill=True, color='steelblue', alpha=0.6,
              label=f'MC irred. qqZZ+ggZZ ({yield_irred:.0f} wtd)')
    ax.stairs(h_pred_n,  bins, fill=True, color='darkorange', alpha=0.5,
              label=f'+ Data-driven Z+X ({yield_zx:.0f} wtd)')
    ax.step(bins[:-1], h_pred_n, where='post', color='red', lw=1.5, alpha=0.8)

    ax.set_ylabel('Norm. events / bin', fontsize=10)
    ax.set_xlim(bins[0], bins[-1])
    ax.set_ylim(bottom=0)
    plt.setp(ax.get_xticklabels(), visible=False)
    if show_legend:
        ax.legend(fontsize=7, loc='upper right')

    # ── Ratio panel: DATA / prediction ───────────────────────────────────────
    with np.errstate(divide='ignore', invalid='ignore'):
        ratio     = np.where(h_pred_n > 0, h_data_n / h_pred_n, np.nan)
        ratio_err = np.where(h_pred_n > 0, err_n    / h_pred_n, np.nan)

    ax_r.axhline(1.0, color='black', linestyle='--', linewidth=1.2)
    ax_r.axhline(0.5, color='gray',  linestyle=':',  linewidth=0.8)
    ax_r.axhline(2.0, color='gray',  linestyle=':',  linewidth=0.8)
    ax_r.errorbar(cx, ratio, yerr=ratio_err,
                  fmt='ko', markersize=3, linewidth=1.0)
    ax_r.set_xlabel(xlabel, fontsize=11)
    ax_r.set_ylabel('Data /\nPred.', fontsize=9)
    ax_r.set_ylim(bottom=0)
    ax_r.grid(True, alpha=0.2)


def plot_sideband_features(df_data, df_zx, df_irred, output_dir, label):
    """
    Normalised shape comparison: DATA vs (MC irreducible + data-driven Z+X).
    Each feature has a main panel + ratio (DATA/prediction) panel below.
    Saves:
      - Full combined figure
      - Per-row figures (row1: first 3, row2: next 3, bot: remaining)
    """
    os.makedirs(output_dir, exist_ok=True)

    w_data  = np.ones(len(df_data))
    w_zx    = df_zx['weight_zx'].values if 'weight_zx' in df_zx.columns else \
              np.zeros(len(df_zx))
    w_irred = df_irred['weight_nominal'].fillna(0.0).values if len(df_irred) > 0 else np.array([])

    yield_data  = float(w_data.sum())
    yield_zx    = float(w_zx.sum())
    yield_irred = float(w_irred.sum()) if len(w_irred) else 0.0

    x_zx_dict    = {col: df_zx[col].values    if col in df_zx.columns    else np.array([]) for col, _, _ in FEATURES}
    x_irred_dict = {col: df_irred[col].values  if col in df_irred.columns else np.array([]) for col, _, _ in FEATURES}

    n_feat = len(FEATURES)
    n_cols = 3
    n_feat_rows = int(np.ceil(n_feat / n_cols))

    # Height ratios: alternating [3, 1] for main + ratio per feature row
    hr = []
    for _ in range(n_feat_rows):
        hr += [3, 1]

    # ── Full combined figure ──────────────────────────────────────────────────
    fig = plt.figure(figsize=(n_cols * 6, n_feat_rows * 5.5))
    gs  = fig.add_gridspec(n_feat_rows * 2, n_cols,
                           height_ratios=hr, hspace=0.08, wspace=0.30)

    for i, (col, xlabel, bins) in enumerate(FEATURES):
        vis_row = i // n_cols
        col_pos = i % n_cols
        gs_row  = vis_row * 2

        if col not in df_data.columns:
            continue

        ax   = fig.add_subplot(gs[gs_row,     col_pos])
        ax_r = fig.add_subplot(gs[gs_row + 1, col_pos], sharex=ax)

        _fill_sideband_axes(ax, ax_r, df_data, w_data,
                             x_zx_dict[col], w_zx,
                             x_irred_dict[col], w_irred,
                             col, xlabel, bins,
                             yield_data, yield_irred, yield_zx,
                             show_legend=(i == 0))

    if fig.axes:
        hep.cms.label('Preliminary', data=True, ax=fig.axes[0], fontsize=11)

    fig.suptitle(
        f'Z+X validation (2b): sideband feature shapes — {label}\n'
        f'Ratio = Data / (MC irred. + data-driven Z+X)',
        fontsize=12, y=1.01,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    safe = label.replace(' ', '_')
    out = os.path.join(output_dir, f'zx_sideband_features_{safe}.png')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Saved: {out}')

    # ── Per-row figures (for slides) ─────────────────────────────────────────
    row_names = ['row1', 'row2', 'bot', 'row4']
    for row_i in range(n_feat_rows):
        row_feats = FEATURES[row_i * n_cols: (row_i + 1) * n_cols]
        n_row_cols = len(row_feats)

        fig_r = plt.figure(figsize=(n_row_cols * 6, 5.5))
        gs_r  = fig_r.add_gridspec(2, n_row_cols,
                                   height_ratios=[3, 1], hspace=0.08, wspace=0.30)

        for j, (col, xlabel, bins) in enumerate(row_feats):
            if col not in df_data.columns:
                continue
            ax   = fig_r.add_subplot(gs_r[0, j])
            ax_r = fig_r.add_subplot(gs_r[1, j], sharex=ax)

            _fill_sideband_axes(ax, ax_r, df_data, w_data,
                                 x_zx_dict[col], w_zx,
                                 x_irred_dict[col], w_irred,
                                 col, xlabel, bins,
                                 yield_data, yield_irred, yield_zx,
                                 show_legend=(j == 0))

        if fig_r.axes:
            hep.cms.label('Preliminary', data=True, ax=fig_r.axes[0], fontsize=11)

        row_tag  = row_names[row_i] if row_i < len(row_names) else f'row{row_i+1}'
        row_path = os.path.join(output_dir, f'zx_sideband_features_{safe}_{row_tag}.png')
        plt.tight_layout()
        fig_r.savefig(row_path, dpi=150, bbox_inches='tight')
        plt.close(fig_r)
        logging.info(f'Saved row: {row_path}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Validation 2b: MVA input features in mass sideband')
    parser.add_argument('--cr-input',       required=True,
                        help='hplusc_mva_4class_CR output directory')
    parser.add_argument('--fake-rates-dir', required=True,
                        help='Directory with fake_rates/<era>/fake_rates.pkl')
    parser.add_argument('--output',         required=True,
                        help='Output directory for plots')
    parser.add_argument('--eras', nargs='+', default=ERAS)
    parser.add_argument('--max-files', type=int, default=None,
                        help='Cap per-dataset file count (for quick testing)')
    args = parser.parse_args()

    data_all  = []
    zx_all    = []
    irred_all = []

    for era in args.eras:
        logging.info(f'\n========== {era} ==========')

        fr_path = os.path.join(args.fake_rates_dir, era, 'fake_rates.pkl')
        if not os.path.exists(fr_path):
            logging.warning(f'No fake rates at {fr_path}, skipping {era}')
            continue
        fake_rates = load_fake_rates(fr_path)

        # ── DATA ──────────────────────────────────────────────────────────────
        logging.info('  Loading DATA CR 3P1F...')
        df_data = load_datasets_by_prefix(args.cr_input, era, DATA_PREFIXES,
                                          max_files=args.max_files)
        df_data = apply_sideband(df_data)
        logging.info(f'  DATA sideband events: {len(df_data):,}')

        # ── Data-driven Z+X (DATA CR events with fake-rate weights) ──────────
        #    Re-use the already loaded DATA df, apply FR weights
        if len(df_data) > 0 and 'l3_istight_3p1f' in df_data.columns:
            fr_w = compute_fr_weight_3p1f(df_data, fake_rates)
            df_zx = df_data.copy()
            df_zx['weight_zx'] = fr_w
        else:
            df_zx = df_data.copy()
            df_zx['weight_zx'] = 0.0
        logging.info(f'  Data-driven Z+X yield in sideband: {df_zx["weight_zx"].sum():.1f}')

        # ── MC irreducible ─────────────────────────────────────────────────────
        logging.info('  Loading MC irreducible CR 3P1F...')
        df_irred = load_datasets_by_prefix(args.cr_input, era, IRRED_PREFIXES,
                                           max_files=args.max_files)
        df_irred = apply_sideband(df_irred)
        if len(df_irred) > 0 and 'weight_nominal' in df_irred.columns:
            w_irred = df_irred['weight_nominal'].fillna(0.0).sum()
        else:
            w_irred = 0.0
        logging.info(f'  MC irred sideband weighted yield: {w_irred:.1f}')

        # ── Per-era plots ──────────────────────────────────────────────────────
        if len(df_data) > 0:
            plot_sideband_features(df_data, df_zx, df_irred, args.output, era)

        data_all.append(df_data)
        zx_all.append(df_zx)
        irred_all.append(df_irred)

    # ── Combined all eras ──────────────────────────────────────────────────────
    if data_all:
        df_data_all  = pd.concat(data_all,  ignore_index=True)
        df_zx_all    = pd.concat(zx_all,    ignore_index=True)
        df_irred_all = pd.concat(irred_all, ignore_index=True)
        logging.info(f'\nCombined: data={len(df_data_all):,}  '
                     f'irred={len(df_irred_all):,}  '
                     f'zx_yield={df_zx_all["weight_zx"].sum():.0f}')
        plot_sideband_features(df_data_all, df_zx_all, df_irred_all,
                               args.output, 'all_eras')

    logging.info('\nDone.')


if __name__ == '__main__':
    main()
