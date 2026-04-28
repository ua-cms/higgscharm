#!/usr/bin/env python3
"""
Validation (2c): Data-driven Z+X vs MC reducible (DY/WZ/tt) feature shape comparison.

Loads both:
  - Data-driven Z+X: fake-rate-weighted DATA CR events (already stored in zx_*_mva.parquet)
  - MC reducible:    fake-rate-weighted MC CR events (DY/WZ/tt from hplusc_mva_4class_CR)

Compares key kinematic feature distributions, normalised to unit area.
A ratio panel (data-driven / MC reducible) is drawn below each feature.

Usage:
    python scripts/validate_zx_reducible_features.py \\
        --zx-input      /eos/user/s/snandaku/Analysis/zx_background_mva \\
        --cr-input      /eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_CR \\
        --fake-rates-dir /eos/user/s/snandaku/higgscharm/outputs/fake_rates \\
        --output        /eos/home-s/snandaku/b-hive_ttcc/docs/plots/zx_validation
"""

import os
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

ERAS = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']

MC_REDUCIBLE_PREFIXES = ['DYJetsToLL_10to50', 'DYJetsToLL_50', 'WZto3LNu', 'TTto2L2Nu']
MC_REDUCIBLE_LABELS = {
    'DYJetsToLL': 'DY+jets',
    'WZto3LNu':   'WZ',
    'TTto2L2Nu':  'ttbar',
}

FEATURES_3P1F = {
    'm4l_3p1f':    (r'$m_{4\ell}$ [GeV]',   np.linspace(0, 250, 26)),
    'pT_4l_3p1f':  (r'$p_T^{4\ell}$ [GeV]', np.linspace(0, 200, 21)),
    'z1_mass_3p1f':(r'$m_{Z1}$ [GeV]',       np.linspace(40, 120, 21)),
    'z2_mass_3p1f':(r'$m_{Z2}$ [GeV]',       np.linspace(0, 120, 21)),
}
FEATURES_2P2F = {
    'm4l_2p2f':    (r'$m_{4\ell}$ [GeV]',   np.linspace(0, 250, 26)),
    'pT_4l_2p2f':  (r'$p_T^{4\ell}$ [GeV]', np.linspace(0, 200, 21)),
    'z1_mass_2p2f':(r'$m_{Z1}$ [GeV]',       np.linspace(40, 120, 21)),
    'z2_mass_2p2f':(r'$m_{Z2}$ [GeV]',       np.linspace(0, 120, 21)),
}

PROCESSES = ['DY+jets', 'WZ', 'ttbar']
COLORS    = ['#4C72B0', '#DD8452', '#55A868']


# ---------------------------------------------------------------------------
# Fake-rate utilities
# ---------------------------------------------------------------------------

def load_fake_rates(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def get_fake_rate_array(pt, eta, pdgid, fake_rates):
    pt    = np.asarray(pt,    dtype=float)
    eta   = np.abs(np.asarray(eta,  dtype=float))
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
    pt    = np.where(~l3_tight, df['l3_pt_3p1f'].values,  df['l4_pt_3p1f'].values)
    eta   = np.where(~l3_tight, df['l3_eta_3p1f'].values, df['l4_eta_3p1f'].values)
    pdgid = np.where(~l3_tight, df['l3_pdgid_3p1f'].values, df['l4_pdgid_3p1f'].values)
    f = np.clip(get_fake_rate_array(pt, eta, pdgid, fake_rates), 0.0, 0.999)
    return np.where(f > 0, f / (1.0 - f), 0.0)


def compute_fr_weight_2p2f(df, fake_rates):
    f1 = np.clip(get_fake_rate_array(df['l3_pt_2p2f'].values, df['l3_eta_2p2f'].values,
                                     df['l3_pdgid_2p2f'].values, fake_rates), 0.0, 0.999)
    f2 = np.clip(get_fake_rate_array(df['l4_pt_2p2f'].values, df['l4_eta_2p2f'].values,
                                     df['l4_pdgid_2p2f'].values, fake_rates), 0.0, 0.999)
    return np.where((f1 > 0) & (f2 > 0), (f1 * f2) / ((1 - f1) * (1 - f2)), 0.0)


# ---------------------------------------------------------------------------
# MC reducible loading
# ---------------------------------------------------------------------------

def _flatten_parquet_fields(arr, needed):
    flat = {}
    for field in needed:
        if field not in arr.fields:
            continue
        col = arr[field]
        flat[field] = ak.to_numpy(ak.flatten(col, axis=1) if col.ndim > 1 else col)
    return flat


def load_mc_reducible_cr(cr_input_dir, era, category, max_files_per_process=None):
    suffix = '_3p1f' if category == 'CR_3P1F' else '_2p2f'
    if category == 'CR_3P1F':
        needed = [
            f'm4l{suffix}', f'pT_4l{suffix}', f'z1_mass{suffix}', f'z2_mass{suffix}',
            f'l3_pt{suffix}', f'l3_eta{suffix}', f'l3_pdgid{suffix}', f'l3_istight{suffix}',
            f'l4_pt{suffix}', f'l4_eta{suffix}', f'l4_pdgid{suffix}', f'l4_istight{suffix}',
            'weight_nominal',
        ]
    else:
        needed = [
            f'm4l{suffix}', f'pT_4l{suffix}', f'z1_mass{suffix}', f'z2_mass{suffix}',
            f'l3_pt{suffix}', f'l3_eta{suffix}', f'l3_pdgid{suffix}',
            f'l4_pt{suffix}', f'l4_eta{suffix}', f'l4_pdgid{suffix}',
            'weight_nominal',
        ]

    frames = []
    for prefix in MC_REDUCIBLE_PREFIXES:
        pattern = os.path.join(cr_input_dir, era, f'{prefix}*', category, '*.parquet')
        files = sorted(glob.glob(pattern))
        if not files:
            logging.warning(f'  No files for {prefix} {era} {category}')
            continue
        if max_files_per_process and len(files) > max_files_per_process:
            files = files[:max_files_per_process]
        logging.info(f'  Loading {len(files)} files for {prefix} ({era}/{category})')
        chunks = []
        for fpath in files:
            try:
                arr = ak.from_parquet(fpath)
                flat = _flatten_parquet_fields(arr, needed)
                if flat and len(next(iter(flat.values()))) > 0:
                    chunks.append(pd.DataFrame(flat))
            except Exception as e:
                logging.debug(f'    Skip {fpath}: {e}')
        if not chunks:
            continue
        df = pd.concat(chunks, ignore_index=True)
        proc = next((v for k, v in MC_REDUCIBLE_LABELS.items() if prefix.startswith(k)),
                    prefix.split('_')[0])
        df['process'] = proc
        frames.append(df)
        logging.info(f'    {proc}: {len(df):,} events')

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Per-feature plot helper (main panel + ratio panel)
# ---------------------------------------------------------------------------

def _fill_feature_axes(ax, ax_r, df_dd, w_dd, df_mc, w_mc, col, xlabel, bins,
                       show_legend=True, show_xlabel=True):
    """
    Fill one feature's main panel (ax) and ratio panel (ax_r).
    Both distributions are normalised to unit area.
    Ratio = data-driven / MC reducible (normalised).
    """
    # ── Data-driven Z+X ───────────────────────────────────────────────────────
    vals_dd = df_dd[col].values if col in df_dd.columns else np.array([])
    h_dd, _ = np.histogram(vals_dd, bins=bins, weights=w_dd[:len(vals_dd)] if len(vals_dd) else np.array([]))
    norm_dd  = h_dd.sum() or 1.0

    ax.step(np.append(bins[:-1], bins[-1]),
            np.append(h_dd / norm_dd, 0),
            where='post', color='black', linewidth=2.0,
            label=f'Data-driven Z+X ({w_dd.sum():.0f} wtd)')

    # ── MC reducible — stacked by process ────────────────────────────────────
    centers  = 0.5 * (bins[:-1] + bins[1:])
    width    = np.diff(bins)
    total_mc = np.zeros(len(bins) - 1)
    proc_hists = {}

    for proc, color in zip(PROCESSES, COLORS):
        mask = df_mc['process'].values == proc
        if not mask.any():
            continue
        h_proc, _ = np.histogram(
            df_mc.loc[mask, col].values if col in df_mc.columns else np.array([]),
            bins=bins, weights=w_mc[mask])
        proc_hists[proc] = h_proc
        total_mc += h_proc

    norm_mc  = total_mc.sum() or 1.0
    bottoms  = np.zeros(len(bins) - 1)
    for proc, color in zip(PROCESSES, COLORS):
        if proc not in proc_hists:
            continue
        h_norm = proc_hists[proc] / norm_mc
        ax.bar(centers, np.maximum(h_norm, 0), width=width,
               bottom=np.maximum(bottoms, 0),
               color=color, alpha=0.7, label=proc)
        bottoms += h_norm

    ax.set_ylim(bottom=0)
    ax.set_ylabel('Norm. events / bin', fontsize=11)
    plt.setp(ax.get_xticklabels(), visible=False)
    if show_legend:
        ax.legend(fontsize=8, framealpha=0.8, loc='upper right')

    # ── Ratio panel: data-driven / MC reducible ───────────────────────────────
    with np.errstate(divide='ignore', invalid='ignore'):
        ratio = np.where(total_mc > 0,
                         (h_dd / norm_dd) / (total_mc / norm_mc),
                         np.nan)

    ax_r.axhline(1.0, color='black', linestyle='--', linewidth=1.2)
    ax_r.axhline(0.5, color='gray',  linestyle=':',  linewidth=0.8)
    ax_r.axhline(2.0, color='gray',  linestyle=':',  linewidth=0.8)
    ax_r.step(np.append(bins[:-1], bins[-1]),
              np.append(ratio, ratio[-1]),
              where='post', color='black', linewidth=1.5)
    ax_r.set_ylim(bottom=0)
    ax_r.set_ylabel('DD / MC', fontsize=9)
    ax_r.grid(True, alpha=0.2)
    if show_xlabel:
        ax_r.set_xlabel(xlabel, fontsize=12)


# ---------------------------------------------------------------------------
# Main plotting function — full grid + per-row images
# ---------------------------------------------------------------------------

def plot_feature_comparison(df_dd, w_dd, df_mc, w_mc, features, cr_label, out_path):
    """
    Compare data-driven Z+X vs MC reducible for each feature.
    Each feature has a main panel (distributions) and a ratio panel below.
    Saves:
      - Full combined figure  (out_path)
      - Per-row figures       (out_path with _row1, _row2, ... suffix)
    """
    items  = list(features.items())
    n_feat = len(items)
    ncols  = 2
    n_feat_rows = (n_feat + ncols - 1) // ncols   # number of visual rows

    # Height ratios: [main, ratio] per feature row
    hr = []
    for _ in range(n_feat_rows):
        hr += [3, 1]

    fig = plt.figure(figsize=(ncols * 7, n_feat_rows * 5.5))
    gs  = fig.add_gridspec(n_feat_rows * 2, ncols,
                           height_ratios=hr, hspace=0.08, wspace=0.30)

    for i, (col, (xlabel, bins)) in enumerate(items):
        vis_row = i // ncols        # which visual row (0-indexed)
        col_pos = i % ncols
        gs_row  = vis_row * 2       # gridspec row for main panel

        ax   = fig.add_subplot(gs[gs_row,     col_pos])
        ax_r = fig.add_subplot(gs[gs_row + 1, col_pos], sharex=ax)

        # Only show legend on first subplot
        show_legend = (i == 0)
        _fill_feature_axes(ax, ax_r, df_dd, w_dd, df_mc, w_mc,
                           col, xlabel, bins,
                           show_legend=show_legend, show_xlabel=True)

    axes_flat = fig.axes
    if axes_flat:
        hep.cms.label('Simulation Preliminary', data=False,
                      ax=axes_flat[0], fontsize=11)

    fig.suptitle(
        f'Z+X validation (2c): data-driven vs MC reducible — {cr_label}\n'
        f'Ratio = data-driven / MC reducible (normalised)',
        fontsize=13, y=1.01,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.98])
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Saved full figure: {out_path}')

    # ── Per-row figures (for slides) ─────────────────────────────────────────
    base = out_path.replace('_all_eras.png', '')
    for row_i in range(n_feat_rows):
        row_items = items[row_i * ncols: (row_i + 1) * ncols]
        n_row_cols = len(row_items)

        fig_r = plt.figure(figsize=(n_row_cols * 7, 5.5))
        gs_r  = fig_r.add_gridspec(2, n_row_cols,
                                   height_ratios=[3, 1], hspace=0.08, wspace=0.30)

        for j, (col, (xlabel, bins)) in enumerate(row_items):
            ax   = fig_r.add_subplot(gs_r[0, j])
            ax_r = fig_r.add_subplot(gs_r[1, j], sharex=ax)
            _fill_feature_axes(ax, ax_r, df_dd, w_dd, df_mc, w_mc,
                               col, xlabel, bins,
                               show_legend=(j == 0), show_xlabel=True)

        if fig_r.axes:
            hep.cms.label('Simulation Preliminary', data=False,
                          ax=fig_r.axes[0], fontsize=11)

        row_path = f'{base}_row{row_i + 1}_all_eras.png'
        plt.tight_layout()
        fig_r.savefig(row_path, dpi=150, bbox_inches='tight')
        plt.close(fig_r)
        logging.info(f'Saved row figure: {row_path}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--zx-input',       required=True,
                        help='Dir with zx_*_mva.parquet files (per-era subdirs)')
    parser.add_argument('--cr-input',       required=True,
                        help='hplusc_mva_4class_CR base dir')
    parser.add_argument('--fake-rates-dir', required=True,
                        help='Dir with fake_rates/<era>/fake_rates.pkl files')
    parser.add_argument('--output',         required=True)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    for cr_type, suffix, features in [
        ('CR_3P1F', '3p1f', FEATURES_3P1F),
        ('CR_2P2F', '2p2f', FEATURES_2P2F),
    ]:
        logging.info(f'\n=== {cr_type} ===')

        dd_frames, mc_frames = [], []
        dd_weights, mc_weights = [], []

        for era in ERAS:
            # Data-driven Z+X
            zx_path = os.path.join(args.zx_input, era, f'zx_{suffix}_{era}_mva.parquet')
            if not os.path.exists(zx_path):
                logging.warning(f'Missing {zx_path}')
                continue
            df_dd = pd.read_parquet(zx_path)
            w_col = 'weight_zx' if 'weight_zx' in df_dd.columns else 'zx_weight'
            w_dd  = df_dd[w_col].values * df_dd.get(
                'zx_sign', pd.Series(np.ones(len(df_dd)))).values
            cols_needed = list(features.keys())
            df_dd = df_dd[[c for c in cols_needed if c in df_dd.columns]].copy()
            dd_frames.append(df_dd)
            dd_weights.append(w_dd[:len(df_dd)])

            # MC reducible
            fr_path = os.path.join(args.fake_rates_dir, era, 'fake_rates.pkl')
            if not os.path.exists(fr_path):
                logging.warning(f'Missing fake rates {fr_path}')
                continue
            fake_rates = load_fake_rates(fr_path)

            max_f = 80 if cr_type == 'CR_2P2F' else None
            df_mc = load_mc_reducible_cr(args.cr_input, era, cr_type,
                                         max_files_per_process=max_f)
            if df_mc.empty:
                continue

            if cr_type == 'CR_3P1F':
                fr_w = compute_fr_weight_3p1f(df_mc, fake_rates)
            else:
                fr_w = compute_fr_weight_2p2f(df_mc, fake_rates)

            w_mc = df_mc['weight_nominal'].values * fr_w
            mc_frames.append(df_mc)
            mc_weights.append(w_mc)

        if not dd_frames or not mc_frames:
            logging.warning(f'No data for {cr_type}, skipping')
            continue

        df_dd_all = pd.concat(dd_frames, ignore_index=True)
        w_dd_all  = np.concatenate(dd_weights)
        df_mc_all = pd.concat(mc_frames, ignore_index=True)
        w_mc_all  = np.concatenate(mc_weights)

        logging.info(f'Data-driven Z+X: {w_dd_all.sum():.1f} weighted events '
                     f'({len(df_dd_all):,} raw)')
        logging.info(f'MC reducible:    {w_mc_all.sum():.1f} weighted events '
                     f'({len(df_mc_all):,} raw)')

        out_path = os.path.join(args.output,
                                f'zx_dd_vs_mc_reducible_{suffix}_all_eras.png')
        cr_label = '3P1F' if cr_type == 'CR_3P1F' else '2P2F'
        plot_feature_comparison(df_dd_all, w_dd_all, df_mc_all, w_mc_all,
                                features, cr_label, out_path)


if __name__ == '__main__':
    main()
