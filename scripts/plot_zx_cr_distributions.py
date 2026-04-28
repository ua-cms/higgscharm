#!/usr/bin/env python3
"""
Plot Z+X control region feature distributions (3P1F and 2P2F) from DATA.

Shows DATA events in 3P1F and 2P2F control regions, all eras combined or
per era, for kinematic validation of the fake rate method.

Mass window 100-150 GeV is applied, matching the signal region definition.
No c-jet requirement is applied (correct CMS H→ZZ methodology).

Usage:
    python plot_zx_cr_distributions.py \
        --input /eos/user/s/snandaku/Analysis/zx_background \
        --output docs/plots/zx_cr_distributions
"""

import os
import argparse
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplhep as hep

hep.style.use("CMS")

ERAS = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']
ERA_LABELS = {
    '2022preEE':    '2022 preEE (7.98 fb⁻¹)',
    '2022postEE':   '2022 postEE (26.67 fb⁻¹)',
    '2023preBPix':  '2023 preBPix (17.79 fb⁻¹)',
    '2023postBPix': '2023 postBPix (9.53 fb⁻¹)',
}

# Colours per CR
COLOR_3P1F = '#3274A1'   # CMS blue
COLOR_2P2F = '#E1812C'   # CMS orange

# ── Feature definitions ──────────────────────────────────────────────────────
# (col_3p1f, col_2p2f, xlabel, bins)
FEATURES = [
    ('m4l_3p1f',      'm4l_2p2f',      r'$m_{4\ell}$ [GeV]',             np.linspace(70, 400, 34)),
    ('z1_mass_3p1f',  'z1_mass_2p2f',  r'$m_{Z_1}$ [GeV]',               np.linspace(40, 120, 21)),
    ('z2_mass_3p1f',  'z2_mass_2p2f',  r'$m_{Z_2}$ [GeV]',               np.linspace(12, 120, 22)),
    ('pT_4l_3p1f',    'pT_4l_2p2f',    r'$p_T^{4\ell}$ [GeV]',           np.linspace(0, 300, 31)),
    ('z1_pt_3p1f',    'z1_pt_2p2f',    r'$p_T^{Z_1}$ [GeV]',             np.linspace(0, 250, 26)),
    ('z2_pt_3p1f',    'z2_pt_2p2f',    r'$p_T^{Z_2}$ [GeV]',             np.linspace(0, 200, 21)),
    ('deltaR_ZZ_3p1f','deltaR_ZZ_2p2f',r'$\Delta R(Z_1,Z_2)$',           np.linspace(0, 6, 25)),
    ('l3_pt_3p1f',    'l3_pt_2p2f',    r'$p_T(\ell_3)$ [GeV]',           np.array([5,7,10,15,20,30,40,50,80])),
    ('l4_pt_3p1f',    'l4_pt_2p2f',    r'$p_T(\ell_4)$ [GeV]',           np.array([5,7,10,15,20,30,40,50,80])),
    ('l3_eta_3p1f',   'l3_eta_2p2f',   r'$|\eta(\ell_3)|$',              np.linspace(0, 2.5, 21)),
    ('l4_eta_3p1f',   'l4_eta_2p2f',   r'$|\eta(\ell_4)|$',              np.linspace(0, 2.5, 21)),
]

# Separate per-CR only features
FEATURES_3P1F_ONLY = [
    ('l3_istight_3p1f', None, r'$\ell_3$ is tight (3P1F)', np.array([-0.5, 0.5, 1.5])),
    ('l4_istight_3p1f', None, r'$\ell_4$ is tight (3P1F)', np.array([-0.5, 0.5, 1.5])),
]

FEATURES_JET = [
    ('jet_multiplicity', r'Jet multiplicity',          np.arange(-0.5, 10.5, 1)),
    ('n_cjet',           r'c-jet multiplicity',         np.arange(-0.5, 8.5, 1)),
    ('jet_ht',           r'$H_T^{\rm jets}$ [GeV]',    np.linspace(0, 600, 31)),
    ('cjet_HT',          r'$H_T^{\rm c-jets}$ [GeV]',  np.linspace(0, 500, 26)),
    ('nSV',              r'Number of SVs',              np.arange(-0.5, 8.5, 1)),
]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _val(col):
    """Return numpy array, handling nested lists."""
    v = col.values
    if hasattr(v[0], '__len__'):
        return np.array([x[0] if len(x) > 0 else np.nan for x in v], dtype=float)
    return v.astype(float)


def load_weighted_parquets(input_dir, eras):
    """Load estimate_zx_background.py output parquets for all eras."""
    df3, df2 = [], []
    for era in eras:
        f3 = os.path.join(input_dir, era, f'zx_3p1f_{era}.parquet')
        f2 = os.path.join(input_dir, era, f'zx_2p2f_{era}.parquet')
        if os.path.exists(f3):
            d = pd.read_parquet(f3)
            d['era'] = era
            df3.append(d)
        if os.path.exists(f2):
            d = pd.read_parquet(f2)
            d['era'] = era
            df2.append(d)
    df3 = pd.concat(df3, ignore_index=True) if df3 else pd.DataFrame()
    df2 = pd.concat(df2, ignore_index=True) if df2 else pd.DataFrame()
    return df3, df2


def safe_col(df, col):
    """Return column values as float array, or None if not present."""
    if col not in df.columns:
        return None
    return _val(df[col])


def plot_comparison(ax, vals3, vals2, bins, xlabel, logy=False,
                    w3=None, w2=None, label3='3P1F DATA', label2='2P2F DATA',
                    mass_window=None):
    """Overlay 3P1F and 2P2F distributions (unweighted counts)."""
    kw = dict(histtype='step', linewidth=2)

    if vals3 is not None and len(vals3) > 0:
        v = vals3[np.isfinite(vals3)]
        ax.hist(v, bins=bins, weights=w3[np.isfinite(vals3)] if w3 is not None else None,
                color=COLOR_3P1F, label=label3, **kw)
    if vals2 is not None and len(vals2) > 0:
        v = vals2[np.isfinite(vals2)]
        ax.hist(v, bins=bins, weights=w2[np.isfinite(vals2)] if w2 is not None else None,
                color=COLOR_2P2F, label=label2, **kw)

    if mass_window is not None:
        ax.axvspan(mass_window[0], mass_window[1], alpha=0.08, color='green',
                   label=f'Signal window [{mass_window[0]},{mass_window[1]}] GeV')

    ax.set_xlabel(xlabel, fontsize=14)
    ax.set_ylabel('Events', fontsize=13)
    if logy:
        ax.set_yscale('log')
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)


def make_figure(title=''):
    fig, ax = plt.subplots(figsize=(8, 6))
    hep.cms.label("Preliminary", data=True, lumi=62.0, com=13.6, ax=ax, fontsize=13)
    if title:
        ax.set_title(title, fontsize=13)
    return fig, ax


def plot_all_features(df3, df2, output_dir, logy=False, use_weights=False):
    os.makedirs(output_dir, exist_ok=True)

    w3 = df3['zx_weight'].values if use_weights and 'zx_weight' in df3.columns else None
    w2 = np.abs(df2['zx_weight'].values) if use_weights and 'zx_weight' in df2.columns else None

    label3 = '3P1F (fake-rate weighted)' if use_weights else '3P1F DATA'
    label2 = '2P2F (fake-rate weighted)' if use_weights else '2P2F DATA'

    # ── Paired features (3P1F vs 2P2F same canvas) ───────────────────────────
    for col3, col2, xlabel, bins in FEATURES:
        v3 = safe_col(df3, col3)
        v2 = safe_col(df2, col2)

        is_m4l = 'm4l' in col3
        mw = (100, 150) if is_m4l else None

        fig, ax = make_figure()
        plot_comparison(ax, v3, v2, bins, xlabel, logy=logy,
                        w3=w3, w2=w2, label3=label3, label2=label2,
                        mass_window=mw)
        name = col3.replace('_3p1f', '')
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, f'zx_cr_{name}.png'), dpi=150)
        fig.savefig(os.path.join(output_dir, f'zx_cr_{name}.pdf'))
        plt.close(fig)
        print(f'  Saved: zx_cr_{name}.png')

    # ── 3P1F-only features ────────────────────────────────────────────────────
    for col3, _, xlabel, bins in FEATURES_3P1F_ONLY:
        v3 = safe_col(df3, col3)
        if v3 is None:
            continue
        fig, ax = make_figure()
        v = v3[np.isfinite(v3)]
        ax.hist(v, bins=bins, weights=w3[np.isfinite(v3)] if w3 is not None else None,
                color=COLOR_3P1F, histtype='step', linewidth=2, label=label3)
        ax.set_xlabel(xlabel, fontsize=14)
        ax.set_ylabel('Events', fontsize=13)
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)
        name = col3.replace('_3p1f', '')
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, f'zx_cr_{name}.png'), dpi=150)
        fig.savefig(os.path.join(output_dir, f'zx_cr_{name}.pdf'))
        plt.close(fig)
        print(f'  Saved: zx_cr_{name}.png')

    # ── Jet features (shared; one distribution covers both CRs) ──────────────
    for col, xlabel, bins in FEATURES_JET:
        v3 = safe_col(df3, col)
        v2 = safe_col(df2, col)

        fig, ax = make_figure()
        plot_comparison(ax, v3, v2, bins, xlabel, logy=logy,
                        w3=w3, w2=w2, label3=label3, label2=label2)
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, f'zx_cr_{col}.png'), dpi=150)
        fig.savefig(os.path.join(output_dir, f'zx_cr_{col}.pdf'))
        plt.close(fig)
        print(f'  Saved: zx_cr_{col}.png')

    # ── m4l per era (stacked) ─────────────────────────────────────────────────
    eras_present = sorted(df3['era'].unique()) if 'era' in df3.columns else []
    if eras_present:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=False)
        axes = axes.flatten()
        for i, era in enumerate(eras_present):
            ax = axes[i]
            d3 = df3[df3['era'] == era]
            d2 = df2[df2['era'] == era] if 'era' in df2.columns else df2

            bins = np.linspace(70, 400, 34)
            v3 = safe_col(d3, 'm4l_3p1f')
            v2 = safe_col(d2, 'm4l_2p2f')
            ww3 = d3['zx_weight'].values if use_weights and 'zx_weight' in d3.columns else None
            ww2 = np.abs(d2['zx_weight'].values) if use_weights and 'zx_weight' in d2.columns else None
            plot_comparison(ax, v3, v2, bins, r'$m_{4\ell}$ [GeV]',
                            w3=ww3, w2=ww2, label3='3P1F', label2='2P2F',
                            mass_window=(100, 150))
            ax.set_title(ERA_LABELS.get(era, era), fontsize=12)
            hep.cms.label("Preliminary", data=True, ax=ax, fontsize=10)

        fig.suptitle('Z+X CR m4l distribution per era', fontsize=14, y=1.01)
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, 'zx_cr_m4l_per_era.png'), dpi=150, bbox_inches='tight')
        fig.savefig(os.path.join(output_dir, 'zx_cr_m4l_per_era.pdf'), bbox_inches='tight')
        plt.close(fig)
        print('  Saved: zx_cr_m4l_per_era.png')

    print(f'\nAll plots saved to: {output_dir}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',  required=True,
                        help='Directory with zx_3p1f/zx_2p2f parquets (estimate_zx_background.py output)')
    parser.add_argument('--output', default='docs/plots/zx_cr_distributions',
                        help='Output directory for plots')
    parser.add_argument('--eras', nargs='+', default=ERAS)
    parser.add_argument('--logy', action='store_true')
    parser.add_argument('--weighted', action='store_true',
                        help='Use fake-rate weights (zx_weight column) for histograms')
    args = parser.parse_args()

    print('Loading parquets...')
    df3, df2 = load_weighted_parquets(args.input, args.eras)
    print(f'  3P1F events: {len(df3):,}   2P2F events: {len(df2):,}')

    if len(df3) == 0:
        print('No 3P1F events found — exiting.')
        return

    print(f'\nPlotting features to: {args.output}')
    plot_all_features(df3, df2, args.output, logy=args.logy, use_weights=args.weighted)


if __name__ == '__main__':
    main()
