#!/usr/bin/env python3
"""
Plot fake rates as CMS-style 1D fake rate vs pT, split by barrel/endcap.

Reads fake_rates.pkl produced by compute_fake_rates.py and produces
4-line plots (Barrel Uncorrected, Barrel Corrected, Endcap Uncorrected,
Endcap Corrected) using mplhep CMS style, mirroring pages 22-23 of
Presentation_HZZ_31_10_2025.pdf.

Eta bin definition (5 coarse bins after rebinning from 50):
  Barrel: |eta| in [0.0, 1.5]  → bins 0, 1, 2  (N_total-weighted average)
  Endcap: |eta| in [1.5, 2.5]  → bins 3, 4     (N_total-weighted average)

Usage:
    python plot_fake_rates_1d.py \
        --input /eos/user/s/snandaku/higgscharm/outputs/fake_rates \
        --output docs/plots/fake_rates
"""

import os
import pickle
import argparse
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplhep as hep

hep.style.use("CMS")

# ── Constants ─────────────────────────────────────────────────────────────────
ERAS = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']
ERA_LABELS = {
    '2022preEE':    '2022 preEE',
    '2022postEE':   '2022 postEE',
    '2023preBPix':  '2023 preBPix',
    '2023postBPix': '2023 postBPix',
}
# Integrated luminosity in fb⁻¹ per era (Run 3, 13.6 TeV)
ERA_LUMI = {
    '2022preEE':    7.98,
    '2022postEE':   26.67,
    '2023preBPix':  17.79,
    '2023postBPix': 9.53,
}

BARREL_BINS = [0, 1, 2]   # |eta| < 1.5
ENDCAP_BINS = [3, 4]       # 1.5 <= |eta| < 2.5

# HEP colour palette
C_BARREL = '#1f4e9e'   # Royal blue
C_ENDCAP  = '#cc0000'  # Bright red


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_fake_rates(pkl_path):
    """Load from .npz (numpy-version agnostic) or fall back to .pkl."""
    npz_path = pkl_path.replace('.pkl', '.npz')
    if os.path.exists(npz_path):
        raw = np.load(npz_path, allow_pickle=False)
        fr = {}
        for key in raw.files:
            parts = key.split('__')
            flavor, subkey, field = parts[0], parts[1], parts[2]
            if flavor not in fr:
                fr[flavor] = {}
            if subkey not in fr[flavor]:
                fr[flavor][subkey] = {}
            fr[flavor][subkey][field] = raw[key]
        return fr
    with open(pkl_path, 'rb') as f:
        return pickle.load(f)


def _region_average(fr_2d, err_2d, n_total_2d, eta_bins):
    """
    Weighted average of fake rate over the given eta bins (weighted by N_total).
    Bins with err >= 1.0 (sentinel for undefined/unstable) are excluded.
    Returns (fr_avg, err_avg, valid) arrays of shape (n_pt,).
    """
    w   = n_total_2d[:, eta_bins].copy()   # (n_pt, n_eta_region)
    fr  = fr_2d[:, eta_bins]
    err = err_2d[:, eta_bins]

    # Zero out weights for undefined bins (err=1.0 sentinel) or negative n_total
    undefined = (err >= 1.0) | (w < 0)
    w[undefined] = 0.0

    w_sum = w.sum(axis=1)
    valid = w_sum > 0

    fr_avg  = np.where(valid, (fr * w).sum(axis=1) / np.where(valid, w_sum, 1.0), 0.0)
    # Error propagation for weighted average: sigma_avg = sqrt(sum(sigma_i^2 * w_i^2)) / sum(w_i)
    err_wt  = np.where(undefined, 0.0, err)
    err_avg = np.where(
        valid,
        np.sqrt((err_wt**2 * w**2).sum(axis=1)) / np.where(valid, w_sum, 1.0),
        0.0,
    )
    return fr_avg, err_avg, valid


def _plot_four_lines(ax, fr_data, era, flavor):
    """
    Draw the 4 series (no connecting lines):
      Barrel Uncorrected  — blue filled circles
      Barrel Corrected    — blue open circles
      Endcap Uncorrected  — red  filled squares
      Endcap Corrected    — red  open squares
    """
    # prefer safe_corrected; fall back to corrected if absent (old pkl format)
    corr_key = 'safe_corrected' if 'safe_corrected' in fr_data else 'corrected'
    pt_edges   = fr_data['uncorrected']['pt_edges']
    pt_centers = 0.5 * (pt_edges[:-1] + pt_edges[1:])
    xerr       = 0.5 * (pt_edges[1:] - pt_edges[:-1])

    # (key, eta_bins, color, fillstyle, marker, label)
    configs = [
        ('uncorrected', BARREL_BINS, C_BARREL, 'full', 'o', 'Barrel Uncorrected'),
        ('uncorrected', ENDCAP_BINS, C_ENDCAP, 'full', 's', 'Endcap Uncorrected'),
        (corr_key,      BARREL_BINS, C_BARREL, 'none', 'o', 'Barrel Corrected'),
        (corr_key,      ENDCAP_BINS, C_ENDCAP, 'none', 's', 'Endcap Corrected'),
    ]

    for key, eta_bins, color, fillstyle, marker, label in configs:
        if key not in fr_data:
            continue
        d = fr_data[key]
        fr_avg, err_avg, valid = _region_average(
            d['fake_rate'], d['fake_rate_err'], d['n_total'], eta_bins
        )
        ax.errorbar(
            pt_centers[valid],
            fr_avg[valid],
            yerr=err_avg[valid],
            xerr=xerr[valid],
            fmt=marker,
            color=color,
            linestyle='none',
            markersize=6,
            markerfacecolor=color if fillstyle == 'full' else 'none',
            markeredgecolor=color,
            markeredgewidth=1.2,
            capsize=3,
            capthick=1.0,
            label=label,
        )

    # CMS label
    hep.cms.label(
        "Preliminary",
        data=True,
        lumi=ERA_LUMI[era],
        com=13.6,
        ax=ax,
        fontsize=12,
        loc=0,
    )

    ax.set_xlabel(r'$p_T\,[\mathrm{GeV}]$', loc='right')
    ax.set_ylabel('Fake Rate', loc='top')
    ax.set_ylim(bottom=0)
    ax.tick_params(direction='in', which='both', top=True, right=True)
    ax.legend(fontsize=9, framealpha=0.8, loc='upper right')


# ── Main plots ────────────────────────────────────────────────────────────────

def plot_all_eras_flavor(fake_rates_all, flavor, output_dir):
    """
    2×2 figure: one subplot per era, for a single flavor (electron or muon).
    Mimics pages 22-23 layout of Presentation_HZZ_31_10_2025.pdf.
    Shared y-axis across all 4 panels for easy era comparison.
    """
    # Fixed y-axis limits per flavor (covers corrected + uncorrected range)
    ylim = (0.0, 0.25) if flavor == 'electron' else (0.0, 0.80)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    for idx, era in enumerate(ERAS):
        ax = axes.flatten()[idx]
        if era not in fake_rates_all:
            ax.set_visible(False)
            continue

        fr_data = fake_rates_all[era][flavor]
        _plot_four_lines(ax, fr_data, era, flavor)
        ax.set_ylim(*ylim)

    plt.tight_layout()
    out = os.path.join(output_dir, f'fake_rate_1d_{flavor}_barrel_endcap.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {out}")


def plot_per_era(fake_rates_all, output_dir):
    """
    One figure per era: 2 subplots (electron top, muon bottom), 4 lines each.
    """
    for era in ERAS:
        if era not in fake_rates_all:
            continue

        fig, axes = plt.subplots(2, 1, figsize=(8, 10))

        for ax, flavor, title in zip(axes,
                                     ['electron', 'muon'],
                                     ['Electron', 'Muon']):
            fr_data = fake_rates_all[era][flavor]
            _plot_four_lines(ax, fr_data, era, flavor)
            ax.set_title(title, fontsize=12)


        plt.tight_layout()
        out = os.path.join(output_dir, f'fake_rate_1d_{era}.png')
        plt.savefig(out, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  Saved: {out}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',  required=True,
                        help='Directory containing <era>/fake_rates.pkl')
    parser.add_argument('--output', required=True,
                        help='Output directory for plots')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    fake_rates_all = {}
    for era in ERAS:
        pkl = os.path.join(args.input, era, 'fake_rates.pkl')
        if not os.path.exists(pkl):
            print(f"  Missing: {pkl}")
            continue
        fr = load_fake_rates(pkl)
        # Support both old pkl (no corrected/uncorrected keys) and new format
        for flavor in ['electron', 'muon']:
            if 'corrected' not in fr.get(flavor, {}):
                # Old pkl: wrap existing data as 'corrected', no uncorrected available
                fr[flavor] = {'corrected': fr[flavor]}
        fake_rates_all[era] = fr
        print(f"  Loaded: {era}")

    if not fake_rates_all:
        print("No fake rate files found. Check --input path.")
        return

    print("\nPlotting per-era figures...")
    plot_per_era(fake_rates_all, args.output)

    print("\nPlotting all-eras by flavor...")
    for flavor in ['electron', 'muon']:
        plot_all_eras_flavor(fake_rates_all, flavor, args.output)

    print("\nDone!")


if __name__ == '__main__':
    main()
