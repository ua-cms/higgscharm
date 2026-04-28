#!/usr/bin/env python3
"""
Validation (2a): Data-driven Z+X vs MC-driven Z+X MVA shape comparison.

Loads MC reducible samples (DY, WZ, ttbar) from the CR workflow, applies the same
fake-rate weighting as the data-driven estimate, runs MVA inference, and overlays
the resulting MVA score distributions against the data-driven Z+X prediction.

If the shapes agree -> the fake-rate method correctly extrapolates from CR to SR.
If they disagree -> CR kinematics differ from SR, and the MVA score shape needs
a correction or systematic to cover the difference.

Usage:
    python scripts/validate_zx_mc_shape.py \\
        --zx-input  /eos/user/s/snandaku/Analysis/zx_background_mva \\
        --cr-input  /eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_CR \\
        --fake-rates-dir /eos/user/s/snandaku/higgscharm/outputs/fake_rates \\
        --config    /eos/home-s/snandaku/b-hive_ttcc/config/hc_zzto4l_mw_training_4class_nomass.yml \\
        --model     /eos/home-s/snandaku/b-hive_ttcc/output/TrainingTask/hc_zzto4l_mw_training_4class_nomass/hcZZ_big4classjetyesmasslooseoldcbal/train_loose_oldcwithmassbal/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal/best_model.pt \\
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

HIGGSCHARM = '/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm'

# Load MVAPostProcessor directly to avoid triggering analysis.postprocess.__init__,
# which imports postprocessor.py → dask → pyarrow version conflict.
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location(
    'mva_inference',
    os.path.join(HIGGSCHARM, 'analysis/postprocess/mva_inference.py'),
)
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
MVAPostProcessor = _mod.MVAPostProcessor

ERAS = ['2022postEE', '2022preEE', '2023preBPix', '2023postBPix']

# MC reducible dataset name prefixes in the CR output
MC_REDUCIBLE_PREFIXES = ['DYJetsToLL_10to50', 'DYJetsToLL_50', 'WZto3LNu', 'TTto2L2Nu']

# Process label for each prefix (for legend)
MC_REDUCIBLE_LABELS = {
    'DYJetsToLL': 'DY+jets',
    'WZto3LNu':   'WZ',
    'TTto2L2Nu':  'ttbar',
}

# Column rename maps (same as run_zx_inference.py)
COL_MAP_3P1F = {
    'm4l_3p1f': 'm4l', 'pT_4l_3p1f': 'pT_4l', 'eta_4l_3p1f': 'eta_4l',
    'phi_4l_3p1f': 'phi_4l', 'z1_mass_3p1f': 'z1_mass', 'z2_mass_3p1f': 'z2_mass',
    'z1_pt_3p1f': 'z1_pt', 'z1_eta_3p1f': 'z1_eta', 'z1_phi_3p1f': 'z1_phi',
    'z2_pt_3p1f': 'z2_pt', 'z2_eta_3p1f': 'z2_eta', 'z2_phi_3p1f': 'z2_phi',
    'deltaR_ZZ_3p1f': 'deltaR_ZZ',
    'z1_l1_pt_3p1f': 'z1_l1_pt', 'z1_l1_eta_3p1f': 'z1_l1_eta', 'z1_l1_phi_3p1f': 'z1_l1_phi',
    'z1_l2_pt_3p1f': 'z1_l2_pt', 'z1_l2_eta_3p1f': 'z1_l2_eta', 'z1_l2_phi_3p1f': 'z1_l2_phi',
    'l3_pt_3p1f': 'z2_l1_pt', 'l3_eta_3p1f': 'z2_l1_eta',
    'l4_pt_3p1f': 'z2_l2_pt', 'l4_eta_3p1f': 'z2_l2_eta',
    'cjets_h_dphi_3p1f': 'cjets_h_dphi_inclusive',
}
COL_MAP_2P2F = {
    'm4l_2p2f': 'm4l', 'pT_4l_2p2f': 'pT_4l', 'eta_4l_2p2f': 'eta_4l',
    'phi_4l_2p2f': 'phi_4l', 'z1_mass_2p2f': 'z1_mass', 'z2_mass_2p2f': 'z2_mass',
    'z1_pt_2p2f': 'z1_pt', 'z1_eta_2p2f': 'z1_eta', 'z1_phi_2p2f': 'z1_phi',
    'z2_pt_2p2f': 'z2_pt', 'z2_eta_2p2f': 'z2_eta', 'z2_phi_2p2f': 'z2_phi',
    'deltaR_ZZ_2p2f': 'deltaR_ZZ',
    'z1_l1_pt_2p2f': 'z1_l1_pt', 'z1_l1_eta_2p2f': 'z1_l1_eta', 'z1_l1_phi_2p2f': 'z1_l1_phi',
    'z1_l2_pt_2p2f': 'z1_l2_pt', 'z1_l2_eta_2p2f': 'z1_l2_eta', 'z1_l2_phi_2p2f': 'z1_l2_phi',
    'l3_pt_2p2f': 'z2_l1_pt', 'l3_eta_2p2f': 'z2_l1_eta',
    'l4_pt_2p2f': 'z2_l2_pt', 'l4_eta_2p2f': 'z2_l2_eta',
    'cjets_h_dphi_2p2f': 'cjets_h_dphi_inclusive',
}
PASSTHROUGH = [
    'jet_multiplicity', 'n_cjet', 'jet_ht', 'cjet_HT', 'nSV',
    'jet_pt', 'jet_eta', 'jet_btagPNetB', 'jet_btagPNetCvL', 'jet_btagPNetCvB',
    'cjets_pt', 'cjets_eta', 'cjets_btagPNetB', 'cjets_btagPNetCvL', 'cjets_btagPNetCvB',
    'weight_nominal',
]


# ---------------------------------------------------------------------------
# Fake-rate utilities (mirrors estimate_zx_background.py)
# ---------------------------------------------------------------------------

def load_fake_rates(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def get_fake_rate_array(pt, eta, pdgid, fake_rates):
    """Vectorised fake-rate lookup for an array of leptons."""
    pt   = np.asarray(pt,    dtype=float)
    eta  = np.abs(np.asarray(eta,  dtype=float))
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
    """f/(1-f) for the one failing lepton in each 3P1F event."""
    l3_tight = df['l3_istight_3p1f'].values.astype(bool)
    l4_tight = df['l4_istight_3p1f'].values.astype(bool)

    # Failing lepton kinematics
    pt    = np.where(~l3_tight, df['l3_pt_3p1f'].values,  df['l4_pt_3p1f'].values)
    eta   = np.where(~l3_tight, df['l3_eta_3p1f'].values, df['l4_eta_3p1f'].values)
    pdgid = np.where(~l3_tight, df['l3_pdgid_3p1f'].values, df['l4_pdgid_3p1f'].values)

    f = get_fake_rate_array(pt, eta, pdgid, fake_rates)
    f = np.clip(f, 0.0, 0.999)
    return np.where(f > 0, f / (1.0 - f), 0.0)


def compute_fr_weight_2p2f(df, fake_rates):
    """f1*f2/((1-f1)*(1-f2)) for both failing leptons in each 2P2F event."""
    f1 = get_fake_rate_array(df['l3_pt_2p2f'].values, df['l3_eta_2p2f'].values,
                             df['l3_pdgid_2p2f'].values, fake_rates)
    f2 = get_fake_rate_array(df['l4_pt_2p2f'].values, df['l4_eta_2p2f'].values,
                             df['l4_pdgid_2p2f'].values, fake_rates)
    f1 = np.clip(f1, 0.0, 0.999)
    f2 = np.clip(f2, 0.0, 0.999)
    return np.where((f1 > 0) & (f2 > 0), (f1 * f2) / ((1 - f1) * (1 - f2)), 0.0)


# ---------------------------------------------------------------------------
# Parquet loading
# ---------------------------------------------------------------------------

def _flatten_parquet_fields(arr, needed):
    """Flatten large_list columns from one awkward array into a dict of arrays."""
    flat = {}
    for field in needed:
        if field not in arr.fields:
            continue
        col = arr[field]
        flat[field] = ak.to_numpy(ak.flatten(col, axis=1) if col.ndim > 1 else col)
    return flat


def load_mc_reducible_cr(cr_input_dir, era, category, max_files=None):
    """
    Load MC reducible (DY, WZ, TT) CR parquet files for one era/category.

    Returns a DataFrame labelled with 'process' (DY, WZ, ttbar).
    """
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
        if max_files:
            files = files[:max_files]
        if not files:
            logging.warning(f'  No files for {prefix} {era} {category}')
            continue

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

        # Map prefix → readable process label
        proc = next((v for k, v in MC_REDUCIBLE_LABELS.items() if prefix.startswith(k)),
                    prefix.split('_')[0])
        df['process'] = proc
        frames.append(df)
        logging.info(f'    {proc}: {len(df):,} events')

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# MVA inference helpers (reuse run_zx_inference.py logic)
# ---------------------------------------------------------------------------

def prepare_for_inference(df, col_map):
    out = {}
    for src, dst in col_map.items():
        if src in df.columns:
            out[dst] = df[src].values
    for col in PASSTHROUGH:
        if col in df.columns:
            out[col] = df[col].values
    return pd.DataFrame(out)


def run_mva_on_df(df_cr, col_map, processor):
    df_mva = prepare_for_inference(df_cr, col_map)
    processor._load_model()
    features = processor.prepare_features(df_mva)
    result   = processor.predict(features)
    for i, cls in enumerate(processor.class_names):
        df_cr[f'mva_score_{cls}'] = result['scores'][:, i]
    df_cr['mva_class_prediction'] = result['class_prediction']
    return df_cr


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

SCORE_COLS = ['mva_score_qqZZ', 'mva_score_ggZZ', 'mva_score_Signal', 'mva_score_Other_Higgs']
SCORE_LABELS = {
    'mva_score_qqZZ':        r'MVA score: $qq\to ZZ$',
    'mva_score_ggZZ':        r'MVA score: $gg\to ZZ$',
    'mva_score_Signal':      r'MVA score: Signal (H$^+$c)',
    'mva_score_Other_Higgs': r'MVA score: Other Higgs',
}
N_BINS = 20


def plot_score_comparison(df_data_zx, df_mc_zx, output_dir, era):
    """
    Overlay data-driven Z+X vs MC-driven Z+X for all 4 MVA scores.
    Both distributions are normalised to unit area.
    """
    os.makedirs(output_dir, exist_ok=True)
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()

    # Net weights
    w_data = df_data_zx['weight_zx'].values            # already signed (3P1F +, 2P2F -)
    w_mc   = (df_mc_zx['zx_fr_weight'] *
              df_mc_zx['zx_sign'] *
              df_mc_zx['weight_nominal'].fillna(1.0)).values

    for ax, col in zip(axes, SCORE_COLS):
        bins = np.linspace(0, 1, N_BINS + 1)
        x_data = df_data_zx[col].values
        x_mc   = df_mc_zx[col].values

        # Positive-only weight normalisation (shape comparison)
        w_data_pos = np.abs(w_data)
        w_mc_pos   = np.abs(w_mc)
        norm_d = w_data_pos.sum()
        norm_m = w_mc_pos.sum()

        h_data, _ = np.histogram(x_data, bins=bins, weights=w_data_pos / norm_d)
        h_mc,   _ = np.histogram(x_mc,   bins=bins, weights=w_mc_pos   / norm_m)

        cx = 0.5 * (bins[:-1] + bins[1:])
        ax.step(bins[:-1], h_data, where='post', color='steelblue',   lw=2, label='Data-driven Z+X')
        ax.step(bins[:-1], h_mc,   where='post', color='darkorange',  lw=2, label='MC-driven Z+X (DY/WZ/tt)')
        ax.set_xlabel(SCORE_LABELS[col], fontsize=12)
        ax.set_ylabel('Normalised events / bin', fontsize=11)
        ax.legend(fontsize=10)
        ax.set_xlim(0, 1)

    hep.cms.label('Preliminary', data=False, ax=axes[0], fontsize=12)
    fig.suptitle(f'Z+X validation (2a): MVA score shapes — {era}', fontsize=13, y=1.01)
    fig.tight_layout()
    out = os.path.join(output_dir, f'zx_mc_vs_datadriven_scores_{era}.png')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Saved: {out}')


def plot_combined_scores(df_data_all, df_mc_all, output_dir):
    """All eras combined overlay."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()

    w_data = df_data_all['weight_zx'].values
    w_mc   = (df_mc_all['zx_fr_weight'] *
              df_mc_all['zx_sign'] *
              df_mc_all['weight_nominal'].fillna(1.0)).values

    for ax, col in zip(axes, SCORE_COLS):
        bins = np.linspace(0, 1, N_BINS + 1)
        w_d = np.abs(w_data); norm_d = w_d.sum()
        w_m = np.abs(w_mc);   norm_m = w_m.sum()
        h_d, _ = np.histogram(df_data_all[col].values, bins=bins, weights=w_d / norm_d)
        h_m, _ = np.histogram(df_mc_all[col].values,   bins=bins, weights=w_m / norm_m)

        ax.step(bins[:-1], h_d, where='post', color='steelblue',  lw=2, label='Data-driven Z+X')
        ax.step(bins[:-1], h_m, where='post', color='darkorange', lw=2, label='MC-driven Z+X (DY/WZ/tt)')
        ax.set_xlabel(SCORE_LABELS[col], fontsize=12)
        ax.set_ylabel('Normalised events / bin', fontsize=11)
        ax.legend(fontsize=10)
        ax.set_xlim(0, 1)

    hep.cms.label('Preliminary', data=False, ax=axes[0], fontsize=12)
    fig.suptitle('Z+X validation (2a): MVA score shapes — all Run 3 eras combined', fontsize=13, y=1.01)
    fig.tight_layout()
    out = os.path.join(output_dir, 'zx_mc_vs_datadriven_scores_combined.png')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Saved: {out}')


def plot_process_breakdown(df_mc_all, output_dir):
    """
    MVA ggZZ score broken down by MC process (DY, WZ, tt) overlaid with data-driven.
    Helps see if one process drives any disagreement.
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    col_gg = 'mva_score_ggZZ'
    col_sg = 'mva_score_Signal'
    bins = np.linspace(0, 1, N_BINS + 1)
    colors = {'DY+jets': 'tab:blue', 'WZ': 'tab:orange', 'ttbar': 'tab:green'}

    for ax, col in zip(axes, [col_gg, col_sg]):
        for proc, grp in df_mc_all.groupby('process'):
            w = np.abs((grp['zx_fr_weight'] * grp['zx_sign'] * grp['weight_nominal'].fillna(1.0)).values)
            if w.sum() == 0:
                continue
            h, _ = np.histogram(grp[col].values, bins=bins, weights=w / w.sum())
            ax.step(bins[:-1], h, where='post', lw=1.8,
                    label=proc, color=colors.get(proc, None))
        ax.set_xlabel(SCORE_LABELS[col], fontsize=12)
        ax.set_ylabel('Normalised events / bin', fontsize=11)
        ax.legend(fontsize=10)
        ax.set_xlim(0, 1)

    hep.cms.label('Preliminary', data=False, ax=axes[0], fontsize=12)
    fig.suptitle('Z+X validation (2a): per-process MC breakdown', fontsize=13, y=1.01)
    fig.tight_layout()
    out = os.path.join(output_dir, 'zx_mc_process_breakdown.png')
    fig.savefig(out, dpi=150, bbox_inches='tight')
    plt.close(fig)
    logging.info(f'Saved: {out}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Validation 2a: data-driven vs MC Z+X MVA shape')
    parser.add_argument('--zx-input',       required=True, help='Dir with zx_*_mva.parquet files')
    parser.add_argument('--cr-input',       required=True, help='hplusc_mva_4class_CR output dir')
    parser.add_argument('--fake-rates-dir', required=True, help='Dir with fake_rates/<era>/fake_rates.pkl')
    parser.add_argument('--config',         required=True, help='b-hive config YAML')
    parser.add_argument('--model',          required=True, help='Path to best_model.pt')
    parser.add_argument('--output',         required=True, help='Output directory for plots')
    parser.add_argument('--eras', nargs='+', default=ERAS)
    parser.add_argument('--max-files', type=int, default=None,
                        help='Cap per-dataset file count (for quick testing)')
    args = parser.parse_args()

    processor = MVAPostProcessor(
        bhive_config_path=args.config,
        bhive_model_path=args.model,
        apply_mass_window=False,
    )

    all_data_dfs = []
    all_mc_dfs   = []

    for era in args.eras:
        logging.info(f'\n========== {era} ==========')

        # ── Load data-driven Z+X (already has MVA scores) ──────────────────────
        frames_dd = []
        for cr_type in ('3p1f', '2p2f'):
            path = os.path.join(args.zx_input, era, f'zx_{cr_type}_{era}_mva.parquet')
            if not os.path.exists(path):
                logging.warning(f'Missing: {path}')
                continue
            df = pd.read_parquet(path)
            logging.info(f'  Data-driven {cr_type.upper()}: {len(df):,} events')
            frames_dd.append(df)

        if not frames_dd:
            logging.warning(f'No data-driven parquets for {era}, skipping')
            continue
        df_data_zx = pd.concat(frames_dd, ignore_index=True)

        # ── Load + weight MC reducible ──────────────────────────────────────────
        fr_path = os.path.join(args.fake_rates_dir, era, 'fake_rates.pkl')
        if not os.path.exists(fr_path):
            logging.warning(f'No fake rates at {fr_path}, skipping {era}')
            continue
        fake_rates = load_fake_rates(fr_path)
        logging.info(f'  Loaded fake rates for {era}')

        mc_frames = []
        for cr_type, col_map, sign in [('CR_3P1F', COL_MAP_3P1F, +1.0),
                                        ('CR_2P2F', COL_MAP_2P2F, -1.0)]:
            df_mc = load_mc_reducible_cr(args.cr_input, era, cr_type,
                                         max_files=args.max_files)
            if df_mc.empty:
                continue

            suffix = '_3p1f' if cr_type == 'CR_3P1F' else '_2p2f'
            # Compute fake-rate weights
            if cr_type == 'CR_3P1F':
                if f'l3_istight{suffix}' not in df_mc.columns:
                    logging.warning(f'  Missing istight column for {cr_type}, skipping')
                    continue
                df_mc['zx_fr_weight'] = compute_fr_weight_3p1f(df_mc, fake_rates)
            else:
                df_mc['zx_fr_weight'] = compute_fr_weight_2p2f(df_mc, fake_rates)
            df_mc['zx_sign'] = sign

            # MVA inference
            logging.info(f'  Running MVA on MC {cr_type} ({len(df_mc):,} events)...')
            df_mc = run_mva_on_df(df_mc, col_map, processor)
            mc_frames.append(df_mc)

        if not mc_frames:
            logging.warning(f'No MC reducible events for {era}')
            continue
        df_mc_zx = pd.concat(mc_frames, ignore_index=True)

        # ── Per-era plots ──────────────────────────────────────────────────────
        plot_score_comparison(df_data_zx, df_mc_zx, args.output, era)

        # ── Summary stats ──────────────────────────────────────────────────────
        w_data = np.abs(df_data_zx['weight_zx'].values).sum()
        w_mc   = np.abs((df_mc_zx['zx_fr_weight'] * df_mc_zx['zx_sign'] *
                         df_mc_zx['weight_nominal'].fillna(1.0)).values).sum()
        logging.info(f'  Data-driven weighted yield: {w_data:.1f}')
        logging.info(f'  MC-driven  weighted yield:  {w_mc:.1f}  (shape comparison only)')

        all_data_dfs.append(df_data_zx)
        all_mc_dfs.append(df_mc_zx)

    # ── Combined plots ─────────────────────────────────────────────────────────
    if all_data_dfs and all_mc_dfs:
        df_data_all = pd.concat(all_data_dfs, ignore_index=True)
        df_mc_all   = pd.concat(all_mc_dfs,   ignore_index=True)
        plot_combined_scores(df_data_all, df_mc_all, args.output)
        plot_process_breakdown(df_mc_all, args.output)

    logging.info('\nDone.')


if __name__ == '__main__':
    main()
