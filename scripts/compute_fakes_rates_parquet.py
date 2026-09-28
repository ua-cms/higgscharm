#!/usr/bin/env python3
"""
Compute fake rates from zplusl_ss (or zplusl_os) parquet output.

Fake rate: f(pT, region, flavor) = N_pass / N_total
where:
  - N_pass = probe leptons passing tight selection (is_passing_lepton == True)
  - N_total = all probe leptons
  - region  = barrel (is_barrel_lepton==1) or endcap (is_barrel_lepton==0)

DATA events are counted unweighted.
WZ MC events are counted weighted by weight_nominal normalized by
lumi * xsec / total_sumw (read from the corresponding coffea files).

Output pkl format matches the coffea script exactly:
    fake_rates[flavor][key]['fake_rate']  shape (n_pt, 2)
    fake_rates[flavor][key]['n_total']    shape (n_pt, 2)
    columns: index 0 = endcap, index 1 = barrel

Directory structure expected:
    <input>/<workflow>/<year>/<dataset>/<flavor>/*.parquet
    <coffea_input>/<workflow>/<year>/<dataset>/<dataset>_*.coffea  (for sumw)

Usage:
    python compute_fake_rates_parquet.py \
        --input /eos/user/p/pkatris/higgscharm/outputs/parquet \
        --coffea-input /eos/user/p/pkatris/higgscharm/outputs \
        --workflow zplusl_os \
        --year 2022postEE \
        --dataset-config analysis/filesets/2022postEE_nanov12.yaml \
        --lumi-config analysis/postprocess/luminosity.yaml

Add --debug to print per-bin count tables.
"""

import os
import glob
import argparse
import pickle
import yaml
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from coffea.util import load
from pathlib import Path


# ── Bin definitions ───────────────────────────────────────────────────────────
PT_EDGES       = np.array([5, 7, 10, 20, 30, 40, 50, 80], dtype=float)
REGION_LABELS  = ['endcap', 'barrel']   # index 0 = endcap, index 1 = barrel

N_PT = len(PT_EDGES) - 1

# ── Config paths ──────────────────────────────────────────────────────────────
LUMI_CONFIG    = "analysis/postprocess/luminosity.yaml"
DATASET_CONFIG = "analysis/filesets/2022postEE_nanov12.yaml"

# ── Dataset classification ────────────────────────────────────────────────────
_DATA_KEYWORDS = (
    'Muon', 'EGamma', 'DoubleEG', 'DoubleMuon', 'MuonEG',
    'SingleMuon', 'SingleElectron', 'SinglePhoton',
)
_WZ_KEYWORDS = ('WZto3LNu', 'WZTo3LNu', 'WZ_')


def _is_data(dataset): return any(kw in dataset for kw in _DATA_KEYWORDS)
def _is_wz(dataset):   return any(kw in dataset for kw in _WZ_KEYWORDS)


# ── Lumi weighting ────────────────────────────────────────────────────────────

def _get_lumi(year, lumi_config):
    with open(lumi_config) as f:
        lumi_cfg = yaml.safe_load(f)
    bare_year = year[:4]
    if year in lumi_cfg:
        lumi = float(lumi_cfg[year])
        print(f"  [lumi] matched key '{year}' -> {lumi:.1f} pb-1")
    elif bare_year in lumi_cfg:
        lumi = float(lumi_cfg[bare_year])
        print(f"  [lumi] WARNING: fell back to '{bare_year}' -> {lumi:.1f} pb-1")
    elif int(bare_year) in lumi_cfg:
        lumi = float(lumi_cfg[int(bare_year)])
        print(f"  [lumi] WARNING: fell back to {int(bare_year)} -> {lumi:.1f} pb-1")
    else:
        raise KeyError(f"Cannot find lumi for year '{year}' in {lumi_config}.")
    return lumi


def _get_lumi_weight(year, dataset_name, total_sumw,
                     lumi_config=LUMI_CONFIG, dataset_config=DATASET_CONFIG):
    lumi = _get_lumi(year, lumi_config)
    with open(dataset_config) as f:
        dataset_cfg = yaml.safe_load(f)
    if dataset_name not in dataset_cfg:
        raise KeyError(f"Dataset '{dataset_name}' not found in {dataset_config}.")
    entry = dataset_cfg[dataset_name]
    xsec  = entry["xsec"]
    era   = entry.get("era", "mc")
    if era not in ("mc", "signal"):
        print(f"  [WARNING] {dataset_name} has era={era} — returning weight=1.0")
        return 1.0
    if total_sumw == 0:
        raise ValueError(f"total_sumw=0 for {dataset_name} — cannot normalise")
    w = (lumi * xsec) / total_sumw
    print(f"  [lumi weight] dataset={dataset_name}, lumi={lumi:.1f} pb-1, "
          f"xsec={xsec:.6g} pb, total_sumw={total_sumw:.4g} -> w={w:.6e}")
    return w


def _get_wz_sumw(coffea_input, workflow, year, dataset_name):
    coffea_pattern = os.path.join(
        coffea_input, workflow, year, dataset_name, f"{dataset_name}_*.coffea"
    )
    coffea_files = glob.glob(coffea_pattern)
    if not coffea_files:
        raise FileNotFoundError(
            f"No coffea files found for {dataset_name} at {coffea_pattern}\n"
            f"Cannot normalize WZ weights without sumw."
        )
    total_sumw = 0.0
    for fpath in coffea_files:
        acc = load(fpath)
        total_sumw += float(acc["metadata"]["sumw"])
        print(f"    [sumw] {Path(fpath).name}: sumw={float(acc['metadata']['sumw']):.4g} "
              f"(running total={total_sumw:.4g})")
    return total_sumw


# ── Debug printing ────────────────────────────────────────────────────────────

def debug_print_2d(label, arr, pt_edges, region_labels):
    pt_labels = [f"{pt_edges[i]:.0f}-{pt_edges[i+1]:.0f}" for i in range(len(pt_edges)-1)]
    col_w  = 14
    header = f"{'pT / region':>16s}" + "".join(f"{r:>{col_w}s}" for r in region_labels) + f"{'SUM':>{col_w}s}"
    print(f"\n  {label}")
    print(f"  {'-' * len(header)}")
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    for i, pt_lab in enumerate(pt_labels):
        row_vals = arr[i, :]
        row_str  = "".join(f"{v:>{col_w}.2f}" for v in row_vals)
        print(f"  {pt_lab:>16s}{row_str}{np.sum(row_vals):>{col_w}.2f}")
    print(f"  {'-' * len(header)}")
    col_sums = arr.sum(axis=0)
    print(f"  {'SUM':>16s}" + "".join(f"{v:>{col_w}.2f}" for v in col_sums) + f"{arr.sum():>{col_w}.2f}")
    print()


def debug_flavor(flavor, n_total_data, n_pass_data, n_total_wz, n_pass_wz,
                 n_total_corr, n_pass_corr, pt_edges, region_labels):
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  DEBUG [{flavor.upper()}]")
    print(sep)
    debug_print_2d("DATA  n_total (unweighted)",       n_total_data, pt_edges, region_labels)
    debug_print_2d("DATA  n_pass  (unweighted)",       n_pass_data,  pt_edges, region_labels)
    debug_print_2d("WZ MC n_total (lumi-weighted)",    n_total_wz,   pt_edges, region_labels)
    debug_print_2d("WZ MC n_pass  (lumi-weighted)",    n_pass_wz,    pt_edges, region_labels)
    debug_print_2d("CORR  n_total = DATA - WZ",        n_total_corr, pt_edges, region_labels)
    debug_print_2d("CORR  n_pass  = DATA - WZ",        n_pass_corr,  pt_edges, region_labels)
    with np.errstate(divide='ignore', invalid='ignore'):
        fr_data = np.where(n_total_data > 0, n_pass_data / n_total_data, np.nan)
        fr_wz   = np.where(n_total_wz   > 0, n_pass_wz   / n_total_wz,   np.nan)
        fr_corr = np.where(n_total_corr > 0, n_pass_corr / n_total_corr, np.nan)
    debug_print_2d("DATA  fake rate",  fr_data, pt_edges, region_labels)
    debug_print_2d("WZ MC fake rate",  fr_wz,   pt_edges, region_labels)
    debug_print_2d("CORR  fake rate",  fr_corr, pt_edges, region_labels)
    print(f"  SCALAR SUMMARY [{flavor}]")
    print(f"    DATA  total probe leptons : {n_total_data.sum():.1f}")
    print(f"    DATA  passing probe leptons: {n_pass_data.sum():.1f}")
    print(f"    WZ MC total (lumi-weighted): {n_total_wz.sum():.2f}")
    print(f"    WZ MC passing (lumi-weighted): {n_pass_wz.sum():.2f}")
    print(f"    WZ / DATA total ratio      : {n_total_wz.sum() / max(n_total_data.sum(), 1):.3f}")
    print()


# ── Parquet loading ───────────────────────────────────────────────────────────

def _read_column(series):
    """
    Read a parquet column that may be:
      - nested/jagged (object dtype, each cell is an array of length 1)
      - flat scalar (float64/int dtype)
    Returns a flat numpy array.
    """
    first = series.iloc[0] if len(series) > 0 else None
    if first is not None and hasattr(first, '__len__') and not isinstance(first, str):
        return np.concatenate(series.values)
    else:
        return series.values.astype(float)


def load_parquet_files(input_dir, workflow, year, flavor,
                       coffea_input=None,
                       lumi_config=LUMI_CONFIG, dataset_config=DATASET_CONFIG,
                       debug=False):
    if coffea_input is None:
        coffea_input = input_dir

    base_path     = os.path.join(input_dir, workflow, year)
    parquet_files = glob.glob(os.path.join(base_path, "*", flavor, "*.parquet"))
    print(f"  [{flavor}] Found {len(parquet_files)} parquet files")

    data_frames = []
    wz_frames   = []

    # Pre-compute lumi weight per WZ dataset
    wz_datasets = set(
        os.path.basename(os.path.dirname(os.path.dirname(f)))
        for f in parquet_files
        if _is_wz(os.path.basename(os.path.dirname(os.path.dirname(f))))
    )
    wz_lumi_weights = {}
    for wz_dataset in wz_datasets:
        print(f"  [WZ] Loading sumw for {wz_dataset} from coffea files...")
        total_sumw = _get_wz_sumw(coffea_input, workflow, year, wz_dataset)
        wz_lumi_weights[wz_dataset] = _get_lumi_weight(
            year, wz_dataset, total_sumw,
            lumi_config=lumi_config, dataset_config=dataset_config,
        )

    for fpath in sorted(parquet_files):
        dataset = os.path.basename(os.path.dirname(os.path.dirname(fpath)))
        is_data = _is_data(dataset)
        is_wz   = _is_wz(dataset)

        if not is_data and not is_wz:
            continue

        try:
            cols = ['probe_lepton_pt', 'probe_lepton_eta',
                    'is_passing_lepton', 'is_barrel_lepton', 'n_missing_hits']
            if is_wz:
                cols.append('weight_nominal')
            df = pd.read_parquet(fpath, columns=cols)
        except Exception as e:
            print(f"  [WARNING] Skipping corrupted file {os.path.basename(fpath)}: {e}")
            continue

        pt_arr      = _read_column(df['probe_lepton_pt'])
        eta_arr     = _read_column(df['probe_lepton_eta'])
        passing_arr = _read_column(df['is_passing_lepton']).astype(bool)
        barrel_arr  = _read_column(df['is_barrel_lepton']).astype(int)

        if is_data:
            exploded = pd.DataFrame({
                'probe_lepton_pt':   pt_arr,
                'probe_lepton_eta':  eta_arr,
                'is_passing_lepton': passing_arr,
                'is_barrel_lepton':  barrel_arr,
                'weight':            np.ones(len(pt_arr), dtype=float),
            })
            data_frames.append(exploded)

        elif is_wz:
            weight_nominal = df['weight_nominal'].values.astype(float)
            n_probes_per_event = df['probe_lepton_pt'].apply(
                lambda x: len(x) if hasattr(x, '__len__') else 1
            ).values
            weight_nominal_repeated = np.repeat(weight_nominal, n_probes_per_event)
            lumi_w  = wz_lumi_weights[dataset]
            weights = lumi_w * weight_nominal_repeated

            if debug:
                print(f"    [WZ debug] {os.path.basename(fpath)}: "
                      f"{len(pt_arr)} probes, lumi_w={lumi_w:.4e}, "
                      f"weight_nominal: min={weight_nominal.min():.2f} "
                      f"max={weight_nominal.max():.2f}, "
                      f"final weight: min={weights.min():.4f} "
                      f"max={weights.max():.4f} sum={weights.sum():.4f}")

            exploded = pd.DataFrame({
                'probe_lepton_pt':   pt_arr,
                'probe_lepton_eta':  eta_arr,
                'is_passing_lepton': passing_arr,
                'is_barrel_lepton':  barrel_arr,
                'weight':            weights,
            })
            wz_frames.append(exploded)

    df_data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
    df_wz   = pd.concat(wz_frames,   ignore_index=True) if wz_frames   else pd.DataFrame()

    n_data_files = sum(1 for f in parquet_files
                       if _is_data(os.path.basename(os.path.dirname(os.path.dirname(f)))))
    n_wz_files   = sum(1 for f in parquet_files
                       if _is_wz(os.path.basename(os.path.dirname(os.path.dirname(f)))))
    print(f"  [{flavor}] DATA files: {n_data_files} -> {len(df_data):,} probe leptons")
    print(f"  [{flavor}] WZ   files: {n_wz_files}   -> {len(df_wz):,} probe leptons")

    if debug and not df_wz.empty:
        w = df_wz['weight']
        print(f"  [{flavor}] WZ final weight summary:")
        print(f"    min={w.min():.4f}  max={w.max():.4f}  "
              f"mean={w.mean():.4f}  sum={w.sum():.4f}  "
              f"n_negative={int((w < 0).sum())}  n_total={len(w)}")

    return df_data, df_wz


# ── Histogram filling ─────────────────────────────────────────────────────────

def fill_histograms(df, pt_edges):
    """
    Fill (pT, region) histograms using is_barrel_lepton directly.
    Output shape: (n_pt, 2) where col 0 = endcap, col 1 = barrel.
    This matches the coffea script output exactly so the same
    plotting script (plot_fake_rates_1d.py) can be used for both.
    """
    n_pt = len(pt_edges) - 1
    n_total = np.zeros((n_pt, 2))
    n_pass  = np.zeros((n_pt, 2))

    if df.empty:
        return n_total, n_pass

    pt      = df['probe_lepton_pt'].values
    barrel  = df['is_barrel_lepton'].values.astype(int)  # 0=endcap, 1=barrel
    w       = df['weight'].values
    passing = df['is_passing_lepton'].values.astype(bool)

    for region in [0, 1]:
        mask = barrel == region
        n_total[:, region], _ = np.histogram(
            pt[mask], bins=pt_edges, weights=w[mask]
        )
        n_pass[:, region], _ = np.histogram(
            pt[mask & passing], bins=pt_edges, weights=w[mask & passing]
        )

    return n_total, n_pass


# ── Fake rate computation ─────────────────────────────────────────────────────

def compute_fake_rate(n_pass, n_total, min_events=10):
    valid = (n_total > min_events) & (n_pass >= 0) & (n_total >= 0)
    with np.errstate(divide='ignore', invalid='ignore'):
        fake_rate = np.where(valid, n_pass / n_total, 0.0)
        fake_rate = np.clip(fake_rate, 0.0, 1.0)
        fake_rate_err = np.where(
            valid,
            np.sqrt(fake_rate * (1 - fake_rate) / np.where(valid, n_total, 1.0)),
            1.0,
        )
    return fake_rate, fake_rate_err


# ── Main extraction ───────────────────────────────────────────────────────────

def extract_fake_rates(input_dir, workflow, year,
                       coffea_input=None,
                       lumi_config=LUMI_CONFIG, dataset_config=DATASET_CONFIG,
                       debug=False):
    if coffea_input is None:
        coffea_input = input_dir

    fake_rates = {}

    for flavor in ['electron', 'muon']:
        print(f"\nProcessing flavor: {flavor}")
        df_data, df_wz = load_parquet_files(
            input_dir, workflow, year, flavor,
            coffea_input=coffea_input,
            lumi_config=lumi_config, dataset_config=dataset_config,
            debug=debug,
        )

        n_total_data, n_pass_data = fill_histograms(df_data, PT_EDGES)
        n_total_wz,   n_pass_wz   = fill_histograms(df_wz,   PT_EDGES)

        n_total_corr = n_total_data - n_total_wz
        n_pass_corr  = n_pass_data  - n_pass_wz

        if debug:
            debug_flavor(flavor,
                         n_total_data, n_pass_data,
                         n_total_wz,   n_pass_wz,
                         n_total_corr, n_pass_corr,
                         PT_EDGES, REGION_LABELS)

        fr_uncorr, fr_err_uncorr = compute_fake_rate(n_pass_data,  n_total_data)
        fr_corr,   fr_err_corr   = compute_fake_rate(n_pass_corr,  n_total_corr)

        stable     = (n_total_corr > 0) & (n_pass_corr >= 0)
        n_unstable = int(np.sum(~stable))
        if n_unstable > 0:
            print(f"  [{flavor}] {n_unstable} bins unstable after WZ subtraction "
                  f"-> falling back to uncorrected")
            if debug:
                pts, regions = np.where(~stable)
                for pi, ri in zip(pts, regions):
                    print(f"    unstable bin: pT={PT_EDGES[pi]:.0f}-{PT_EDGES[pi+1]:.0f} "
                          f"region={REGION_LABELS[ri]}  "
                          f"n_total_corr={n_total_corr[pi,ri]:.2f}  "
                          f"n_pass_corr={n_pass_corr[pi,ri]:.2f}")

        safe_fr     = np.where(stable, fr_corr,     fr_uncorr)
        safe_fr_err = np.where(stable, fr_err_corr, fr_err_uncorr)

        fake_rates[flavor] = {
            'uncorrected': {
                'fake_rate':     fr_uncorr,
                'fake_rate_err': fr_err_uncorr,
                'n_pass':        n_pass_data,
                'n_total':       n_total_data,
                'pt_edges':      PT_EDGES,
                'region_labels': REGION_LABELS,
            },
            'corrected': {
                'fake_rate':     fr_corr,
                'fake_rate_err': fr_err_corr,
                'n_pass':        n_pass_corr,
                'n_total':       n_total_corr,
                'pt_edges':      PT_EDGES,
                'region_labels': REGION_LABELS,
            },
            'safe_corrected': {
                'fake_rate':     safe_fr,
                'fake_rate_err': safe_fr_err,
                'n_pass':        np.where(stable, n_pass_corr,  n_pass_data),
                'n_total':       np.where(stable, n_total_corr, n_total_data),
                'pt_edges':      PT_EDGES,
                'region_labels': REGION_LABELS,
            },
        }

        # top-level keys alias to safe_corrected
        for k in ('fake_rate', 'fake_rate_err', 'n_pass', 'n_total',
                  'pt_edges', 'region_labels'):
            fake_rates[flavor][k] = fake_rates[flavor]['safe_corrected'][k]

        for label in ['uncorrected', 'corrected', 'safe_corrected']:
            fr = fake_rates[flavor][label]['fake_rate']
            valid_fr = fr[fr > 0]
            if len(valid_fr):
                print(f"  [{label}] {flavor}: mean f={np.mean(valid_fr):.3f}, "
                      f"max f={np.max(fr):.3f}")

    return fake_rates


# ── Plotting ──────────────────────────────────────────────────────────────────

def plot_fake_rates(fake_rates, output_dir):
    """Simple 1D plot per flavor, barrel and endcap as separate lines."""
    os.makedirs(output_dir, exist_ok=True)
    colors  = ['#cc0000', '#1f4e9e']   # endcap=red, barrel=blue
    markers = ['s', 'o']

    for flavor, data in fake_rates.items():
        fig, ax = plt.subplots(figsize=(8, 6))
        pt_edges      = data['pt_edges']
        region_labels = data['region_labels']
        fr            = data['fake_rate']
        pt_centers    = 0.5 * (pt_edges[:-1] + pt_edges[1:])
        xerr          = 0.5 * (pt_edges[1:] - pt_edges[:-1])

        for ri, (rlabel, color, marker) in enumerate(zip(region_labels, colors, markers)):
            ax.errorbar(
                pt_centers, fr[:, ri],
                xerr=xerr,
                fmt=marker, color=color,
                linestyle='none', markersize=5,
                capsize=3, label=rlabel,
            )

        ax.set_xlabel('Probe lepton $p_T$ [GeV]')
        ax.set_ylabel('Fake Rate')
        ax.set_ylim(0, 0.5)
        ax.set_title(f'{flavor.capitalize()} Fake Rate (safe corrected)')
        ax.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'fake_rate_{flavor}.pdf'))
        plt.savefig(os.path.join(output_dir, f'fake_rate_{flavor}.png'), dpi=150)
        plt.close()
        print(f"Saved: {output_dir}/fake_rate_{flavor}.png")


# ── Save ──────────────────────────────────────────────────────────────────────

def save_fake_rates(fake_rates, output_path):
    with open(output_path, 'wb') as f:
        pickle.dump(fake_rates, f)
    print(f"Saved fake rates to: {output_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compute fake rates from zplusl parquet output'
    )
    parser.add_argument('--input',          required=True,
                        help='Input directory containing parquet files (parent of <workflow>/<year>/)')
    parser.add_argument('--coffea-input',   default=None,
                        help='Input directory containing coffea files for sumw (default: same as --input)')
    parser.add_argument('--workflow',       default='zplusl_os',
                        choices=['zplusl_ss', 'zplusl_os'])
    parser.add_argument('--year',           default='2022postEE')
    parser.add_argument('--output',         default=None)
    parser.add_argument('--lumi-config',    default=None)
    parser.add_argument('--dataset-config', default=None)
    parser.add_argument('--debug',          action='store_true')
    args = parser.parse_args()

    lumi_config    = args.lumi_config    or LUMI_CONFIG
    dataset_config = args.dataset_config or DATASET_CONFIG
    coffea_input   = args.coffea_input   or args.input

    if args.output is None:
        args.output = os.path.join(
            os.path.dirname(args.input),
            'fake_rates_parquet', args.year
        )

    print(f"Input (parquet): {args.input}")
    print(f"Input (coffea) : {coffea_input}")
    print(f"Workflow:        {args.workflow}")
    print(f"Year:            {args.year}")
    print(f"Output:          {args.output}")
    print(f"Debug:           {args.debug}")

    fake_rates = extract_fake_rates(
        args.input, args.workflow, args.year,
        coffea_input=coffea_input,
        lumi_config=lumi_config, dataset_config=dataset_config,
        debug=args.debug,
    )

    os.makedirs(args.output, exist_ok=True)
    save_fake_rates(fake_rates, os.path.join(args.output, 'fake_rates.pkl'))
    plot_fake_rates(fake_rates, args.output)

    print("\nDone!")


if __name__ == '__main__':
    main()