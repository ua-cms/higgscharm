#!/usr/bin/env python3
"""
Compute fake rates from zplusl_ss (or zplusl_os) workflow output.

Fake rate: f(pT, eta, flavor) = N_pass / N_total
where:
  - N_pass = probe leptons passing tight selection
  - N_total = all probe leptons (relaxed but not necessarily tight)

Usage:
    python compute_fake_rates.py --input /path/to/zplusl_ss/output --year 2022postEE
"""

import os
import glob
import argparse
import pickle
import numpy as np
import matplotlib.pyplot as plt
from coffea.util import load


def compute_fake_rate(hist_pass, hist_total, min_events=10):
    """
    Compute fake rate from pass and total histograms.

    Args:
        hist_pass: Histogram of passing (tight) leptons
        hist_total: Histogram of all (relaxed) leptons
        min_events: Minimum events in bin for valid fake rate

    Returns:
        fake_rate: Array of fake rates
        fake_rate_err: Array of fake rate uncertainties (binomial)
    """
    n_pass = hist_pass.values()
    n_total = hist_total.values()

    # Valid bins: n_total > min_events AND both non-negative
    # (MC subtraction can produce negative counts in some bins)
    valid = (n_total > min_events) & (n_pass >= 0) & (n_total >= 0)

    with np.errstate(divide='ignore', invalid='ignore'):
        fake_rate = np.where(valid, n_pass / n_total, 0.0)
        # Clip to physical range [0, 1]
        fake_rate = np.clip(fake_rate, 0.0, 1.0)
        # Binomial uncertainty: sqrt(f*(1-f)/N)
        fake_rate_err = np.where(
            valid,
            np.sqrt(fake_rate * (1 - fake_rate) / np.where(valid, n_total, 1.0)),
            1.0  # Large uncertainty for invalid bins
        )

    return fake_rate, fake_rate_err


# CMS Run 3 data primary dataset name fragments — used to distinguish DATA from MC
_DATA_KEYWORDS = (
    'Muon', 'EGamma', 'DoubleEG', 'DoubleMuon', 'MuonEG',
    'SingleMuon', 'SingleElectron', 'SinglePhoton',
)

# WZ dataset directory name fragments — used to identify WZ MC for prompt subtraction
_WZ_KEYWORDS = ('WZto3LNu', 'WZTo3LNu', 'WZ_')


def _is_data_dataset(dataset_dir):
    """Return True if the dataset directory name belongs to a DATA sample."""
    name = os.path.basename(dataset_dir)
    return any(kw in name for kw in _DATA_KEYWORDS)


def _is_wz_dataset(dataset_dir):
    """Return True if the dataset directory name belongs to a WZ MC sample."""
    name = os.path.basename(dataset_dir)
    return any(kw in name for kw in _WZ_KEYWORDS)


def load_zplusl_histograms(input_dir, year, workflow="zplusl_ss"):
    """
    Load histograms from zplusl_ss or zplusl_os workflow output.
    Returns two separate merged histogram dicts: one for DATA only,
    one for WZ MC only, so the caller can compute the corrected fake
    rate as DATA − WZ (removing prompt WZ → ℓℓℓν contamination).

    Args:
        input_dir: Path to workflow output directory
        year: Year (e.g., "2022postEE")
        workflow: Workflow name

    Returns:
        (hist_data, hist_wz): dicts of coffea histograms
    """
    coffea_path = os.path.join(input_dir, workflow, year)
    if not os.path.exists(coffea_path):
        raise FileNotFoundError(f"Output not found: {coffea_path}")

    coffea_files = glob.glob(os.path.join(coffea_path, "**", "*.coffea"), recursive=True)
    print(f"Found {len(coffea_files)} coffea files in {coffea_path}")

    hist_data = {}   # DATA only (for uncorrected fake rate)
    hist_wz   = {}   # WZ MC only (for prompt-lepton subtraction)

    for fpath in coffea_files:
        dataset_dir = os.path.dirname(fpath)
        is_data = _is_data_dataset(dataset_dir)
        is_wz   = _is_wz_dataset(dataset_dir)
        data = load(fpath)
        if 'histograms' not in data:
            continue
        for key, hist in data['histograms'].items():
            if is_data:
                if key in hist_data:
                    hist_data[key] += hist
                else:
                    hist_data[key] = hist.copy()
            if is_wz:
                if key in hist_wz:
                    hist_wz[key] += hist
                else:
                    hist_wz[key] = hist.copy()

    n_data = sum(1 for f in coffea_files if _is_data_dataset(os.path.dirname(f)))
    n_wz   = sum(1 for f in coffea_files if _is_wz_dataset(os.path.dirname(f)))
    n_mc   = len(coffea_files) - n_data
    print(f"  DATA files: {n_data},  WZ files: {n_wz},  other MC files: {n_mc - n_wz}")
    return hist_data, hist_wz


def _extract_flavor_rates(hist, flavor, eta_rebin=10):
    """Slice, rebin, and return (fr, fr_err, n_pass, n_total, pt_edges, eta_edges)."""
    h = hist[{
        'category': flavor,
        'variation': 'nominal',
        'n_missing_hits': sum,
        'is_barrel_lepton': sum,
    }]
    h = h[{'probe_lepton_eta': slice(0, None, complex(0, eta_rebin))}]
    hist_total = h[{'is_passing_lepton': sum}]
    hist_pass  = h[{'is_passing_lepton': 1}]
    fr, fr_err = compute_fake_rate(hist_pass, hist_total)
    return {
        'fake_rate':     fr,
        'fake_rate_err': fr_err,
        'n_pass':        hist_pass.values(),
        'n_total':       hist_total.values(),
        'pt_edges':      hist_total.axes['probe_lepton_pt'].edges,
        'eta_edges':     hist_total.axes['probe_lepton_eta'].edges,
    }


def extract_fake_rates(hist_data, hist_wz):
    """
    Extract both uncorrected (data-only) and corrected (data − WZ MC)
    fake rates from zplusl histograms.

    Correct fake rate = DATA − WZ, removing prompt WZ → ℓℓℓν contamination
    from the numerator, matching the CMS AN-18-340 (Figure 28/31) prescription.
    DY and other MC are NOT subtracted (they model real Z events, not fakes).

    Args:
        hist_data: Merged histogram from DATA files only
        hist_wz:   Merged histogram from WZ MC files only

    Returns:
        dict keyed by flavor, each containing 'corrected' and 'uncorrected' sub-dicts
    """
    fake_rates = {}
    hist_name  = "probe_lepton"
    ETA_REBIN  = 10   # 50 fine eta bins → 5 coarse bins [0,0.5,1.0,1.5,2.0,2.5]

    # Build data−WZ histogram for corrected fake rate
    hist_corrected = {}
    if hist_name in hist_data:
        hist_corrected[hist_name] = hist_data[hist_name].copy()
        if hist_name in hist_wz:
            hist_corrected[hist_name] = hist_corrected[hist_name] + (hist_wz[hist_name] * -1)
            print(f"  Applied WZ subtraction to '{hist_name}'")
        else:
            print(f"  WARNING: WZ histogram '{hist_name}' not found — using uncorrected data")

    for source_label, histograms in [('corrected', hist_corrected), ('uncorrected', hist_data)]:
        if hist_name not in histograms:
            print(f"  '{hist_name}' not in {source_label} histograms — skipping")
            continue

        hist = histograms[hist_name]
        if source_label == 'corrected':
            print(f"Histogram axes: {[ax.name for ax in hist.axes]}")

        for flavor in ['electron', 'muon']:
            result = _extract_flavor_rates(hist, flavor, ETA_REBIN)
            if flavor not in fake_rates:
                fake_rates[flavor] = {}
            fake_rates[flavor][source_label] = result

            fr = result['fake_rate']
            valid_fr = fr[fr > 0]
            print(f"  [{source_label}] {flavor}: mean f={np.mean(valid_fr):.3f}, max f={np.max(fr):.3f}")

    # Build "safe_corrected": use WZ-corrected rate where numerically stable,
    # fall back to data-only (uncorrected) where WZ over-subtracts.
    # Corrected is stable when corrected n_total > 0 AND corrected n_pass >= 0.
    # This guards against WZ MC statistical fluctuations at high pT in the SS CR.
    for flavor in ['electron', 'muon']:
        if 'corrected' not in fake_rates.get(flavor, {}) or 'uncorrected' not in fake_rates.get(flavor, {}):
            continue
        corr  = fake_rates[flavor]['corrected']
        uncorr = fake_rates[flavor]['uncorrected']
        stable = (corr['n_total'] > 0) & (corr['n_pass'] >= 0)
        n_unstable = int(np.sum(~stable))
        if n_unstable > 0:
            print(f"  [{flavor}] {n_unstable} bins with unstable WZ subtraction → falling back to uncorrected")
        safe_fr     = np.where(stable, corr['fake_rate'],     uncorr['fake_rate'])
        safe_fr_err = np.where(stable, corr['fake_rate_err'], uncorr['fake_rate_err'])
        fake_rates[flavor]['safe_corrected'] = {
            'fake_rate':     safe_fr,
            'fake_rate_err': safe_fr_err,
            'n_pass':        np.where(stable, corr['n_pass'],  uncorr['n_pass']),
            'n_total':       np.where(stable, corr['n_total'], uncorr['n_total']),
            'pt_edges':      corr['pt_edges'],
            'eta_edges':     corr['eta_edges'],
        }
        fr_valid = safe_fr[safe_fr > 0]
        print(f"  [safe_corrected] {flavor}: mean f={np.mean(fr_valid):.3f}, max f={np.max(safe_fr):.3f}")

    # Expose top-level safe_corrected values for backwards-compat with estimate_zx_background.py
    for flavor in ['electron', 'muon']:
        src = fake_rates[flavor].get('safe_corrected') or fake_rates[flavor].get('corrected') or fake_rates[flavor].get('uncorrected')
        if src:
            for k in ('fake_rate', 'fake_rate_err', 'n_pass', 'n_total', 'pt_edges', 'eta_edges'):
                fake_rates[flavor][k] = src[k]

    return fake_rates


def plot_fake_rates(fake_rates, output_dir):
    """Plot fake rates as 2D histograms."""
    os.makedirs(output_dir, exist_ok=True)

    for flavor, data in fake_rates.items():
        fig, ax = plt.subplots(figsize=(10, 8))

        # Create 2D plot
        pt_edges = data['pt_edges']
        eta_edges = data['eta_edges']
        fr = data['fake_rate']

        # Plot
        mesh = ax.pcolormesh(pt_edges, eta_edges, fr.T, cmap='viridis', vmin=0, vmax=0.5)
        plt.colorbar(mesh, ax=ax, label='Fake Rate')

        ax.set_xlabel('Probe lepton $p_T$ [GeV]')
        ax.set_ylabel('Probe lepton $|\\eta|$')
        ax.set_title(f'{flavor.capitalize()} Fake Rate')

        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'fake_rate_{flavor}.pdf'))
        plt.savefig(os.path.join(output_dir, f'fake_rate_{flavor}.png'), dpi=150)
        plt.close()

        print(f"Saved fake rate plot: {output_dir}/fake_rate_{flavor}.pdf")


def save_fake_rates(fake_rates, output_path):
    """Save fake rates to pickle file."""
    with open(output_path, 'wb') as f:
        pickle.dump(fake_rates, f)
    print(f"Saved fake rates to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Compute fake rates from zplusl workflow')
    parser.add_argument('--input', type=str, required=True,
                        help='Input directory with zplusl output')
    parser.add_argument('--year', type=str, default='2022postEE',
                        help='Year/era')
    parser.add_argument('--workflow', type=str, default='zplusl_ss',
                        choices=['zplusl_ss', 'zplusl_os'],
                        help='Workflow name')
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory for fake rate files')
    args = parser.parse_args()

    if args.output is None:
        args.output = os.path.join(args.input, 'fake_rates', args.year)

    print(f"Loading histograms from: {args.input}")
    print(f"Year: {args.year}")
    print(f"Workflow: {args.workflow}")

    # Load histograms (DATA-only and WZ MC-only)
    hist_data, hist_wz = load_zplusl_histograms(args.input, args.year, args.workflow)
    print(f"Loaded {len(hist_data)} histogram types (data), {len(hist_wz)} (WZ MC)")

    # Extract both corrected (data−WZ) and uncorrected (data-only) fake rates
    fake_rates = extract_fake_rates(hist_data, hist_wz)

    # Save first (always), then plot
    os.makedirs(args.output, exist_ok=True)
    save_fake_rates(fake_rates, os.path.join(args.output, 'fake_rates.pkl'))

    # Plot
    plot_fake_rates(fake_rates, args.output)

    print("\nDone!")


if __name__ == '__main__':
    main()
