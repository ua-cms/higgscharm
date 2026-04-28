#!/usr/bin/env python3
"""
Compare two inference results to understand why they give different limits.

This script helps diagnose WHY two MVA models produce different asymptotic limits
by examining their signal/background separation and per-bin sensitivity.
"""

import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep

plt.style.use(hep.style.CMS)


def softmax(x, axis=None):
    x_max = np.max(x, axis=axis, keepdims=True)
    exp_x = np.exp(x - x_max)
    return exp_x / np.sum(exp_x, axis=axis, keepdims=True)


def load_inference(inference_path, name="Model"):
    """Load inference results and compute signal probability."""
    print(f"\nLoading {name} from: {inference_path}")

    predictions = np.load(os.path.join(inference_path, 'prediction.npy'))
    truth = np.load(os.path.join(inference_path, 'truth.npy'))

    probs = softmax(predictions, axis=1)
    signal_class_idx = 2  # From your config
    signal_prob = probs[:, signal_class_idx]

    print(f"  Total events: {len(predictions)}")
    print(f"  Signal prob range: [{signal_prob.min():.4f}, {signal_prob.max():.4f}]")

    return signal_prob, truth


def compute_separation_metrics(signal_prob, truth, bins=20):
    """Compute metrics that explain limit differences."""

    bin_edges = np.linspace(0, 1, bins + 1)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    # Class indices from your config
    class_names = ["qqZZ", "ggH", "signal", "hplusb", "other_higgs"]
    signal_idx = 2
    bkg_indices = [0, 1, 3, 4]

    # Get signal and background probabilities
    is_signal = (truth == signal_idx)
    is_bkg = np.isin(truth, bkg_indices)

    signal_probs = signal_prob[is_signal]
    bkg_probs = signal_prob[is_bkg]

    # Compute histograms (normalized to expected yields would be better, but this shows shape)
    signal_hist, _ = np.histogram(signal_probs, bins=bin_edges, density=True)
    bkg_hist, _ = np.histogram(bkg_probs, bins=bin_edges, density=True)

    # Metrics
    metrics = {}

    # 1. Signal efficiency at high MVA scores
    for threshold in [0.5, 0.7, 0.9]:
        sig_eff = (signal_probs >= threshold).sum() / len(signal_probs)
        bkg_eff = (bkg_probs >= threshold).sum() / len(bkg_probs)
        bkg_rej = 1 - bkg_eff
        metrics[f'sig_eff_{threshold}'] = sig_eff
        metrics[f'bkg_rej_{threshold}'] = bkg_rej
        metrics[f'sig_over_sqrt_bkg_{threshold}'] = sig_eff / np.sqrt(bkg_eff) if bkg_eff > 0 else np.inf

    # 2. ROC AUC (proxy for overall separation)
    from sklearn.metrics import roc_auc_score
    labels = np.concatenate([np.ones(len(signal_probs)), np.zeros(len(bkg_probs))])
    scores = np.concatenate([signal_probs, bkg_probs])
    metrics['roc_auc'] = roc_auc_score(labels, scores)

    # 3. Signal concentration in top bins
    top_bins = bin_centers >= 0.8
    metrics['signal_in_top_bins'] = signal_hist[top_bins].sum() / signal_hist.sum() if signal_hist.sum() > 0 else 0
    metrics['bkg_in_top_bins'] = bkg_hist[top_bins].sum() / bkg_hist.sum() if bkg_hist.sum() > 0 else 0

    # 4. Mean signal probability for signal vs background
    metrics['mean_prob_signal'] = signal_probs.mean()
    metrics['mean_prob_bkg'] = bkg_probs.mean()
    metrics['prob_separation'] = metrics['mean_prob_signal'] - metrics['mean_prob_bkg']

    return metrics, signal_hist, bkg_hist, bin_edges


def plot_comparison(inference1, inference2, name1, name2, output_dir):
    """Create comparison plots."""

    signal_prob1, truth1 = inference1
    signal_prob2, truth2 = inference2

    metrics1, sig_hist1, bkg_hist1, bins = compute_separation_metrics(signal_prob1, truth1)
    metrics2, sig_hist2, bkg_hist2, _ = compute_separation_metrics(signal_prob2, truth2)

    bin_centers = (bins[:-1] + bins[1:]) / 2

    # Figure 1: Distribution comparison
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))

    # Top left: Model 1 distributions
    ax = axes[0, 0]
    ax.step(bins[:-1], sig_hist1, where='post', label='Signal', color='red', linewidth=2)
    ax.step(bins[:-1], bkg_hist1, where='post', label='Background', color='blue', linewidth=2)
    ax.fill_between(bins[:-1], sig_hist1, step='post', alpha=0.3, color='red')
    ax.fill_between(bins[:-1], bkg_hist1, step='post', alpha=0.3, color='blue')
    ax.set_xlabel('MVA Signal Probability')
    ax.set_ylabel('Normalized density')
    ax.set_title(f'{name1}')
    ax.legend()
    ax.set_xlim(0, 1)

    # Top right: Model 2 distributions
    ax = axes[0, 1]
    ax.step(bins[:-1], sig_hist2, where='post', label='Signal', color='red', linewidth=2)
    ax.step(bins[:-1], bkg_hist2, where='post', label='Background', color='blue', linewidth=2)
    ax.fill_between(bins[:-1], sig_hist2, step='post', alpha=0.3, color='red')
    ax.fill_between(bins[:-1], bkg_hist2, step='post', alpha=0.3, color='blue')
    ax.set_xlabel('MVA Signal Probability')
    ax.set_ylabel('Normalized density')
    ax.set_title(f'{name2}')
    ax.legend()
    ax.set_xlim(0, 1)

    # Bottom left: Signal distribution comparison
    ax = axes[1, 0]
    ax.step(bins[:-1], sig_hist1, where='post', label=name1, color='red', linewidth=2)
    ax.step(bins[:-1], sig_hist2, where='post', label=name2, color='darkred', linewidth=2, linestyle='--')
    ax.fill_between(bins[:-1], sig_hist1, step='post', alpha=0.2, color='red')
    ax.fill_between(bins[:-1], sig_hist2, step='post', alpha=0.2, color='darkred')
    ax.set_xlabel('MVA Signal Probability')
    ax.set_ylabel('Normalized density')
    ax.set_title('Signal Distribution Comparison')
    ax.legend()
    ax.set_xlim(0, 1)
    ax.axvline(x=0.8, color='gray', linestyle=':', label='High-score region')

    # Bottom right: Background distribution comparison
    ax = axes[1, 1]
    ax.step(bins[:-1], bkg_hist1, where='post', label=name1, color='blue', linewidth=2)
    ax.step(bins[:-1], bkg_hist2, where='post', label=name2, color='darkblue', linewidth=2, linestyle='--')
    ax.fill_between(bins[:-1], bkg_hist1, step='post', alpha=0.2, color='blue')
    ax.fill_between(bins[:-1], bkg_hist2, step='post', alpha=0.2, color='darkblue')
    ax.set_xlabel('MVA Signal Probability')
    ax.set_ylabel('Normalized density')
    ax.set_title('Background Distribution Comparison')
    ax.legend()
    ax.set_xlim(0, 1)
    ax.axvline(x=0.8, color='gray', linestyle=':')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'distribution_comparison.png'), dpi=150)
    plt.savefig(os.path.join(output_dir, 'distribution_comparison.pdf'))
    print(f"\nSaved: {output_dir}/distribution_comparison.png")
    plt.close()

    # Figure 2: Metrics comparison
    fig, ax = plt.subplots(figsize=(12, 8))

    metrics_to_plot = [
        ('roc_auc', 'ROC AUC'),
        ('sig_eff_0.5', 'Signal Eff (>0.5)'),
        ('sig_eff_0.7', 'Signal Eff (>0.7)'),
        ('sig_eff_0.9', 'Signal Eff (>0.9)'),
        ('bkg_rej_0.7', 'Bkg Rejection (>0.7)'),
        ('signal_in_top_bins', 'Signal in top bins'),
        ('prob_separation', 'Mean prob separation'),
    ]

    x = np.arange(len(metrics_to_plot))
    width = 0.35

    values1 = [metrics1[m[0]] for m in metrics_to_plot]
    values2 = [metrics2[m[0]] for m in metrics_to_plot]

    bars1 = ax.bar(x - width/2, values1, width, label=name1, color='steelblue')
    bars2 = ax.bar(x + width/2, values2, width, label=name2, color='coral')

    ax.set_ylabel('Value')
    ax.set_title('Separation Metrics Comparison\n(Higher = Better for limit)')
    ax.set_xticks(x)
    ax.set_xticklabels([m[1] for m in metrics_to_plot], rotation=45, ha='right')
    ax.legend()
    ax.set_ylim(0, 1.1)

    # Add value labels
    for bar, val in zip(bars1, values1):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                f'{val:.3f}', ha='center', va='bottom', fontsize=9)
    for bar, val in zip(bars2, values2):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02,
                f'{val:.3f}', ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'metrics_comparison.png'), dpi=150)
    plt.savefig(os.path.join(output_dir, 'metrics_comparison.pdf'))
    print(f"Saved: {output_dir}/metrics_comparison.png")
    plt.close()

    return metrics1, metrics2


def print_interpretation(metrics1, metrics2, name1, name2, limit1=None, limit2=None):
    """Print interpretation of why limits differ."""

    print("\n" + "=" * 70)
    print("INTERPRETATION: Why the limits are different")
    print("=" * 70)

    print(f"\n{'Metric':<35} {name1:>15} {name2:>15} {'Better':>10}")
    print("-" * 75)

    comparisons = [
        ('ROC AUC', 'roc_auc', True),
        ('Signal Eff @ MVA>0.7', 'sig_eff_0.7', True),
        ('Bkg Rejection @ MVA>0.7', 'bkg_rej_0.7', True),
        ('Signal in top 20% bins', 'signal_in_top_bins', True),
        ('Bkg in top 20% bins', 'bkg_in_top_bins', False),  # Lower is better
        ('Mean prob (signal)', 'mean_prob_signal', True),
        ('Mean prob (background)', 'mean_prob_bkg', False),  # Lower is better
        ('Prob separation', 'prob_separation', True),
    ]

    score1, score2 = 0, 0

    for label, key, higher_better in comparisons:
        v1, v2 = metrics1[key], metrics2[key]
        if higher_better:
            better = name1 if v1 > v2 else name2
            if v1 > v2:
                score1 += 1
            else:
                score2 += 1
        else:
            better = name1 if v1 < v2 else name2
            if v1 < v2:
                score1 += 1
            else:
                score2 += 1
        print(f"{label:<35} {v1:>15.4f} {v2:>15.4f} {better:>10}")

    print("-" * 75)
    print(f"\n{'Overall score:':<35} {score1:>15} {score2:>15}")

    winner = name1 if score1 > score2 else name2
    print(f"\n→ {winner} should give a BETTER (lower) expected limit")

    print("\n" + "=" * 70)
    print("KEY INSIGHT:")
    print("=" * 70)
    print("""
The asymptotic limit depends on how well the MVA separates signal from background.
A model with:
  • Higher signal efficiency at high MVA scores
  • Lower background contamination at high MVA scores
  • Better overall ROC AUC

...will produce LOWER (better) limits because:
  1. Signal concentrates in a few high-score bins
  2. Background is suppressed in those bins
  3. The likelihood ratio test can more easily distinguish signal from background
  4. Less signal strength (μ) is needed for a significant excess
""")

    if limit1 is not None and limit2 is not None:
        print(f"\nYour observed limits: {name1}={limit1:.2f}, {name2}={limit2:.2f}")
        expected_better = name1 if score1 > score2 else name2
        actual_better = name1 if limit1 < limit2 else name2
        if expected_better == actual_better:
            print(f"✓ This matches expectations: {actual_better} has better separation AND better limit")
        else:
            print(f"⚠ Unexpected: metrics suggest {expected_better} should be better, but {actual_better} has lower limit")
            print("  This could be due to: different event counts, weight distributions, or systematics")


def main():
    if len(sys.argv) < 3:
        print("Usage: python compare_inference_sensitivity.py <inference_path1> <inference_path2> [name1] [name2] [limit1] [limit2]")
        print("\nExample:")
        print("  python compare_inference_sensitivity.py /path/to/model1/test_attack_nominal /path/to/model2/test_attack_nominal Model1 Model2 3.5 2.1")
        sys.exit(1)

    path1 = sys.argv[1]
    path2 = sys.argv[2]
    name1 = sys.argv[3] if len(sys.argv) > 3 else "Model1"
    name2 = sys.argv[4] if len(sys.argv) > 4 else "Model2"
    limit1 = float(sys.argv[5]) if len(sys.argv) > 5 else None
    limit2 = float(sys.argv[6]) if len(sys.argv) > 6 else None

    # Load both inferences
    inference1 = load_inference(path1, name1)
    inference2 = load_inference(path2, name2)

    # Create output directory
    output_dir = "/eos/user/s/snandaku/Analysis/combine/inference_comparison"
    os.makedirs(output_dir, exist_ok=True)

    # Compare
    metrics1, metrics2 = plot_comparison(inference1, inference2, name1, name2, output_dir)

    # Print interpretation
    print_interpretation(metrics1, metrics2, name1, name2, limit1, limit2)


if __name__ == "__main__":
    main()
