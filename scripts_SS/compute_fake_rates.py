"""
compute_and_plot_fr.py

Parquet translation of the other half of the two ROOT scripts:
  - compute_fake_rates_root_debug.py  -> debug_print_2d() / debug_flavor()
  - the plotting script's create_fr_plot()  -> the 4-curve FR-vs-pT plot

Reads the pkl produced by fill_fr_histos.py (raw DATA/WZ all/pass counts per
flavor and region), computes CORR = DATA - WZ exactly as the ROOT scripts do,
prints the same debug tables, and draws the same 4 curves (barrel/endcap x
uncorrected/corrected) with matplotlib instead of ROOT/cmsstyle.

No re-reading of parquet happens here -- this stage only consumes the pkl.
"""

import argparse
import pickle

import numpy as np
import matplotlib.pyplot as plt

REGION_LABELS = ["endcap", "barrel"]


# ---------------------------------------------------------------------------
# debug_print_2d / debug_flavor -- copied structure from
# compute_fake_rates_root_debug.py, unchanged formatting.
# ---------------------------------------------------------------------------

def debug_print_2d(label, arr, pt_edges, region_labels):
    pt_labels = [f"{pt_edges[i]:.0f}-{pt_edges[i+1]:.0f}" for i in range(len(pt_edges) - 1)]
    col_w = 14
    header = (f"{'pT / region':>16s}" + "".join(f"{r:>{col_w}s}" for r in region_labels)
              + f"{'SUM':>{col_w}s}")
    print(f"\n  {label}")
    print(f"  {'-' * len(header)}")
    print(f"  {header}")
    print(f"  {'-' * len(header)}")
    for i, pt_lab in enumerate(pt_labels):
        row_vals = arr[i, :]
        row_str = "".join(f"{v:>{col_w}.2f}" for v in row_vals)
        print(f"  {pt_lab:>16s}{row_str}{np.sum(row_vals):>{col_w}.2f}")
    print(f"  {'-' * len(header)}")
    col_sums = arr.sum(axis=0)
    print(f"  {'SUM':>16s}" + "".join(f"{v:>{col_w}.2f}" for v in col_sums) + f"{arr.sum():>{col_w}.2f}")
    print()


def debug_flavor(flavor, n_total_data, n_pass_data, n_total_wz, n_pass_wz,
                  n_total_corr, n_pass_corr, pt_edges, region_labels):
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  DEBUG [{flavor.upper()}]  (parquet)")
    print(sep)

    debug_print_2d("DATA  n_total (unweighted)", n_total_data, pt_edges, region_labels)
    debug_print_2d("DATA  n_pass  (unweighted)", n_pass_data, pt_edges, region_labels)
    debug_print_2d("WZ MC n_total (weighted)", n_total_wz, pt_edges, region_labels)
    debug_print_2d("WZ MC n_pass  (weighted)", n_pass_wz, pt_edges, region_labels)
    debug_print_2d("CORR  n_total = DATA - WZ", n_total_corr, pt_edges, region_labels)
    debug_print_2d("CORR  n_pass  = DATA - WZ", n_pass_corr, pt_edges, region_labels)

    with np.errstate(divide="ignore", invalid="ignore"):
        fr_data = np.where(n_total_data > 0, n_pass_data / n_total_data, np.nan)
        fr_wz = np.where(n_total_wz > 0, n_pass_wz / n_total_wz, np.nan)
        fr_corr = np.where(n_total_corr > 0, n_pass_corr / n_total_corr, np.nan)

    debug_print_2d("DATA  fake rate", fr_data, pt_edges, region_labels)
    debug_print_2d("WZ MC fake rate", fr_wz, pt_edges, region_labels)
    debug_print_2d("CORR  fake rate", fr_corr, pt_edges, region_labels)

    print(f"  SCALAR SUMMARY [{flavor}]  (parquet)")
    print(f"    DATA  total probe leptons      : {n_total_data.sum():.1f}")
    print(f"    DATA  passing probe leptons    : {n_pass_data.sum():.1f}")
    print(f"    WZ MC total (weighted)         : {n_total_wz.sum():.4f}")
    print(f"    WZ MC passing (weighted)       : {n_pass_wz.sum():.4f}")
    print(f"    WZ / DATA total ratio          : {n_total_wz.sum() / max(n_total_data.sum(), 1):.4f}")
    print()

    return fr_data, fr_wz, fr_corr


# ---------------------------------------------------------------------------
# create_fr_plot() translation -- same 4 curves, matplotlib instead of
# ROOT/cmsstyle. Binomial errors on the ratio, matching TH1::Divide's
# default (approximate) error propagation for independent pass/total.
# ---------------------------------------------------------------------------

def binomial_errors(n_pass, n_total):
    with np.errstate(divide="ignore", invalid="ignore"):
        fr = np.where(n_total > 0, n_pass / n_total, np.nan)
        err = np.where(n_total > 0, np.sqrt(fr * (1 - fr) / n_total), np.nan)
    return fr, err


def plot_fr(flavor, pt_edges, n_total_data, n_pass_data, n_total_corr, n_pass_corr, outdir):
    import os
    os.makedirs(outdir, exist_ok=True)
    centers = (pt_edges[:-1] + pt_edges[1:]) / 2
    xerr = (pt_edges[1:] - pt_edges[:-1]) / 2

    fig, ax = plt.subplots(figsize=(7, 6))
    region_style = {"endcap": ("tab:red", "s"), "barrel": ("tab:blue", "o")}
    region_idx = {"endcap": 0, "barrel": 1}

    for region, idx in region_idx.items():
        color, marker = region_style[region]

        fr_uncorr, err_uncorr = binomial_errors(n_pass_data[:, idx], n_total_data[:, idx])
        ax.errorbar(centers, fr_uncorr, xerr=xerr, yerr=err_uncorr, color=color, marker=marker,
                     linestyle="-", label=f"{region.capitalize()} Uncorrected")

        fr_corr, err_corr = binomial_errors(n_pass_corr[:, idx], n_total_corr[:, idx])
        ax.errorbar(centers, fr_corr, xerr=xerr, yerr=err_corr, color=color, marker=marker,
                     linestyle="--", label=f"{region.capitalize()} Corrected")

    ax.set_xlabel(r"$p_T(e)$ [GeV]" if flavor == "electron" else r"$p_T(\mu)$ [GeV]")
    ax.set_ylabel("Fake Rate")
    ax.set_ylim(0, 0.35)
    ax.set_xlim(pt_edges[0], pt_edges[-1])
    ax.legend(fontsize=9, frameon=False)
    ax.set_title(f"Fake rate vs $p_T$ -- {flavor}", fontsize=11)
    fig.tight_layout()

    out_path = os.path.join(outdir, f"fr_{flavor}.png")
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"[INFO] Saved plot -> {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Debug tables + FR plot from fill_fr_histos.py output")
    p.add_argument("--fr-histos", required=True, help="pkl output from fill_fr_histos.py")
    p.add_argument("--outdir", default="fr_plots")
    p.add_argument("--output", default="fr_debug.npz")
    args = p.parse_args()

    with open(args.fr_histos, "rb") as f:
        data = pickle.load(f)  # {flavor: {pt_edges, n_total_data, n_pass_data, n_total_wz, n_pass_wz}}

    npz_payload = {"region_labels": np.array(REGION_LABELS)}

    for flavor, d in data.items():
        pt_edges = d["pt_edges"]
        n_total_data, n_pass_data = d["n_total_data"], d["n_pass_data"]
        n_total_wz, n_pass_wz = d["n_total_wz"], d["n_pass_wz"]

        n_total_corr = n_total_data - n_total_wz
        n_pass_corr = n_pass_data - n_pass_wz

        debug_flavor(flavor, n_total_data, n_pass_data, n_total_wz, n_pass_wz,
                     n_total_corr, n_pass_corr, pt_edges, REGION_LABELS)

        plot_fr(flavor, pt_edges, n_total_data, n_pass_data, n_total_corr, n_pass_corr, args.outdir)

        npz_payload[f"{flavor}_pt_edges"] = pt_edges
        npz_payload[f"{flavor}_n_total_data"] = n_total_data
        npz_payload[f"{flavor}_n_pass_data"] = n_pass_data
        npz_payload[f"{flavor}_n_total_wz"] = n_total_wz
        npz_payload[f"{flavor}_n_pass_wz"] = n_pass_wz
        npz_payload[f"{flavor}_n_total_corr"] = n_total_corr
        npz_payload[f"{flavor}_n_pass_corr"] = n_pass_corr

    np.savez(args.output, **npz_payload)
    print(f"[INFO] Saved debug arrays -> {args.output}")


if __name__ == "__main__":
    main()