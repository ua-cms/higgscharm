"""
fit_ZL.py

Parquet translation of the ROOT "FR_MissingHits_Fits.root" script: for each
(eta, pT) bin, computes the fake rate and average missing-hits in four Z-mass
windows, then fits a straight line (fake rate vs avg missing-hits) through
those points. Electron-only, same as the original -- missing hits is a
conversion-rejection variable, not meaningful for muons.

  ROOT                                            -> parquet
  --------------------------------------------------------------------------
  ZLalle_pt2/eta2/lostHits (all probe electrons)   -> probe_lepton_pt/eta/
                                                       n_missing_hits, masked
  ZLpasse_pt2/eta2 (passing probe electrons)       -> same events, masked
                                                       additionally by
                                                       is_passing_lepton == 1
  ZLalle_mass / ZLpasse_mass (dilepton mass)       -> zcand_mass
  ZLalle_trimass / ZLpasse_trimass (3lep mass)     -> trilepton_mass
  eta_bins = [0.0, 1.479, 2.5]  (i_eta 0=barrel,   -> is_barrel_lepton == 1
              1=endcap)                               / == 0
  TF1("fit","[0]*x+[1]",0,3) + graph.Fit("Q")      -> scipy.optimize.curve_fit
  TGraphErrors / TF1, saved to .root + .png        -> pickle + matplotlib .png

Output: pickle -> { "eta{i}_pt{j}": {"points": {x,y,ex,ey}, "fit": (a,b),
                                       "fit_err": (da,db)} }
(same "eta{i}_pt{j}" key convention as the original, eta_idx 0=EB, 1=EE, to
line up with the next stage, fit_ZLL, which applies this fit to the SS CR.)
"""

import argparse
import math
import os
import pickle

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

from fill_fr_histos import discover_data_samples, load_category_events, get_col

MASS_WINDOWS = {
    "40_120": ("zcand_mass", 40.0, 120.0),
    "60_120": ("zcand_mass", 60.0, 120.0),
    "91.2_7": ("zcand_mass", 84.2, 98.2),
    "91.2_5": ("trilepton_mass", 86.2, 96.2),
}

# eta_idx 0 -> barrel (EB), eta_idx 1 -> endcap (EE) -- matches the original
# ROOT script's eta_bins = [0.0, 1.479, 2.5] ordering (i_eta=0 is the first,
# lower-|eta| bin).
ETA_IDX_TO_IS_BARREL = {0: 1, 1: 0}


def linear(x, a, b):
    return a * x + b


def load_all_electron_events(base_dir, year):
    """Sum all data groups' electron-category events into one array."""
    import awkward as ak
    groups = discover_data_samples(base_dir, year)
    arrays = []
    for gname, sample_dirs in groups.items():
        ev = load_category_events(sample_dirs, "electron")
        if ev is not None:
            arrays.append(ev)
    if not arrays:
        raise RuntimeError("No electron-category data events found")
    return ak.concatenate(arrays) if len(arrays) > 1 else arrays[0]


def fit_missing_hits(events, pt_edges):
    pt = get_col(events, "probe_lepton_pt")
    is_barrel = get_col(events, "is_barrel_lepton", is_bool=True)
    is_pass = get_col(events, "is_passing_lepton", is_bool=True)
    n_hits = get_col(events, "n_missing_hits")
    zcand_mass = get_col(events, "zcand_mass")
    trilepton_mass = get_col(events, "trilepton_mass")
    mass_by_var = {"zcand_mass": zcand_mass, "trilepton_mass": trilepton_mass}

    results = {}

    for i_eta, region_is_barrel in ETA_IDX_TO_IS_BARREL.items():
        region_mask = is_barrel == region_is_barrel

        for i_pt in range(len(pt_edges) - 1):
            pt_lo, pt_hi = pt_edges[i_pt], pt_edges[i_pt + 1]
            if i_pt < len(pt_edges) - 2:
                pt_mask = (pt > pt_lo) & (pt <= pt_hi)
            else:
                pt_mask = pt > pt_lo  # last bin: open-ended, same as original

            xs, ys, exs, eys = [], [], [], []

            for _, (mass_field, mz_lo, mz_hi) in MASS_WINDOWS.items():
                mass = mass_by_var[mass_field]
                mass_mask = (mass > mz_lo) & (mass < mz_hi)

                all_mask = region_mask & pt_mask & mass_mask
                pass_mask = all_mask & (is_pass == 1)

                n_all = int(np.sum(all_mask))
                n_pass = int(np.sum(pass_mask))
                if n_all == 0:
                    continue
                n_fail = n_all - n_pass

                hits_all = n_hits[all_mask]
                hits_all = hits_all[~np.isnan(hits_all)]
                if len(hits_all) == 0:
                    continue
                lost_hits_sum = float(np.sum(hits_all))
                avg_hits = lost_hits_sum / n_all

                fr = n_pass / n_all
                fr_err = math.sqrt(max(n_pass, 0) * max(n_fail, 0)) / (n_all ** 2)
                avg_hits_err = math.sqrt(
                    (1.0 / n_all ** 2) * lost_hits_sum
                    + ((lost_hits_sum / n_all ** 2) ** 2) * n_all
                )

                xs.append(avg_hits)
                exs.append(avg_hits_err)
                ys.append(fr)
                eys.append(fr_err)

            key = f"eta{i_eta}_pt{i_pt}"
            if len(xs) < 2:
                print(f"[WARNING] {key}: only {len(xs)} usable mass-window point(s), skipping fit")
                continue

            xs_a, ys_a = np.array(xs), np.array(ys)
            try:
                popt, pcov = curve_fit(linear, xs_a, ys_a, p0=[1.0, 0.0])
            except Exception as exc:
                print(f"[WARNING] fit failed for {key}: {exc}")
                continue

            results[key] = {
                "points": {"x": xs_a, "y": ys_a, "ex": np.array(exs), "ey": np.array(eys)},
                "fit": tuple(popt),
                "fit_err": tuple(np.sqrt(np.diag(pcov))),
            }
            print(f"[INFO] {key}: pt=({pt_lo:.0f},{pt_hi:.0f}) n_pts={len(xs)} "
                  f"fit a={popt[0]:.4f} b={popt[1]:.4f}")

    return results


def plot_fits(results, outdir):
    os.makedirs(outdir, exist_ok=True)
    for key, entry in results.items():
        pts = entry["points"]
        a, b = entry["fit"]

        fig, ax = plt.subplots(figsize=(5, 4))
        ax.errorbar(pts["x"], pts["y"], xerr=pts["ex"], yerr=pts["ey"],
                     fmt="o", color="tab:blue", markersize=5)
        if len(pts["x"]) > 0:
            xline = np.linspace(0, max(pts["x"].max() * 1.2, 1.0), 50)
            ax.plot(xline, linear(xline, a, b), color="tab:red", linewidth=1.5)
        ax.set_xlabel("Average Missing Hits")
        ax.set_ylabel("Fake Rate")
        ax.set_title(key)
        fig.tight_layout()
        fig.savefig(os.path.join(outdir, f"fr_{key}.png"), dpi=150)
        plt.close(fig)
    print(f"[INFO] Saved {len(results)} fit plot(s) -> {outdir}")


def main():
    p = argparse.ArgumentParser(description="Fit fake rate vs avg missing-hits per (eta, pT) bin, electron only")
    p.add_argument("--base-dir", required=True, help="zplusl_ss base dir")
    p.add_argument("--year", required=True)
    p.add_argument("--pt-edges", type=float, nargs="+", default=[7, 10, 20, 30, 40, 50, 80],
                    help="electron pT bin edges (default matches the original ROOT script -- no 5 GeV bin)")
    p.add_argument("--outdir", default="fit_ZL_plots")
    p.add_argument("--output", default="fit_ZL.pkl")
    args = p.parse_args()

    pt_edges = np.array(args.pt_edges, dtype=float)

    events = load_all_electron_events(args.base_dir, args.year)
    print(f"[INFO] Loaded {len(events)} electron-category events")

    results = fit_missing_hits(events, pt_edges)
    plot_fits(results, args.outdir)

    with open(args.output, "wb") as f:
        pickle.dump(results, f)
    print(f"[INFO] Saved -> {args.output}")


if __name__ == "__main__":
    main()