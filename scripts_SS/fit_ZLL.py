"""
fit_ZLL.py

Parquet translation of "compute_corrected_FR" / Corrected_FR_Graphs.root:
applies fit_ZL's per-(eta,pT)-bin linear fit (fake rate vs avg missing hits)
to the SS control region's OWN electron legs, producing corrected fake-rate
graphs (corrected_fr_EB / corrected_fr_EE). Electron-only, same as fit_ZL --
muon legs use the raw OS fake-rate graph directly (no missing-hits fit),
handled by whatever downstream script builds the final ZX estimate.

  ROOT (ZLLSSCR4e branch_prefix)                    -> parquet (hplus_FR_SS)
  --------------------------------------------------------------------------
  ZLLSSCR4e_lep3_pt/eta/pdgId/lostHits               -> l1_pt/eta/flavor/
  ZLLSSCR4e_lep4_pt/eta/pdgId/lostHits                  missing_hits, l2_*
  ZLLSSCR4e_mass > 100 (base selection)              -> zll_mass_{fs}_sscr
                                                         > 100, fs resolved
                                                         per-event (see below)
  fit from FR_MissingHits_Fits.root                  -> fit_ZL.py's pkl

============================== ASSUMPTIONS ==================================
- Only one category (CR_SS) exists per the config, so this script first
  tries "<sample_dir>/CR_SS/*.parquet" and falls back to
  "<sample_dir>/*.parquet" if that subfolder doesn't exist -- adjust
  CATEGORY_SUBDIR below if the real layout differs.
- is_ss requires l1.pdgId == l2.pdgId, so Z2 (the SS pair) is always
  same-flavor -- l1/l2 are never a mixed e/mu pair. Since only electron legs
  get the missing-hits correction, only electron-Z2 events matter here, and
  for those the correct per-event mass is whichever of zll_mass_4e_sscr /
  zll_mass_2mu2e_sscr is non-NaN (Z1's flavor, which picks between the two,
  isn't exposed as a separate field in this config).
- Data only (no WZ/MC), matching the original ZX estimation's use of the SS
  CR as an observed-data-driven estimate.
===============================================================================
"""

import argparse
import glob
import math
import os
import pickle

import awkward as ak
import numpy as np

from fill_fr_histos import discover_data_samples, get_col
from fit_ZL import linear, ETA_IDX_TO_IS_BARREL

CATEGORY_SUBDIR = "CR_SS"


def load_sscr_events(sample_dirs):
    files = []
    for d in sample_dirs:
        cat_dir = os.path.join(d, CATEGORY_SUBDIR)
        search_dir = cat_dir if os.path.isdir(cat_dir) else d
        files.extend(sorted(glob.glob(os.path.join(search_dir, "*.parquet"))))
    if not files:
        return None
    arrays = [ak.from_parquet(f) for f in files]
    return ak.concatenate(arrays) if len(arrays) > 1 else arrays[0]


def load_all_sscr_data(base_dir, year):
    groups = discover_data_samples(base_dir, year)
    arrays = []
    for gname, sample_dirs in groups.items():
        ev = load_sscr_events(sample_dirs)
        if ev is not None:
            arrays.append(ev)
    if not arrays:
        raise RuntimeError("No SS CR data events found")
    return ak.concatenate(arrays) if len(arrays) > 1 else arrays[0]


def resolve_electron_z2_mass(events):
    """Per-event mass for electron-flavor Z2 pairs: whichever of
    zll_mass_4e_sscr / zll_mass_2mu2e_sscr is non-NaN."""
    m_4e = get_col(events, "zll_mass_4e_sscr")
    m_2mu2e = get_col(events, "zll_mass_2mu2e_sscr")
    return np.where(~np.isnan(m_4e), m_4e, m_2mu2e)


def make_graph(x, y, ex, ey, name=""):
    return {"name": name, "x": np.asarray(x, float), "y": np.asarray(y, float),
            "ex": np.asarray(ex, float), "ey": np.asarray(ey, float)}


def compute_corrected_fr(events, fit_results, pt_edges):
    mass = resolve_electron_z2_mass(events)
    base = mass > 100

    l1_pt = get_col(events, "l1_pt")
    l1_eta = get_col(events, "l1_eta")  # config expression already takes np.abs()
    l1_flavor = get_col(events, "l1_flavor")
    l1_hits = get_col(events, "l1_missing_hits")

    l2_pt = get_col(events, "l2_pt")
    l2_eta = get_col(events, "l2_eta")
    l2_flavor = get_col(events, "l2_flavor")
    l2_hits = get_col(events, "l2_missing_hits")

    is_e_leg1 = base & (l1_flavor == 11)
    is_e_leg2 = base & (l2_flavor == 11)

    graphs = {}

    for i_eta, region_is_barrel in ETA_IDX_TO_IS_BARREL.items():
        region_name = "EB" if region_is_barrel == 1 else "EE"
        eta_lo, eta_hi = (0.0, 1.479) if region_is_barrel == 1 else (1.479, 2.5)

        xs, ys, exs, eys = [], [], [], []

        for i_pt in range(len(pt_edges) - 1):
            pt_lo, pt_hi = pt_edges[i_pt], pt_edges[i_pt + 1]
            if i_pt < len(pt_edges) - 2:
                mask1 = is_e_leg1 & (l1_pt > pt_lo) & (l1_pt <= pt_hi) & (l1_eta >= eta_lo) & (l1_eta < eta_hi)
                mask2 = is_e_leg2 & (l2_pt > pt_lo) & (l2_pt <= pt_hi) & (l2_eta >= eta_lo) & (l2_eta < eta_hi)
            else:
                mask1 = is_e_leg1 & (l1_pt > pt_lo) & (l1_eta >= eta_lo) & (l1_eta < eta_hi)
                mask2 = is_e_leg2 & (l2_pt > pt_lo) & (l2_eta >= eta_lo) & (l2_eta < eta_hi)

            hits1 = l1_hits[mask1]
            hits2 = l2_hits[mask2]
            hits1 = hits1[~np.isnan(hits1)]
            hits2 = hits2[~np.isnan(hits2)]

            n_all = len(hits1) + len(hits2)
            if n_all == 0:
                continue

            lost_hits_sum = float(np.sum(hits1)) + float(np.sum(hits2))
            avg_hits = lost_hits_sum / n_all
            avg_hits_err = math.sqrt(
                (1.0 / n_all ** 2) * lost_hits_sum
                + ((lost_hits_sum / n_all ** 2) ** 2) * n_all
            )

            fit_key = f"eta{i_eta}_pt{i_pt}"
            fit_entry = fit_results.get(fit_key)
            if fit_entry is None:
                print(f"[WARNING] no fit for {fit_key}, skipping pt bin ({pt_lo:.0f},{pt_hi:.0f}) in {region_name}")
                continue
            a, b = fit_entry["fit"]
            fake_rate = linear(avg_hits, a, b)
            fake_rate_lo = linear(avg_hits - avg_hits_err, a, b)
            fake_rate_err = abs(fake_rate - fake_rate_lo)

            xs.append((pt_lo + pt_hi) / 2)
            exs.append((pt_hi - pt_lo) / 2)
            ys.append(np.clip(fake_rate, 0.001, 0.999))
            eys.append(fake_rate_err)

            print(f"[INFO] {region_name} pt=({pt_lo:.0f},{pt_hi:.0f}): n={n_all} "
                  f"avg_hits={avg_hits:.4f} -> FR={fake_rate:.4f} +/- {fake_rate_err:.4f}")

        name = f"corrected_fr_{region_name}"
        graphs[name] = make_graph(xs, ys, exs, eys, name)

    return graphs


def main():
    p = argparse.ArgumentParser(description="Apply fit_ZL to the SS CR's own electron legs -> corrected FR graphs")
    p.add_argument("--base-dir", required=True, help="hplus_FR_SS base dir")
    p.add_argument("--year", required=True)
    p.add_argument("--fit-zl", required=True, help="pkl output from fit_ZL.py")
    p.add_argument("--pt-edges", type=float, nargs="+", default=[7, 10, 20, 30, 40, 50, 80])
    p.add_argument("--output", default="fit_ZLL.pkl")
    args = p.parse_args()

    pt_edges = np.array(args.pt_edges, dtype=float)

    with open(args.fit_zl, "rb") as f:
        fit_results = pickle.load(f)

    events = load_all_sscr_data(args.base_dir, args.year)
    print(f"[INFO] Loaded {len(events)} SS CR data events")

    graphs = compute_corrected_fr(events, fit_results, pt_edges)

    with open(args.output, "wb") as f:
        pickle.dump(graphs, f)
    print(f"[INFO] Saved -> {args.output}")


if __name__ == "__main__":
    main()