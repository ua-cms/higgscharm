import argparse
import glob
import os
import pickle
import re

import awkward as ak
import numpy as np
import yaml

# ---------------------------------------------------------------------------
# Constants matching the ROOT scripts exactly
# ---------------------------------------------------------------------------

FLAVOR_CATEGORY = {"electron": "electron", "muon": "muon"}
REGION_LABELS = ["endcap", "barrel"]  # column order in all (n_pt, 2) arrays

_CHUNK_SUFFIX_RE = re.compile(r"_\d+$")
DATA_PREFIXES = ("Muon", "DoubleMuon", "EGamma", "SingleMuon", "MuonEG")


def group_name(sample_dir_name):
    return _CHUNK_SUFFIX_RE.sub("", sample_dir_name)


def is_data_sample(sample_dir_name):
    return sample_dir_name.startswith(DATA_PREFIXES)


# ---------------------------------------------------------------------------
# Sample discovery
# ---------------------------------------------------------------------------

def discover_data_samples(base_dir, year):
    """Returns dict: group_name -> list of sample directory paths (data only)."""
    year_dir = os.path.join(base_dir, year)
    groups = {}
    for entry in sorted(os.listdir(year_dir)):
        full = os.path.join(year_dir, entry)
        if os.path.isdir(full) and is_data_sample(entry):
            groups.setdefault(group_name(entry), []).append(full)
    return groups


def discover_mc_sample(base_dir, year, sample_name):
    """Returns list of directory paths matching a given MC sample name (chunked or not)."""
    year_dir = os.path.join(base_dir, year)
    dirs = []
    for entry in sorted(os.listdir(year_dir)):
        full = os.path.join(year_dir, entry)
        if os.path.isdir(full) and group_name(entry) == sample_name:
            dirs.append(full)
    return dirs


def load_category_events(sample_dirs, category):
    files = []
    for d in sample_dirs:
        cat_dir = os.path.join(d, category)
        if os.path.isdir(cat_dir):
            files.extend(sorted(glob.glob(os.path.join(cat_dir, "*.parquet"))))
    if not files:
        return None
    arrays = [ak.from_parquet(f) for f in files]
    return ak.concatenate(arrays) if len(arrays) > 1 else arrays[0]


# ---------------------------------------------------------------------------
# Histogram-with-overflow (replacement for utils.add_overflow + Histo1D)
# ---------------------------------------------------------------------------

def hist_with_overflow(values, edges, weights=None):
    values = np.asarray(values, dtype=float)
    if weights is None:
        weights = np.ones(len(values), dtype=float)
    else:
        weights = np.asarray(weights, dtype=float)

    idx = np.digitize(values, edges) - 1          # -1 = underflow, nbins = overflow
    idx = np.clip(idx, 0, len(edges) - 2)          # fold under/overflow into edge bins

    counts = np.zeros(len(edges) - 1, dtype=float)
    np.add.at(counts, idx, weights)
    return counts


# ---------------------------------------------------------------------------
# process_region() translation: builds "all"/"pass" pT counts for one
# (flavor, region), for both DATA and WZ, in the fixed Z-mass window.
# ---------------------------------------------------------------------------

def get_col(events, name, is_bool=False):
    """Extract a flat, length-N numpy array for a field that may be stored
    as a scalar-per-event OR as a length-<=1 jagged list per event (the
    parquet equivalent of the ROOT branches' `[0]` leading-candidate
    indexing). Missing entries become NaN (float columns) or 0 (bool/int
    columns, treated as failing whatever cut uses them)."""
    arr = events[name]
    try:
        arr = ak.firsts(arr, axis=1)  # jagged (var * T) -> option[T], one per event
    except Exception:
        pass  # already flat (depth 1, e.g. zcand_mass: ?float64) -- use as-is

    np_arr = ak.to_numpy(arr)
    if np.ma.isMaskedArray(np_arr):
        fill = 0 if is_bool else np.nan
        np_arr = np_arr.filled(fill)

    return np_arr.astype(int) if is_bool else np_arr.astype(float)


def process_region(events, pt_edges, mz_lo, mz_hi, region_is_barrel, weights=None):
    if events is None or len(events) == 0:
        return np.zeros(len(pt_edges) - 1), np.zeros(len(pt_edges) - 1)

    mass = get_col(events, "zcand_mass")
    is_barrel = get_col(events, "is_barrel_lepton", is_bool=True)
    is_pass = get_col(events, "is_passing_lepton", is_bool=True)
    pt = get_col(events, "probe_lepton_pt")

    region_val = 1 if region_is_barrel else 0
    mass_mask = (mass > mz_lo) & (mass < mz_hi)
    region_mask = is_barrel == region_val

    all_mask = mass_mask & region_mask
    pass_mask = all_mask & (is_pass == 1)

    w = None if weights is None else weights[all_mask]
    n_total = hist_with_overflow(pt[all_mask], pt_edges, weights=w)

    w_pass = None if weights is None else weights[pass_mask]
    n_pass = hist_with_overflow(pt[pass_mask], pt_edges, weights=w_pass)

    return n_total, n_pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Build DATA + WZ all/pass pT counts (parquet translation of process_region)")
    p.add_argument("--config", required=True)
    p.add_argument("--base-dir", required=True, help="zplusl_ss base dir")
    p.add_argument("--year", required=True)
    p.add_argument("--wz-sample", default="WZto3LNu")
    p.add_argument("--mass-lo", type=float, default=84.2)
    p.add_argument("--mass-hi", type=float, default=98.2)
    p.add_argument("--output", default="fr_histos.pkl")
    args = p.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    pt_edges = np.array(cfg["histogram_config"]["axes"]["probe_lepton_pt"]["edges"], dtype=float)

    data_groups = discover_data_samples(args.base_dir, args.year)
    print(f"[INFO] Found {len(data_groups)} data sample group(s)")

    wz_dirs = discover_mc_sample(args.base_dir, args.year, args.wz_sample)
    print(f"[INFO] Found {len(wz_dirs)} directory(ies) for WZ sample '{args.wz_sample}'")

    output = {}

    for flavor, category in FLAVOR_CATEGORY.items():
        n_total_data = np.zeros((len(pt_edges) - 1, 2))
        n_pass_data = np.zeros((len(pt_edges) - 1, 2))
        n_total_wz = np.zeros((len(pt_edges) - 1, 2))
        n_pass_wz = np.zeros((len(pt_edges) - 1, 2))

        # ---- DATA: sum over all data groups (unweighted, weight=1) ----
        for gname, sample_dirs in data_groups.items():
            events = load_category_events(sample_dirs, category)
            if events is None:
                continue
            for region, col in [("endcap", 0), ("barrel", 1)]:
                nt, npss = process_region(events, pt_edges, args.mass_lo, args.mass_hi,
                                           region_is_barrel=(region == "barrel"), weights=None)
                n_total_data[:, col] += nt
                n_pass_data[:, col] += npss

        # ---- WZ MC: weighted if a "weight" column exists ----
        wz_events = load_category_events(wz_dirs, category)
        if wz_events is not None:
            if "weight" in wz_events.fields:
                weights = get_col(wz_events, "weight")
            else:
                print(f"[WARNING] WZ sample ({category}) has no 'weight' column -- "
                      f"filling with weight=1. Normalization is almost certainly WRONG.")
                weights = np.ones(len(wz_events), dtype=float)
            for region, col in [("endcap", 0), ("barrel", 1)]:
                nt, npss = process_region(wz_events, pt_edges, args.mass_lo, args.mass_hi,
                                           region_is_barrel=(region == "barrel"), weights=weights)
                n_total_wz[:, col] += nt
                n_pass_wz[:, col] += npss
        else:
            print(f"[WARNING] no WZ events found for category '{category}' -- WZ counts are zero")

        output[flavor] = {
            "pt_edges": pt_edges,
            "n_total_data": n_total_data, "n_pass_data": n_pass_data,
            "n_total_wz": n_total_wz, "n_pass_wz": n_pass_wz,
        }
        print(f"[RESULT] {flavor}: DATA total={n_total_data.sum():.1f} pass={n_pass_data.sum():.1f}  "
              f"WZ total={n_total_wz.sum():.4f} pass={n_pass_wz.sum():.4f}")

    with open(args.output, "wb") as f:
        pickle.dump(output, f)
    print(f"[INFO] Saved -> {args.output}")


if __name__ == "__main__":
    main()