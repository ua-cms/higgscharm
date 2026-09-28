#!/usr/bin/env python3
"""
H+c truth diagnostic script.
For each event:
  1. Finds all last-copy Higgs and charm quarks
  2. Checks if they exist in every event
  3. Plots the pdgId of their ancestors
  4. Checks if they share a common ancestor
"""
import numpy as np
import awkward as ak
import pyarrow.dataset as ds
import matplotlib.pyplot as plt
from collections import Counter
from pathlib import Path

# -------------------------------------------------------
# Config
# -------------------------------------------------------
BASE     = "/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/"
sig_file = BASE + "SomeSMSignal/base"

HIGGS_PDGID = 25
CHARM_PDGID = 4
MAX_DEPTH   = 50

# statusFlag bit masks (NanoAOD convention)
FLAG_IS_HARD_PROCESS  = (1 << 7)
FLAG_IS_INITIAL_STATE = (1 << 11)
FLAG_IS_FIRST_COPY    = (1 << 12)
FLAG_IS_LAST_COPY     = (1 << 13)


# -------------------------------------------------------
# Utilities
# -------------------------------------------------------
def get_flags(status_flags):
    return {
        "is_hard":          (status_flags & FLAG_IS_HARD_PROCESS).astype(bool),
        "is_initial_state": (status_flags & FLAG_IS_INITIAL_STATE).astype(bool),
        "is_first_copy":    (status_flags & FLAG_IS_FIRST_COPY).astype(bool),
        "is_last_copy":     (status_flags & FLAG_IS_LAST_COPY).astype(bool),
    }


def trace_ancestors(idx, mother_idx, pdg, max_depth=MAX_DEPTH):
    """
    Walk up the mother chain from idx.
    Returns list of (index, pdgId) from idx up to beam/root.
    """
    chain   = []
    visited = set()
    n       = len(pdg)
    while 0 <= idx < n and idx not in visited:
        visited.add(idx)
        chain.append((int(idx), int(pdg[idx])))
        if abs(int(pdg[idx])) == 2212:
            break
        midx = int(mother_idx[idx])
        if midx < 0 or midx >= n:
            break
        idx = midx
        if len(chain) >= max_depth:
            break
    return chain


def find_common_ancestor(chain_a, chain_b):
    """
    Find the first common (idx, pdgId) between two ancestor chains.
    Returns (idx, pdgId) or (None, None).
    """
    set_a = {idx for idx, _ in chain_a}
    for idx, pid in chain_b:
        if idx in set_a:
            return idx, pid
    return None, None


def ancestor_pdgids(chain):
    """Return list of pdgIds in the chain, excluding the particle itself (step 0)."""
    return [pid for _, pid in chain[1:]]


# -------------------------------------------------------
# Per-event analysis
# -------------------------------------------------------
def analyse_event(ievt, arrays, verbose=False):
    """
    For one event:
      - Find last-copy Higgs and charm
      - Walk their ancestor chains
      - Check for common ancestors
    """
    pdg          = np.array(arrays["genpart_pdgId"][ievt],            dtype=int)
    mother_idx   = np.array(arrays["genpart_genPartIdxMother"][ievt], dtype=int)
    status_flags = np.array(arrays["genpart_statusFlags"][ievt],      dtype=int)
    g_pt         = np.array(arrays["genpart_pt"][ievt],               dtype=float)
    g_eta        = np.array(arrays["genpart_eta"][ievt],              dtype=float)
    g_phi        = np.array(arrays["genpart_phi"][ievt],              dtype=float)

    flags = get_flags(status_flags)

    # --- 1. Find last-copy Higgs and charm ---
    higgs_lastcopy = np.where(
        (np.abs(pdg) == HIGGS_PDGID) & flags["is_last_copy"]
    )[0]

    charm_lastcopy = np.where(
        (np.abs(pdg) == CHARM_PDGID) & flags["is_last_copy"]
    )[0]

    # Also find hard-process only versions for comparison
    higgs_hard = np.where(
        (np.abs(pdg) == HIGGS_PDGID) & flags["is_hard"]
    )[0]
    charm_hard = np.where(
        (np.abs(pdg) == CHARM_PDGID) & flags["is_hard"]
    )[0]

    # --- 2. Walk ancestor chains ---
    higgs_chains = {}
    for hi in higgs_lastcopy:
        higgs_chains[int(hi)] = trace_ancestors(hi, mother_idx, pdg)

    charm_chains = {}
    for ci in charm_lastcopy:
        charm_chains[int(ci)] = trace_ancestors(ci, mother_idx, pdg)

    # --- 3. Find common ancestors between each charm and each Higgs ---
    common_ancestors = []   # list of dicts
    for ci, cchain in charm_chains.items():
        for hi, hchain in higgs_chains.items():
            aidx, apid = find_common_ancestor(cchain, hchain)
            common_ancestors.append({
                "charm_idx":      ci,
                "higgs_idx":      hi,
                "common_idx":     aidx,
                "common_pdgId":   apid,
                "charm_chain":    cchain,
                "higgs_chain":    hchain,
                "charm_depth":    len(cchain),
                "higgs_depth":    len(hchain),
            })

    if verbose:
        print(f"\n{'='*70}")
        print(f"Event {ievt}  |  {len(pdg)} GenParts")
        print(f"  Last-copy Higgs : {len(higgs_lastcopy)} at indices {higgs_lastcopy.tolist()}")
        print(f"  Last-copy Charm : {len(charm_lastcopy)} at indices {charm_lastcopy.tolist()}")
        print(f"  Hard-proc Higgs : {len(higgs_hard)}")
        print(f"  Hard-proc Charm : {len(charm_hard)}")

        print(f"\n  --- Higgs ancestor chains ---")
        for hi, hchain in higgs_chains.items():
            print(f"  Higgs[{hi}]  pT={g_pt[hi]:.1f}  eta={g_eta[hi]:.3f}")
            for step, (idx, pid) in enumerate(hchain):
                marker = " ← START" if step == 0 else ""
                print(f"    step {step:>2}: idx={idx:>3}  pdgId={pid:>6}{marker}")

        print(f"\n  --- Charm ancestor chains ---")
        for ci, cchain in charm_chains.items():
            is_init = bool(flags["is_initial_state"][ci])
            is_hard = bool(flags["is_hard"][ci])
            print(f"  Charm[{ci}]  pdg={pdg[ci]}  pT={g_pt[ci]:.1f}"
                  f"  eta={g_eta[ci]:.3f}  isHard={is_hard}  isInitState={is_init}")
            for step, (idx, pid) in enumerate(cchain):
                marker = " ← START" if step == 0 else ""
                print(f"    step {step:>2}: idx={idx:>3}  pdgId={pid:>6}{marker}")

        print(f"\n  --- Common ancestors (charm ↔ Higgs) ---")
        for ca in common_ancestors:
            print(f"  charm[{ca['charm_idx']}] ↔ Higgs[{ca['higgs_idx']}]"
                  f"  →  common ancestor: idx={ca['common_idx']}  "
                  f"pdgId={ca['common_pdgId']}")

    return {
        "n_higgs_lastcopy":  len(higgs_lastcopy),
        "n_charm_lastcopy":  len(charm_lastcopy),
        "n_higgs_hard":      len(higgs_hard),
        "n_charm_hard":      len(charm_hard),
        "higgs_chains":      higgs_chains,
        "charm_chains":      charm_chains,
        "common_ancestors":  common_ancestors,
        "pdg":               pdg,
        "g_pt":              g_pt,
        "flags":             flags,
    }


# -------------------------------------------------------
# Main loop
# -------------------------------------------------------
def run(max_events=5000, n_verbose=5):
    print("Loading parquet...")
    table  = ds.dataset(sig_file, format="parquet").to_table()
    arrays = ak.from_arrow(table)
    n_evt  = len(arrays["genpart_pdgId"])
    n_run  = min(n_evt, max_events)
    print(f"  Total events in file : {n_evt}")
    print(f"  Events to process    : {n_run}")

    # Counters
    n_has_higgs        = 0
    n_has_charm        = 0
    n_has_both         = 0
    n_has_common_anc   = 0

    # Ancestor pdgId accumulators
    higgs_ancestor_pdgids  = []   # all ancestor pdgIds seen in Higgs chains
    charm_ancestor_pdgids  = []   # all ancestor pdgIds seen in charm chains
    common_ancestor_pdgids = []   # pdgId of the common ancestor (charm ↔ Higgs)
    common_ancestor_depths = []   # how deep into the chain the common ancestor is

    # Per-event summary for existence plot
    ev_n_higgs = []
    ev_n_charm = []

    for ievt in range(n_run):
        verbose = ievt < n_verbose
        try:
            res = analyse_event(ievt, arrays, verbose=verbose)
        except Exception as e:
            print(f"  [SKIP] Event {ievt}: {e}")
            continue

        nh = res["n_higgs_lastcopy"]
        nc = res["n_charm_lastcopy"]
        ev_n_higgs.append(nh)
        ev_n_charm.append(nc)

        has_h = nh > 0
        has_c = nc > 0

        if has_h: n_has_higgs += 1
        if has_c: n_has_charm += 1
        if has_h and has_c: n_has_both += 1

        # Collect ancestor pdgIds for Higgs chains (skip step 0 = Higgs itself)
        for hi, hchain in res["higgs_chains"].items():
            higgs_ancestor_pdgids.extend(ancestor_pdgids(hchain))

        # Collect ancestor pdgIds for charm chains (skip step 0 = charm itself)
        for ci, cchain in res["charm_chains"].items():
            charm_ancestor_pdgids.extend(ancestor_pdgids(cchain))

        # Common ancestor info
        for ca in res["common_ancestors"]:
            if ca["common_pdgId"] is not None:
                n_has_common_anc += 1
                common_ancestor_pdgids.append(ca["common_pdgId"])
                # depth = position of common ancestor in charm chain
                for step, (idx, pid) in enumerate(ca["charm_chain"]):
                    if idx == ca["common_idx"]:
                        common_ancestor_depths.append(step)
                        break

    # -------------------------------------------------------
    # Summary printout
    # -------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"SUMMARY  ({n_run} events processed)")
    print(f"{'='*60}")
    print(f"  Events with last-copy Higgs        : {n_has_higgs} / {n_run}  "
          f"({100*n_has_higgs/n_run:.1f}%)")
    print(f"  Events with last-copy Charm        : {n_has_charm} / {n_run}  "
          f"({100*n_has_charm/n_run:.1f}%)")
    print(f"  Events with BOTH                   : {n_has_both} / {n_run}  "
          f"({100*n_has_both/n_run:.1f}%)")
    print(f"  Charm-Higgs pairs with common anc  : {n_has_common_anc}")

    print(f"\n  Top-10 Higgs ancestor pdgIds:")
    for pid, cnt in Counter(np.abs(higgs_ancestor_pdgids)).most_common(10):
        print(f"    |pdgId|={pid:<6}  count={cnt}")

    print(f"\n  Top-10 Charm ancestor pdgIds:")
    for pid, cnt in Counter(np.abs(charm_ancestor_pdgids)).most_common(10):
        print(f"    |pdgId|={pid:<6}  count={cnt}")

    print(f"\n  Top-10 Common ancestor pdgIds (charm ↔ Higgs):")
    for pid, cnt in Counter(np.abs(common_ancestor_pdgids)).most_common(10):
        print(f"    |pdgId|={pid:<6}  count={cnt}")
    print(f"{'='*60}")

    # -------------------------------------------------------
    # Plots
    # -------------------------------------------------------
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("H+c Truth Diagnostic: Last-copy Higgs and Charm Ancestors",
                 fontsize=13, fontweight="bold")

    # --- Plot 1: N last-copy Higgs per event ---
    axes[0, 0].hist(ev_n_higgs, bins=np.arange(0, max(ev_n_higgs or [1])+2) - 0.5,
                    histtype="step", linewidth=2, color="steelblue")
    axes[0, 0].set_xlabel("N last-copy Higgs per event")
    axes[0, 0].set_ylabel("Events")
    axes[0, 0].set_title("Last-copy Higgs multiplicity")
    axes[0, 0].grid(alpha=0.3)

    # --- Plot 2: N last-copy Charm per event ---
    axes[0, 1].hist(ev_n_charm, bins=np.arange(0, max(ev_n_charm or [1])+2) - 0.5,
                    histtype="step", linewidth=2, color="tomato")
    axes[0, 1].set_xlabel("N last-copy Charm per event")
    axes[0, 1].set_ylabel("Events")
    axes[0, 1].set_title("Last-copy Charm multiplicity")
    axes[0, 1].grid(alpha=0.3)

    # --- Plot 3: Higgs ancestor pdgIds ---
    higgs_anc_abs = np.abs(higgs_ancestor_pdgids)
    unique_h, counts_h = np.unique(higgs_anc_abs, return_counts=True)
    order_h = np.argsort(counts_h)[::-1][:15]
    axes[0, 2].bar(range(len(order_h)),
                   counts_h[order_h],
                   tick_label=[str(unique_h[i]) for i in order_h],
                   color="steelblue", alpha=0.8)
    axes[0, 2].set_xlabel("|pdgId| of ancestor")
    axes[0, 2].set_ylabel("Count")
    axes[0, 2].set_title("Higgs ancestor pdgIds (all chain steps)")
    axes[0, 2].tick_params(axis="x", rotation=45)
    axes[0, 2].grid(alpha=0.3, axis="y")

    # --- Plot 4: Charm ancestor pdgIds ---
    charm_anc_abs = np.abs(charm_ancestor_pdgids)
    unique_c, counts_c = np.unique(charm_anc_abs, return_counts=True)
    order_c = np.argsort(counts_c)[::-1][:15]
    axes[1, 0].bar(range(len(order_c)),
                   counts_c[order_c],
                   tick_label=[str(unique_c[i]) for i in order_c],
                   color="tomato", alpha=0.8)
    axes[1, 0].set_xlabel("|pdgId| of ancestor")
    axes[1, 0].set_ylabel("Count")
    axes[1, 0].set_title("Charm ancestor pdgIds (all chain steps)")
    axes[1, 0].tick_params(axis="x", rotation=45)
    axes[1, 0].grid(alpha=0.3, axis="y")

    # --- Plot 5: Common ancestor pdgIds ---
    if common_ancestor_pdgids:
        comm_abs = np.abs(common_ancestor_pdgids)
        unique_ca, counts_ca = np.unique(comm_abs, return_counts=True)
        order_ca = np.argsort(counts_ca)[::-1][:15]
        axes[1, 1].bar(range(len(order_ca)),
                       counts_ca[order_ca],
                       tick_label=[str(unique_ca[i]) for i in order_ca],
                       color="mediumpurple", alpha=0.8)
        axes[1, 1].set_xlabel("|pdgId| of common ancestor")
        axes[1, 1].set_ylabel("Count")
        axes[1, 1].set_title("Common ancestor pdgId (charm ↔ Higgs)")
        axes[1, 1].tick_params(axis="x", rotation=45)
        axes[1, 1].grid(alpha=0.3, axis="y")
    else:
        axes[1, 1].text(0.5, 0.5, "No common ancestors found",
                        ha="center", va="center", transform=axes[1, 1].transAxes)
        axes[1, 1].set_title("Common ancestor pdgId (charm ↔ Higgs)")

    # --- Plot 6: Depth of common ancestor in charm chain ---
    if common_ancestor_depths:
        axes[1, 2].hist(common_ancestor_depths,
                        bins=np.arange(0, max(common_ancestor_depths)+2) - 0.5,
                        histtype="step", linewidth=2, color="seagreen")
        axes[1, 2].set_xlabel("Depth in charm chain where common ancestor found")
        axes[1, 2].set_ylabel("Count")
        axes[1, 2].set_title("Chain depth to common ancestor")
        axes[1, 2].grid(alpha=0.3)
    else:
        axes[1, 2].text(0.5, 0.5, "No common ancestors found",
                        ha="center", va="center", transform=axes[1, 2].transAxes)
        axes[1, 2].set_title("Chain depth to common ancestor")

    plt.tight_layout()
    plt.savefig("hc_truth_diagnostic.png", dpi=150)
    plt.close()
    print("\n  Saved: hc_truth_diagnostic.png")


if __name__ == "__main__":
    run(max_events=6000, n_verbose=5)