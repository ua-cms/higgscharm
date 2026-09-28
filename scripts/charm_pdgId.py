#!/usr/bin/env python3
import numpy as np
import awkward as ak
import pyarrow.dataset as ds
import matplotlib.pyplot as plt
from collections import Counter

BASE     = "/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/"
sig_file = BASE + "SomeSMSignal/base"

FLAG_IS_HARD_PROCESS = (1 << 7)
FLAG_IS_LAST_COPY    = (1 << 13)
CHARM_PDGID          = 4

print("Loading...")
table  = ds.dataset(sig_file, format="parquet").to_table()
arrays = ak.from_arrow(table)
n_evt  = len(arrays["genpart_pdgId"])
print(f"  Events: {n_evt}")

mother_pdgids_hard     = []   # isHard only
mother_pdgids_last     = []   # isLastCopy only
mother_pdgids_either   = []   # isHard OR isLastCopy
mother_pdgids_all      = []   # no flag requirement at all

counts = {
    "all_charms":         0,
    "isHard_only":        0,
    "isLast_only":        0,
    "isHard_AND_isLast":  0,
    "isHard_OR_isLast":   0,
}

for ievt in range(n_evt):
    pdg          = np.array(arrays["genpart_pdgId"][ievt],            dtype=int)
    mother_idx   = np.array(arrays["genpart_genPartIdxMother"][ievt], dtype=int)
    status_flags = np.array(arrays["genpart_statusFlags"][ievt],      dtype=int)

    is_hard      = (status_flags & FLAG_IS_HARD_PROCESS).astype(bool)
    is_last_copy = (status_flags & FLAG_IS_LAST_COPY).astype(bool)

    all_charm   = np.where(np.abs(pdg) == CHARM_PDGID)[0]
    hard_charm  = np.where((np.abs(pdg) == CHARM_PDGID) & is_hard)[0]
    last_charm  = np.where((np.abs(pdg) == CHARM_PDGID) & is_last_copy)[0]
    both_charm  = np.where((np.abs(pdg) == CHARM_PDGID) & is_hard & is_last_copy)[0]
    either_charm = np.where((np.abs(pdg) == CHARM_PDGID) & (is_hard | is_last_copy))[0]

    counts["all_charms"]        += len(all_charm)
    counts["isHard_only"]       += len(hard_charm)
    counts["isLast_only"]       += len(last_charm)
    counts["isHard_AND_isLast"] += len(both_charm)
    counts["isHard_OR_isLast"]  += len(either_charm)

    def get_mother_pdg(indices):
        result = []
        for ci in indices:
            midx = int(mother_idx[ci])
            if 0 <= midx < len(pdg):
                result.append(abs(int(pdg[midx])))
        return result

    mother_pdgids_hard.extend(get_mother_pdg(hard_charm))
    mother_pdgids_last.extend(get_mother_pdg(last_charm))
    mother_pdgids_either.extend(get_mother_pdg(either_charm))
    mother_pdgids_all.extend(get_mother_pdg(all_charm))

# Summary
print("\n=== Charm quark counts across all events ===")
for label, cnt in counts.items():
    print(f"  {label:<25}: {cnt}")

print("\n=== Mother pdgId (isHard only) ===")
for pid, cnt in Counter(mother_pdgids_hard).most_common(10):
    print(f"  |pdgId|={pid:<6}  count={cnt}")

print("\n=== Mother pdgId (isLastCopy only) ===")
for pid, cnt in Counter(mother_pdgids_last).most_common(10):
    print(f"  |pdgId|={pid:<6}  count={cnt}")

print("\n=== Mother pdgId (no flag requirement) ===")
for pid, cnt in Counter(mother_pdgids_all).most_common(10):
    print(f"  |pdgId|={pid:<6}  count={cnt}")

# Plot all 3 side by side
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle("Mother pdgId of charm quarks under different flag selections",
             fontsize=12, fontweight="bold")

for ax, data, title in zip(
    axes,
    [mother_pdgids_hard, mother_pdgids_last, mother_pdgids_all],
    ["isHard only", "isLastCopy only", "No flag requirement"],
):
    if not data:
        ax.text(0.5, 0.5, "No entries", ha="center", va="center",
                transform=ax.transAxes)
        ax.set_title(title)
        continue
    unique, cnts = np.unique(data, return_counts=True)
    order = np.argsort(cnts)[::-1][:15]
    ax.bar(range(len(order)), cnts[order],
           tick_label=[str(unique[i]) for i in order],
           color="steelblue", alpha=0.8)
    ax.set_xlabel("|pdgId| of mother")
    ax.set_ylabel("Count")
    ax.set_title(f"Charm mother pdgId\n({title})")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(alpha=0.3, axis="y")

plt.tight_layout()
plt.savefig("charm_mother_pdgid_breakdown.png", dpi=150)
plt.close()
print("\n  Saved: charm_mother_pdgid_breakdown.png")