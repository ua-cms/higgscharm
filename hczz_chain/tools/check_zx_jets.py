#!/usr/bin/env python3
"""Z+X estimate sanity check (2026-10-29 bug: the per-jet kinematics of the Z+X parquets were all NaN): every era has its
zx_*.parquet and no event with jets has NaN leading-jet kinematics.  check_zx_jets.py <zx estimate dir> <era...>"""
import glob, os, sys
import pandas as pd
bad = 0
for era in sys.argv[2:]:
    fs = sorted(glob.glob(f"{sys.argv[1]}/{era}/zx_*.parquet"))
    if not fs:
        print("NO Z+X parquet for", era); bad += 1
    for f in fs:
        t = pd.read_parquet(f, columns=["jet_multiplicity", "jet_pt_0", "jet_eta_0", "jet_phi_0"]); h = t.jet_multiplicity > 0
        fr = float(t.loc[h, ["jet_pt_0", "jet_eta_0", "jet_phi_0"]].isna().mean().max()); print(f, "events", len(t), "NaN jet kin", fr); bad += fr > 0
sys.stdout.flush(); os._exit(1 if bad else 0)
