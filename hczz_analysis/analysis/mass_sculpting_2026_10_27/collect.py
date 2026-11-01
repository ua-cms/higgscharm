#!/usr/bin/env python3
"""Collect (m4l, mva_score_Signal, weight) for qqZZ and ggZZ, 2022+2023, production SM H+c MVA
(train_loose_oldcwithmassbal, scored_v1), normalised as the datacard builder (bookkeeping.py)."""
import os, sys
from concurrent.futures import ThreadPoolExecutor
import numpy as np
REPO = "/eos/user/s/snandaku/Higgscharmnew/higgscharm"
sys.path.insert(0, os.path.join(REPO, "analysis/postprocess")); sys.path.insert(0, REPO)
import bookkeeping as bk

ERAS = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]
SCORED = os.environ.get("SCORED", "/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1")
SUMW = "/eos/user/s/snandaku/Analysis/v4v3_card_2026_10_23/sumw_table.json"
PROCS = ("qqZZ", "ggZZ")
COLS = ["zz_mass_inclusive", "z1_mass", "z2_mass", "mva_score_Signal", "mva_score_qqZZ", "mva_score_ggZZ", "mva_score_Other_Higgs", "weight_nominal"]

bk.configure(ERAS)
sumw = bk.load_sumw_table(SUMW)
E, dc = bk.E, bk.dc
tasks = []
for era in ERAS:
    for name in E.listdir(os.path.join(SCORED, era)):
        proc = bk.process_of(name, [], "background")
        if proc not in PROCS:
            continue
        base = os.path.join(SCORED, era, name, "base")
        if not E.isdir(base):
            continue
        s = dc.event_scale(era, name, sumw)
        tasks += [(proc, era, f, s) for f in E.files(base, ".parquet")]
print(len(tasks), "files")

def one(t):
    proc, era, f, s = t
    df = bk._read_file(f, COLS)
    return proc, era, df, s

out = {p: {c: [] for c in COLS + ["w", "era"]} for p in PROCS}
with ThreadPoolExecutor(16) as ex:
    for proc, era, df, s in ex.map(one, tasks):
        for c in COLS:
            out[proc][c].append(df[c].values.astype(float))
        out[proc]["w"].append(df["weight_nominal"].fillna(0).values * s)
        out[proc]["era"].append(np.full(len(df), ERAS.index(era)))
here = os.path.dirname(os.path.abspath(__file__))
for p in PROCS:
    arr = {c: np.concatenate(v) for c, v in out[p].items()}
    np.savez(os.path.join(here, f"events_{p}.npz"), **arr)
    print(p, len(arr["w"]), "events, yield", arr["w"].sum(),
          "window", arr["w"][(arr["zz_mass_inclusive"] >= 105) & (arr["zz_mass_inclusive"] < 160)].sum())
