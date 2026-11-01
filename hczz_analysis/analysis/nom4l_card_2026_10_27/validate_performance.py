#!/usr/bin/env python3
"""
2026-10-27: does the scoring of the card inputs perform like the model did in training? (user)

(a) training-like sample: NFILES random files of the training filelist (hplusc_mva_4class), 90 <= m4l < 160,
    scored here with MVAPostProcessor(CONFIG, MODEL, era); labels = the training truth per process (read from the
    b-hive dataset npz 'truth' of each process prefix). Accuracy should be close to the recorded validation
    accuracy (validation_metrics.npz) -> the scoring code builds the inputs the model was trained on.
(b) card inputs: the scored nominal MC (base/*.parquet) of each SCORED dir, 90 <= m4l < 160, same labels.
Metrics (unweighted, as the training accuracy): accuracy, per-class recall, AUC Signal vs each class and vs all.
FAIL if (a) accuracy is off the recorded validation accuracy by > 0.03, or if an AUC of (b) differs from (a) by > 0.05.

usage: validate_performance.py <out.json> --config CFG --model MODEL --dataset DATASET_DIR --metrics VAL_NPZ
                               --filelist FILELIST --scored DIR [DIR ...] [--nfiles 300]
"""
import os, re, sys, json, random, argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd

H = "/eos/user/s/snandaku/Higgscharmnew/higgscharm"
sys.path.insert(0, H)
from analysis.postprocess.mva_inference import MVAPostProcessor

ap = argparse.ArgumentParser()
ap.add_argument("out")
for k in ("--config", "--model", "--dataset", "--metrics", "--filelist"):
    ap.add_argument(k, required=True)
ap.add_argument("--scored", nargs="+", required=True)
ap.add_argument("--nfiles", type=int, default=300)
a = ap.parse_args()
CLASSES = ["qqZZ", "ggZZ", "Signal", "Other_Higgs"]
ERAS = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]

# process prefix -> class, from the training dataset's own truth
label = {}
for f in sorted(os.listdir(a.dataset)):
    if not f.endswith(".npz"):
        continue
    pre = f.split("_eos_user_")[0]
    if pre in label:
        continue
    t = np.load(os.path.join(a.dataset, f), allow_pickle=True)["truth"]
    if len(t):
        cls = Counter(np.argmax(np.stack([t[f"is_{c}"] for c in CLASSES], 1), 1).tolist()).most_common(1)[0][0]
        label[pre] = cls
prefixes = sorted(label, key=len, reverse=True)
print("labels:", {p: CLASSES[c] for p, c in sorted(label.items())})


def class_of(name):
    """dataset dir name -> class index (longest training prefix that the name starts with)."""
    if name.startswith("Stitch3FS_cH") or name.startswith("HPlusCharm"):
        return CLASSES.index("Signal")
    for p in prefixes:
        if name.startswith(p):
            return label[p]
    return None


def era_of(path):
    return next((e for e in ERAS if f"/{e}/" in path), None)


def auc(sp, sn):
    if len(sp) == 0 or len(sn) == 0:
        return float("nan")
    x = np.concatenate([sp, sn]); r = pd.Series(x).rank().values
    return float((r[:len(sp)].sum() - len(sp) * (len(sp) + 1) / 2) / (len(sp) * len(sn)))


def metrics(S, y):
    pred = S.argmax(1)
    out = {"n": int(len(y)), "acc": float((pred == y).mean()),
           "n_per_class": {c: int((y == i).sum()) for i, c in enumerate(CLASSES)},
           "recall": {c: float((pred[y == i] == i).mean()) if (y == i).any() else None for i, c in enumerate(CLASSES)}}
    sig = S[:, 2]
    out["auc_sig_vs_all"] = auc(sig[y == 2], sig[y != 2])
    for i, c in enumerate(CLASSES):
        if c != "Signal":
            out[f"auc_sig_vs_{c}"] = auc(sig[y == 2], sig[y == i])
    return out


procs = {}


def score_file(path):
    era = era_of(path)
    df = pd.read_parquet(path)
    m = df["zz_mass_inclusive"].values
    df = df[(m >= 90) & (m < 160)]
    if len(df) == 0:
        return None
    if era not in procs:
        procs[era] = MVAPostProcessor(bhive_config_path=a.config, bhive_model_path=a.model, apply_mass_window=False, era=era)
    return procs[era].predict(procs[era].prepare_features(df))["scores"]


# (a)
files = [l.strip() for l in open(a.filelist) if l.strip()]
random.Random(7).shuffle(files)
S_a, y_a = [], []
for f in files:
    if len(S_a) >= a.nfiles:
        break
    name = f.split("/")[-3]
    c = class_of(re.sub(r"_\d+$", "", name))
    if c is None or era_of(f) is None or not os.path.exists(f):
        continue
    s = score_file(f)
    if s is not None:
        S_a.append(s); y_a.append(np.full(len(s), c))
ref = metrics(np.concatenate(S_a), np.concatenate(y_a))
val = np.load(a.metrics)["acc"]
ref["recorded_val_acc_best"] = float(val.max()); ref["recorded_val_acc_last"] = float(val[-1])
print("\n(a) training-like sample:", json.dumps(ref, indent=1))

# (b)
res = {"training_like": ref, "card": {}, "ok": True, "failures": []}
if abs(ref["acc"] - ref["recorded_val_acc_last"]) > 0.03:
    res["ok"] = False; res["failures"].append(f"(a) acc {ref['acc']:.3f} vs recorded val acc {ref['recorded_val_acc_last']:.3f}")
S_b, y_b = [], []


def read_scored(path):
    df = pd.read_parquet(path, columns=["zz_mass_inclusive"] + [f"mva_score_{c}" for c in CLASSES])
    m = df["zz_mass_inclusive"].values
    return df[(m >= 90) & (m < 160)][[f"mva_score_{c}" for c in CLASSES]].values


for root in a.scored:
    tasks = []
    for era in ERAS:
        for d in sorted(os.listdir(os.path.join(root, era))):
            c = class_of(re.sub(r"_\d+$", "", d))
            base = os.path.join(root, era, d, "base")
            if c is None or not os.path.isdir(base):
                continue
            tasks += [(os.path.join(base, f), c) for f in os.listdir(base) if f.endswith(".parquet")]
    with ThreadPoolExecutor(16) as ex:
        for (p, c), s in zip(tasks, ex.map(lambda t: read_scored(t[0]), tasks)):
            if len(s):
                S_b.append(s); y_b.append(np.full(len(s), c))
card = metrics(np.concatenate(S_b), np.concatenate(y_b))
res["card"] = card
print("\n(b) card inputs:", json.dumps(card, indent=1))
for k in [k for k in ref if k.startswith("auc_")]:
    if not (abs(card[k] - ref[k]) <= 0.05):
        res["ok"] = False; res["failures"].append(f"{k}: card {card[k]:.3f} vs training-like {ref[k]:.3f}")
json.dump(res, open(a.out, "w"), indent=1)
print("\nPERFORMANCE CHECK", "PASSED" if res["ok"] else "FAILED", res["failures"], "->", a.out)
sys.exit(0 if res["ok"] else 1)
