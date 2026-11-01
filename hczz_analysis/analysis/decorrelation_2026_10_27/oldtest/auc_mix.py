#!/usr/bin/env python3
"""2026-10-28 (user): AUCs of a model on the CURRENT test split (no overlap with the new models' training), with the
class composition of the production test set behind Signal_vs_all 0.910: qqZZ/ggZZ/Signal/Other_Higgs =
15068/6894/24998/15069 (62,029 events).

Discriminants exactly as MLP_HcZZ_MW_Deep_4class.calculate_roc_list (ROCCurveTask): Signal_vs_X = P(S)/(P(S)+P(X)) on
S + X events; Signal_vs_all = P(S)/sum(P) on all events. Pairwise AUCs do not depend on the mix; Signal_vs_all does.

Reported: (raw) all events of the split, unweighted (= ROCCurveTask on it); (weighted) per-class weights so the
effective counts match the target mix; (subsample) mean +- std over N random subsamples with exactly the target counts
(only if every class has enough events).

usage: auc_mix.py <InferenceTask nominal dir with prediction.npy/truth.npy> [--n 20] [--out json]
"""
import argparse, json, os
import numpy as np
from scipy.special import softmax
from sklearn.metrics import roc_auc_score

TARGET = np.array([15068, 6894, 24998, 15069])   # qqZZ, ggZZ, Signal, Other_Higgs (production test set)
NAMES = ["qqZZ", "ggZZ", "Signal", "Other_Higgs"]

ap = argparse.ArgumentParser(); ap.add_argument("dir"); ap.add_argument("--n", type=int, default=20); ap.add_argument("--out")
a = ap.parse_args()
p = np.load(os.path.join(a.dir, "prediction.npy")); t = np.load(os.path.join(a.dir, "truth.npy"))
if t.ndim > 1: t = np.argmax(t, axis=1)
if np.abs(np.mean(p.sum(-1)) - 1) > 1e-3: p = softmax(p, axis=-1)
S = 2


def aucs(idx, w=None):
    r = {}
    tt, pp = t[idx], p[idx]; ww = None if w is None else w[idx]
    for k in (0, 1, 3):
        m = (tt == S) | (tt == k)
        d = pp[m, S] / np.maximum(pp[m, S] + pp[m, k], 1e-30)
        r[f"Signal_vs_{NAMES[k]}"] = roc_auc_score(tt[m] == S, d, sample_weight=None if ww is None else ww[m])
    r["Signal_vs_all"] = roc_auc_score(tt == S, pp[:, S] / pp.sum(1), sample_weight=ww)
    return r


n = np.bincount(t, minlength=4); allidx = np.arange(len(t))
res = {"dir": a.dir, "counts": n.tolist(), "target": TARGET.tolist()}
res["raw"] = aucs(allidx)
w = (TARGET / np.maximum(n, 1))[t]
res["weighted"] = aucs(allidx, w)
if np.all(n >= TARGET):
    rng = np.random.default_rng(2026); sub = []
    for _ in range(a.n):
        idx = np.concatenate([rng.choice(np.where(t == k)[0], TARGET[k], replace=False) for k in range(4)])
        sub.append(aucs(idx))
    res["subsample_mean"] = {k: float(np.mean([s[k] for s in sub])) for k in sub[0]}
    res["subsample_std"] = {k: float(np.std([s[k] for s in sub])) for k in sub[0]}
else:
    res["subsample_note"] = f"not enough events per class for exact subsamples (have {n.tolist()})"
print(json.dumps(res, indent=1))
if a.out: json.dump(res, open(a.out, "w"), indent=1)
