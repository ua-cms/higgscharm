#!/usr/bin/env python3
"""2026-10-28 (user): which inputs of the massinvlep classifier are useful? Permutation importance on the CURRENT test
split (never in its training), 90 <= m4l < 160, inputs prepared by the analysis scorer (mva_inference.prepare_features,
= training inputs, validated 98/98 on 10-27).

For each input group (one feature over all its candidates, e.g. lepton_phi = 4 columns) the group's columns are permuted
jointly across events (same permutation for all its columns), the network is re-evaluated, and the drop of each AUC
(b-hive discriminants, signal-vs-all with the production test composition) is recorded; mean +- std over NREP permutations.

Also (user): gradient importance, mean over events of |dP(signal)/dx_i| x std(x_i) (scale-free), summed over the
group's columns, for P(signal) and for the S-vs-qqZZ / S-vs-OH discriminants P(S)/(P(S)+P(k)).

usage: perm_importance.py <b-hive config> <model.pt> <out.json> [--max-files N] [--nrep 3]
"""
import argparse, collections, json, re, sys
import numpy as np, pandas as pd
from sklearn.metrics import roc_auc_score
sys.path.insert(0, '/eos/user/s/snandaku/Higgscharmnew/higgscharm')
from analysis.postprocess.mva_inference import MVAPostProcessor as MVAInference

ap = argparse.ArgumentParser(); ap.add_argument('config'); ap.add_argument('model'); ap.add_argument('out')
ap.add_argument('--max-files', type=int, default=100000); ap.add_argument('--nrep', type=int, default=3)
a = ap.parse_args()
TARGET = np.array([15068, 6894, 24998, 15069])  # production test composition qqZZ, ggZZ, S, OH
ERAS = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']
files = [l.strip() for l in open('/eos/user/s/snandaku/b-hive_ttcc/filelists/hcZZ_fs_test_20261028.txt') if l.strip()]
byera = collections.defaultdict(list)
for f in files:
    e = next((x for x in ERAS if f'/{x}/' in f), None)
    if e: byera[e].append(f)

Xs, Ts = [], []
for era, fl in byera.items():
    inf = MVAInference(a.config, a.model, apply_mass_window=True, era=era); inf._load_model()
    for f in fl[:a.max_files]:
        df = pd.read_parquet(f)
        if len(df) == 0: continue
        m4l = inf._get_col(df, ['zz_mass_inclusive', 'm4l'])
        df = df[(m4l >= 90) & (m4l < 160)]
        if len(df) == 0: continue
        t = np.argmax(np.column_stack([df[c].to_numpy().astype(bool) for c in ['is_qqZZ', 'is_ggZZ', 'is_Signal', 'is_Other_Higgs']]), 1)
        Xs.append(inf.prepare_features(df)); Ts.append(t)
    print(era, sum(len(x) for x in Xs), flush=True)
X = np.vstack(Xs).astype(np.float32); t = np.concatenate(Ts)
cfg = inf.config
layout = list(cfg.get('global_features', []))
for grp, nk in (('cpf_candidates', 'n_cpf_candidates'), ('npf_candidates', 'n_npf_candidates'),
                ('vtx_features', 'n_vtx_candidates'), ('lt_candidates', 'n_lt_candidates')):
    fs, k = list(cfg.get(grp, [])), int(cfg.get(nk, 1))
    layout += [f'{grp[:3]}:{f}' for _ in range(k) for f in fs]
assert len(layout) == X.shape[1], (len(layout), X.shape)
groups = collections.OrderedDict()
for i, n in enumerate(layout):
    groups.setdefault(n, []).append(i)
# jet tag categories as one group (they are one-hot pieces of the same tag)
cat = [n for n in groups if re.match(r'cpf:jet_is_', n)]
if cat:
    groups['cpf:jet_is_* (all tag categories)'] = sum((groups[n] for n in cat), [])
n = np.bincount(t, minlength=4); w = (TARGET / np.maximum(n, 1))[t]
print('events', len(t), 'per class', n.tolist(), flush=True)


def aucs(p):
    S = 2; r = {}
    for k, nm in ((0, 'qqZZ'), (1, 'ggZZ'), (3, 'OH')):
        m = (t == S) | (t == k)
        r[nm] = roc_auc_score(t[m] == S, p[m, S] / np.maximum(p[m, S] + p[m, k], 1e-30))
    r['all'] = roc_auc_score(t == S, p[:, S] / p.sum(1), sample_weight=w)
    return r


import torch
assert inf._model_type == 'flat', inf._model_type
model = inf._model.eval(); sd = X.std(0)
gsum = {k: np.zeros(X.shape[1]) for k in ('P(S)', 'S/qqZZ', 'S/OH')}
for i0 in range(0, len(X), 20000):
    xb = torch.from_numpy(X[i0:i0 + 20000]).float().requires_grad_(True)
    pr = torch.softmax(model(xb), 1)
    for k, f in (('P(S)', pr[:, 2]), ('S/qqZZ', pr[:, 2] / (pr[:, 2] + pr[:, 0])), ('S/OH', pr[:, 2] / (pr[:, 2] + pr[:, 3]))):
        g, = torch.autograd.grad(f.sum(), xb, retain_graph=True)
        gsum[k] += g.abs().sum(0).numpy()
grad = {g: {k: float(np.sum(gsum[k][idx] / len(X) * sd[idx])) for k in gsum} for g, idx in groups.items()}
print('gradient importance (mean |dF/dx| x std, summed over the group):', flush=True)
for g in sorted(grad, key=lambda g: -grad[g]['P(S)']):
    print(f"  {g:45s} " + ' '.join(f"{k}:{grad[g][k]:.4f}" for k in gsum), flush=True)

base = aucs(inf.predict(X)['scores'])
print('baseline', {k: round(v, 4) for k, v in base.items()}, flush=True)
rng = np.random.default_rng(2026); res = {}
for g, idx in groups.items():
    drops = []
    for _ in range(a.nrep):
        Xp = X.copy(); perm = rng.permutation(len(X)); Xp[:, idx] = X[perm][:, idx]
        r = aucs(inf.predict(Xp)['scores']); drops.append({k: base[k] - r[k] for k in base})
    res[g] = {'ncols': len(idx), **{k: [float(np.mean([d[k] for d in drops])), float(np.std([d[k] for d in drops]))] for k in base}}
    print(f"{g:45s} n={len(idx):2d} " + ' '.join(f"{k}:{res[g][k][0]:+.4f}" for k in base), flush=True)
json.dump({'gradient': grad, 'baseline': base, 'events': len(t), 'per_class': n.tolist(), 'groups': res}, open(a.out, 'w'), indent=1)
