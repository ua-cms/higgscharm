#!/usr/bin/env python3
"""2026-10-30 feature screen (decorrelated MVA): HistGradientBoosting, 5-fold CV AUC for S vs qqZZ / ggZZ / OH.
base ~ fsextv2: leptons pt/m4l + iso/sip/mvaHZZIso, Z pt/m4l, pT4l/m4l, eta4l, deltaR_ZZ, jet_ht, cjet_HT, cjets_h_dphi,
3 jets raw pt + c-tag CATEGORY-like info (PNet CvL/CvB/B each in 5 quantile bins), + the 12 angle/jet-kinematic features.
Groups added one at a time: cont (continuous PNet scores), qg (PNet QvG), jm (jet mass + nSV), zm (z1/z2 mass / m4l),
upart (UParT CvL/CvB/B continuous instead of PNet). Also |corr(feature, m4l)| in ZZ for the new columns (sculpting risk)."""
import sys, numpy as np, awkward as ak
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import cross_val_score, StratifiedKFold
sys.path.insert(0, '/eos/user/s/snandaku/b-hive_ttcc/utils'); import hczz_extra_features as X
fl = [l.strip() for l in open('/eos/user/s/snandaku/b-hive_ttcc/filelists/hcZZ_fs_train_20261028.txt') if l.strip()]
cols0 = ['m4l','pT_4l','eta_4l','phi_4l','lepton_pt','lepton_eta','lepton_phi','lepton_mass','lepton_charge','lepton_pfRelIso03_all',
        'lepton_sip3d','lepton_mvaHZZIso','z_pt','jet_pt','jet_eta','jet_phi','jet_mass','jet_btagPNetCvL','jet_btagPNetCvB',
        'jet_btagPNetB','jet_btagPNetQvG','jet_btagUParTAK4CvL','jet_btagUParTAK4CvB','jet_btagUParTAK4B','nSV','z1_mass','z2_mass',
        'cjets_pt','cjets_eta','cjets_phi','jet_ht','cjet_HT','deltaR_ZZ','cjets_h_dphi_inclusive']
cols = [c for c in cols0 if 'UParT' not in c]
def padf(e, k, n):
    if k not in e.fields: return np.full((len(e), n), -1.0)
    if e[k].ndim == 1: return ak.to_numpy(ak.fill_none(e[k], -1.0)).astype(float)[:, None]
    return pad(e[k], n)
def pad(x, n): return ak.to_numpy(ak.fill_none(ak.pad_none(x, n, clip=True), -1.0)).astype(float)
def load(pats, nmax):
    G = {k: [] for k in ('base', 'cont', 'qg', 'jm', 'zm', 'm4l')}
    files = [x for x in fl if any(p in x for p in pats)][:nmax]
    for f in files:
        try:
            import pyarrow.parquet as pq
            av = set(pq.read_schema(f).names); e = ak.from_parquet(f, columns=[c for c in cols if c in av])
        except Exception: continue
        if len(e) == 0: continue
        sc = lambda k: ak.to_numpy(ak.fill_none(ak.firsts(e[k]) if e[k].ndim > 1 else e[k], np.nan)).astype(float)
        m = sc('m4l'); keep = (m >= 105) & (m < 160) & (ak.to_numpy(ak.num(e.lepton_pt)) == 4)
        if not keep.any(): continue
        e = e[keep]; m = m[keep]; lp = pad(e.lepton_pt, 4)
        r = X.compute_all(lp, pad(e.lepton_eta, 4), pad(e.lepton_phi, 4), pad(e.lepton_mass, 4), pad(e.lepton_charge, 4), sc('pT_4l'),
              sc('eta_4l'), sc('phi_4l'), pad(e.jet_pt, 1)[:, 0], pad(e.jet_eta, 1)[:, 0], pad(e.jet_phi, 1)[:, 0],
              pad(e.cjets_pt, 1)[:, 0], pad(e.cjets_eta, 1)[:, 0], pad(e.cjets_phi, 1)[:, 0])
        G['base'].append(np.column_stack([lp / m[:, None], pad(e.lepton_pfRelIso03_all, 4), pad(e.lepton_sip3d, 4), pad(e.lepton_mvaHZZIso, 4),
              pad(e.z_pt, 2) / m[:, None], sc('pT_4l') / m, sc('eta_4l'), sc('deltaR_ZZ'), sc('jet_ht'), sc('cjet_HT'), sc('cjets_h_dphi_inclusive'),
              pad(e.jet_pt, 3), np.digitize(pad(e.jet_btagPNetCvL, 3), [0.05, 0.15, 0.3, 0.6]), np.digitize(pad(e.jet_btagPNetCvB, 3), [0.1, 0.3, 0.5, 0.8]),
              np.digitize(pad(e.jet_btagPNetB, 3), [0.03, 0.1, 0.3, 0.7])] + [r[k] for k in X.ANGLE_FEATURES + X.JET_FEATURES]))
        G['cont'].append(np.column_stack([pad(e.jet_btagPNetCvL, 3), pad(e.jet_btagPNetCvB, 3), pad(e.jet_btagPNetB, 3)]))
        G['qg'].append(padf(e, 'jet_btagPNetQvG', 3)); G['jm'].append(np.column_stack([padf(e, 'jet_mass', 3), padf(e, 'nSV', 1)]))
        G['zm'].append(np.column_stack([sc('z1_mass') / m, sc('z2_mass') / m]))
        G['m4l'].append(m)
    return {k: np.vstack(v) if k != 'm4l' else np.concatenate(v) for k, v in G.items()}
S = load(['HPlusCharm', 'Stitch3FS'], 600); print('S', len(S['m4l']), flush=True)
BK = {'qqZZ': load(['ZZto4L'], 300), 'ggZZ': load(['Contin'], 300), 'OH': load(['GluGluHtoZZ', 'TTH', 'ZHto', 'WplusH', 'WminusH', 'VBF', 'HB', 'HPlusBottom'], 300)}
zz = {k: np.vstack([BK['qqZZ'][k], BK['ggZZ'][k]]) if k != 'm4l' else np.concatenate([BK['qqZZ'][k], BK['ggZZ'][k]]) for k in BK['qqZZ']}
for g in ('cont', 'qg', 'jm', 'zm'):
    c = [abs(np.corrcoef(zz[g][:, i], zz['m4l'])[0, 1]) for i in range(zz[g].shape[1])]
    print(f"|corr(m4l)| in ZZ  {g:6s} max {np.nanmax(c):.3f}", flush=True)
cv = StratifiedKFold(5, shuffle=True, random_state=1)
sets = {'base': ['base'], '+cont': ['base', 'cont'], '+qg': ['base', 'qg'], '+jm': ['base', 'jm'], '+zm': ['base', 'zm'], '+cont+qg+jm': ['base', 'cont', 'qg', 'jm']}
for b, Bk in BK.items():
    n = min(len(S['m4l']), len(Bk['m4l']), 60000); rng = np.random.default_rng(0)
    i = rng.choice(len(S['m4l']), n, replace=False); j = rng.choice(len(Bk['m4l']), n, replace=False)
    out = []
    for name, gs in sets.items():
        Xs = np.vstack([np.hstack([S[g][i] for g in gs]), np.hstack([Bk[g][j] for g in gs])]); y = np.r_[np.ones(n), np.zeros(n)]
        a = cross_val_score(HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05), Xs, y, cv=cv, scoring='roc_auc')
        out.append(f"{name} {a.mean():.4f}±{a.std() / np.sqrt(5):.4f}")
    print(f"S vs {b} (n={n} each): " + " | ".join(out), flush=True)
