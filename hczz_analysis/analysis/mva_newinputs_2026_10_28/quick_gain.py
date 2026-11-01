"""2026-10-28: quick estimate of the gain from the new inputs before a full training: HistGradientBoosting,
S vs qqZZ / ggZZ / OH, 5-fold CV AUC, feature sets: base (massinvlep-like: lepton pt/m4l, eta, phi, charge; Z pt/m4l;
pT4l/m4l, eta4l; 3 jets pt/eta/phi + PNet CvL/CvB/B; n_cjet, jet_ht, cjet_HT, deltaR_ZZ, cjets_h_dphi), +angles, +jetkin, +both."""
import sys, numpy as np, awkward as ak
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import cross_val_score, StratifiedKFold
sys.path.insert(0, '/eos/user/s/snandaku/b-hive_ttcc/utils'); import hczz_extra_features as X
fl = [l.strip() for l in open('/eos/user/s/snandaku/b-hive_ttcc/filelists/hplusc_mva_4class_train.txt') if l.strip()]
cols = ['m4l','pT_4l','eta_4l','phi_4l','lepton_pt','lepton_eta','lepton_phi','lepton_mass','lepton_charge','z_pt','z_eta',
        'jet_pt','jet_eta','jet_phi','jet_btagPNetCvL','jet_btagPNetCvB','jet_btagPNetB','cjets_pt','cjets_eta','cjets_phi',
        'n_cjet','jet_ht','cjet_HT','deltaR_ZZ','cjets_h_dphi_inclusive']
def pad(x,n): return ak.to_numpy(ak.fill_none(ak.pad_none(x, n, clip=True), 0.0)).astype(float)
def load(pat, nmax):
    B, A, J = [], [], []
    for f in [x for x in fl if pat in x][:nmax]:
        e = ak.from_parquet(f, columns=cols)
        if len(e)==0: continue
        sc = lambda k: ak.to_numpy(ak.fill_none(ak.firsts(e[k]) if e[k].ndim > 1 else e[k], np.nan)).astype(float)
        m = sc('m4l'); keep=(m>=105)&(m<160)&(ak.to_numpy(ak.num(e.lepton_pt))==4)
        if not keep.any(): continue
        e=e[keep]; m=m[keep]
        lp=pad(e.lepton_pt,4); jp=pad(e.jet_pt,3)
        base=np.column_stack([lp/m[:,None], pad(e.lepton_eta,4), pad(e.lepton_phi,4), pad(e.lepton_charge,4), pad(e.z_pt,2)/m[:,None], pad(e.z_eta,2),
              sc('pT_4l')/m, sc('eta_4l'), jp, pad(e.jet_eta,3), pad(e.jet_phi,3), pad(e.jet_btagPNetCvL,3), pad(e.jet_btagPNetCvB,3), pad(e.jet_btagPNetB,3),
              sc('n_cjet'), sc('jet_ht'), sc('cjet_HT'), sc('deltaR_ZZ'), sc('cjets_h_dphi_inclusive')])
        r = X.compute_all(lp, pad(e.lepton_eta,4), pad(e.lepton_phi,4), pad(e.lepton_mass,4), pad(e.lepton_charge,4), sc('pT_4l'), sc('eta_4l'), sc('phi_4l'),
              pad(e.jet_pt,1)[:,0], pad(e.jet_eta,1)[:,0], pad(e.jet_phi,1)[:,0], pad(e.cjets_pt,1)[:,0], pad(e.cjets_eta,1)[:,0], pad(e.cjets_phi,1)[:,0])
        B.append(base); A.append(np.column_stack([r[k] for k in X.ANGLE_FEATURES])); J.append(np.column_stack([r[k] for k in X.JET_FEATURES]))
    return np.vstack(B), np.vstack(A), np.vstack(J)
S = load('HPlusCharm', 400); print('S', len(S[0]))
bk = {'qqZZ': load('ZZto4L', 250), 'ggZZ': load('Contin', 250), 'OH': load('GluGluHtoZZ', 120)}
cv = StratifiedKFold(5, shuffle=True, random_state=1)
for b, Bk in bk.items():
    n = min(len(S[0]), len(Bk[0])); rng = np.random.default_rng(0)
    i = rng.choice(len(S[0]), n, replace=False); j = rng.choice(len(Bk[0]), n, replace=False)
    out = []
    for name, sel in (('base', (0,)), ('+angles', (0,1)), ('+jetkin', (0,2)), ('+both', (0,1,2))):
        Xs = np.vstack([np.hstack([S[k][i] for k in sel]), np.hstack([Bk[k][j] for k in sel])]); y = np.r_[np.ones(n), np.zeros(n)]
        sc_ = cross_val_score(HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05), Xs, y, cv=cv, scoring='roc_auc')
        out.append(f"{name} {sc_.mean():.4f}±{sc_.std()/np.sqrt(5):.4f}")
    print(f"S vs {b} (n={n} each):", ' | '.join(out))
