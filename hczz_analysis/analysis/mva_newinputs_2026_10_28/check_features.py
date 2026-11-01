"""2026-10-28: validate utils/hczz_extra_features.py on the MVA parquets (test filelist): lepton-swap invariance,
per-feature separation (AUC S vs qqZZ / ggZZ / OH) and m4l correlation in qqZZ (105-160)."""
import sys, numpy as np, awkward as ak
from sklearn.metrics import roc_auc_score
sys.path.insert(0, '/eos/user/s/snandaku/b-hive_ttcc/utils'); import hczz_extra_features as X
fl = [l.strip() for l in open('/eos/user/s/snandaku/b-hive_ttcc/filelists/hplusc_mva_4class_test.txt') if l.strip()]
cols = ['m4l','pT_4l','eta_4l','phi_4l','lepton_pt','lepton_eta','lepton_phi','lepton_mass','lepton_charge','jet_pt','jet_eta','jet_phi','cjets_pt','cjets_eta','cjets_phi','is_qqZZ','is_ggZZ','is_Signal','is_Other_Higgs']
def lead(a): return ak.to_numpy(ak.fill_none(ak.firsts(a), 0.0)).astype(float)
D = {}
for cls, pat, nmax in (('Signal','HPlusCharm',60),('qqZZ','ZZto4L',60),('ggZZ','Contin',60),('OH','GluGluHtoZZ',40)):
    rows=[]
    for f in [x for x in fl if pat in x][:nmax]:
        e = ak.from_parquet(f, columns=cols)
        if len(e)==0: continue
        sc = lambda k: ak.to_numpy(ak.fill_none(ak.firsts(e[k]) if e[k].ndim > 1 else e[k], np.nan)).astype(float)
        m4l = sc('m4l'); keep = (m4l>=105)&(m4l<160)&(ak.to_numpy(ak.num(e.lepton_pt))==4)
        if not keep.any(): continue
        e = e[keep]; m4l = m4l[keep]
        a = lambda k: ak.to_numpy(ak.fill_none(ak.pad_none(e[k], 4, clip=True), 0.0)).astype(float)
        args = lambda sw: (a('lepton_pt')[:,sw],a('lepton_eta')[:,sw],a('lepton_phi')[:,sw],a('lepton_mass')[:,sw],a('lepton_charge')[:,sw],
                           sc('pT_4l'),sc('eta_4l'),sc('phi_4l'),lead(e.jet_pt),lead(e.jet_eta),lead(e.jet_phi),lead(e.cjets_pt),lead(e.cjets_eta),lead(e.cjets_phi))
        r = X.compute_all(*args([0,1,2,3]))
        if cls=='Signal' and not rows:
            r2 = X.compute_all(*args([1,0,3,2]))
            print('swap invariance max|diff|:', max(float(np.max(np.abs(r[k]-r2[k]))) for k in X.ALL_FEATURES))
        r['m4l']=m4l; rows.append(r)
    D[cls]={k:np.concatenate([r[k] for r in rows]) for k in rows[0]}
    print(cls, 'events', len(D[cls]['m4l']))
print(f"{'feature':22s} {'S range':>16s} {'AUC S/qqZZ':>10s} {'S/ggZZ':>7s} {'S/OH':>6s} {'corr m4l qqZZ':>13s}")
for k in X.ALL_FEATURES:
    s=D['Signal'][k]; row=[]
    for b in ('qqZZ','ggZZ','OH'):
        y=np.r_[np.ones(len(s)),np.zeros(len(D[b][k]))]; v=np.r_[s,D[b][k]]; au=roc_auc_score(y,v); row.append(max(au,1-au))
    q=D['qqZZ']; m=q[k]!=X.NOJET if k.startswith('kin') else np.ones(len(q[k]),bool)
    c=np.corrcoef(q[k][m],q['m4l'][m])[0,1]
    print(f"{k:22s} {s.min():7.2f}..{s.max():6.2f} {row[0]:10.3f} {row[1]:7.3f} {row[2]:6.3f} {c:13.3f}")
print('cos_theta_star |.|>0.8 fraction: S %.3f qqZZ %.3f (spin-0 flat expect 0.2)'%(np.mean(np.abs(D['Signal']['ang_cos_theta_star'])>0.8),np.mean(np.abs(D['qqZZ']['ang_cos_theta_star'])>0.8)))
