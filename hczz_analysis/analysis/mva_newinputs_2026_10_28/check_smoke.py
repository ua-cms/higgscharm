#!/usr/bin/env python3
"""2026-10-28: validate the fsext smoke dataset (DatasetConstructorTask/hc_zzto4l_mw_training_4class_fsext/smoke_20261028).
Per source file: stored events == parquet events with 90 <= m4l < 160 and the stitching filter; class label; stored
field layout == config (+ aux m4l last in lt); new features == utils/hczz_extra_features.py on the parquet (leptons
massless, leading jet / c jet), same event order."""
import glob, os, re, sys
import numpy as np, awkward as ak, yaml
sys.path.insert(0, '/eos/user/s/snandaku/b-hive_ttcc/utils'); import hczz_extra_features as X
B = '/eos/user/s/snandaku/b-hive_ttcc'
cfg = yaml.safe_load(open(f'{B}/config/hc_zzto4l_mw_training_4class_fsext.yml'))
D = f'{B}/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_fsext/smoke_20261028'
src = [l.strip() for l in open(f'{B}/filelists/smoke_fsext_20261028.txt') if l.strip()]
K = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_%2FEvents%3B1_\d+-\d+)")
npz = {K.search(os.path.basename(f)).group(1): f for f in glob.glob(D + '/*.npz')}
ok = True
def first(x): return ak.to_numpy(ak.fill_none(ak.firsts(x) if x.ndim > 1 else x, np.nan)).astype(float)
def pad(x, n): return ak.to_numpy(ak.fill_none(ak.pad_none(x, n, clip=True), 0.0)).astype(float)
for f in src:
    k = K.search(os.path.basename(f)).group(1); name = f.split('/')[-3]
    e = ak.from_parquet(f)
    m = first(e.m4l); keep = (m >= 90) & (m < 160)
    if 'HPlusCharm' in f: keep &= ak.to_numpy(e.n_genc10) == 0
    if 'Stitch3FS' in f: keep &= ak.to_numpy(e.n_genc10) >= 1
    e = e[keep]
    if k not in npz:
        print(f'{name:40s} parquet sel {len(e):4d}  no npz'); ok &= len(e) == 0; continue
    d = np.load(npz[k], allow_pickle=True); g = d['global_features']; n = len(g)
    lay = (list(g.dtype.names) == cfg['global_features'] and list(d['npf_arr'].dtype.names) == cfg['npf_candidates']
           and list(d['lt_arr'].dtype.names) == cfg['lt_candidates'] + ['m4l'] and list(d['vtx_arr'].dtype.names) == cfg['vtx_features']
           and list(d['cpf_arr'].dtype.names) == cfg['cpf_candidates'])
    tr = d['truth']; lab = [t for t in tr.dtype.names if tr[t].all()]
    ref = X.compute_all(pad(e.lepton_pt, 4), pad(e.lepton_eta, 4), pad(e.lepton_phi, 4), np.zeros((len(e), 4)), pad(e.lepton_charge, 4),
                        first(e.pT_4l), first(e.eta_4l), first(e.phi_4l), pad(e.jet_pt, 1)[:, 0], pad(e.jet_eta, 1)[:, 0], pad(e.jet_phi, 1)[:, 0],
                        pad(e.cjets_pt, 1)[:, 0], pad(e.cjets_eta, 1)[:, 0], pad(e.cjets_phi, 1)[:, 0])
    if n == len(e):
        dev = max(float(np.max(np.abs(g[c].astype(float) - ref[c]))) for c in X.ALL_FEATURES)
        dm4l = float(np.max(np.abs(d['lt_arr']['m4l'].ravel() - first(e.m4l))))
    else:
        dev = dm4l = float('nan')
    good = lay and n == len(e) and dev < 1e-4 and dm4l < 1e-3 and len(lab) == 1
    ok &= good
    print(f'{name:40s} parquet sel {len(e):4d} npz {n:4d} label {lab} layout {lay} max|feat diff| {dev:.2e} m4l {dm4l:.1e} {"OK" if good else "FAIL"}')
print('ALL OK' if ok else 'SOME FAILED')
