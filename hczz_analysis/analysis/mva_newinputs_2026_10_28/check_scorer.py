"""2026-10-28: analysis scorer (mva_inference.prepare_features) vs b-hive stored inputs on the fsext smoke dataset,
all 77 columns (candidate-major layout, mass-scaled columns divided by the stored aux m4l)."""
import glob, os, re, sys, numpy as np, pandas as pd, yaml, awkward as ak
sys.path.insert(0, '/eos/user/s/snandaku/Higgscharmnew/higgscharm')
from analysis.postprocess.mva_inference import MVAPostProcessor
B = '/eos/user/s/snandaku/b-hive_ttcc'; C = f'{B}/config/hc_zzto4l_mw_training_4class_fsext.yml'; cfg = yaml.safe_load(open(C))
D = f'{B}/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_fsext/smoke_20261028'
K = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_%2FEvents%3B1_\d+-\d+)")
npz = {K.search(os.path.basename(f)).group(1): f for f in glob.glob(D + '/*.npz')}
scaled = set(cfg['mass_scaled_features']); worst = 0; names = None
for f in [l.strip() for l in open(f'{B}/filelists/smoke_fsext_20261028.txt') if l.strip()]:
    era = next(e for e in ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix'] if f'/{e}/' in f)
    inf = MVAPostProcessor(C, '', era=era)
    df = pd.read_parquet(f); m4l = inf._get_col(df, ['zz_mass_inclusive', 'm4l']); keep = (m4l >= 90) & (m4l < 160)
    if 'HPlusCharm' in f: keep &= df['n_genc10'].to_numpy() == 0
    if 'Stitch3FS' in f: keep &= df['n_genc10'].to_numpy() >= 1
    df = df[keep]; Xs = inf.prepare_features(df)
    d = np.load(npz[K.search(os.path.basename(f)).group(1)]); m = d['lt_arr']['m4l'].ravel().astype(float)
    cols, nm = [], []
    for g in cfg['global_features']: cols.append(d['global_features'][g]); nm.append(g)
    for blk, key, nk in (('cpf_arr', 'cpf_candidates', 'n_cpf_candidates'), ('npf_arr', 'npf_candidates', 'n_npf_candidates'), ('vtx_arr', 'vtx_features', 'n_vtx_candidates')):
        for j in range(cfg[nk]):
            for g in cfg[key]: cols.append(d[blk][g][:, j]); nm.append(f'{g}[{j}]')
    for g in cfg['lt_candidates']: cols.append(d['lt_arr'][g].ravel()); nm.append(g)
    Xb = np.column_stack(cols).astype(float)
    for i, n in enumerate(nm):
        if n.split('[')[0] in scaled: Xb[:, i] /= m
    assert Xs.shape == Xb.shape, (Xs.shape, Xb.shape)
    diff = np.abs(Xs - Xb).max(0); rel = diff / np.maximum(np.abs(Xb).max(0), 1e-6)
    bad = [(nm[i], float(diff[i])) for i in np.where(rel > 1e-4)[0]]
    worst = max(worst, float(rel.max())); names = nm
    print(f"{f.split('/')[-3]:32s} n={len(df):4d} cols={Xs.shape[1]} max rel diff {rel.max():.1e} {'OK' if not bad else bad[:5]}")
print('worst rel diff', f'{worst:.1e}', 'ALL 77 MATCH' if worst < 1e-4 else 'MISMATCH')
