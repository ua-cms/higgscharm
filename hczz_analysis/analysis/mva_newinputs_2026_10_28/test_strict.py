"""2026-10-28: strict scorer (missing columns / scalar candidate slots raise) on real card inputs: one bkg file per
era (all bkg samples) and signal 4FS/3FS, for prod, massinvlep and fsext configs."""
import glob, sys, numpy as np, pandas as pd, collections
sys.path.insert(0, '/eos/user/s/snandaku/Higgscharmnew/higgscharm')
from analysis.postprocess.mva_inference import MVAPostProcessor
B = '/eos/user/s/snandaku/b-hive_ttcc/config/'; O = '/eos/user/s/snandaku/higgscharm/outputs/'
cfgs = {'prod': '/eos/user/s/snandaku/Higgscharmnew/higgscharm/models/hc_zzto4l_mw_training_4class_nomass.yml',
        'massinvlep': B + 'hc_zzto4l_mw_training_4class_massinvlep.yml', 'fsext': B + 'hc_zzto4l_mw_training_4class_fsext.yml'}
files = []
for era in ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']:
    by = collections.defaultdict(list)
    for f in glob.glob(f'{O}hplusc_mva_4class_ctag2d_jecshifts_bkg/{era}/*/base/*.parquet'):
        by[f.split('/')[-3].rsplit('_', 1)[0] if f.split('/')[-3][-1].isdigit() else f.split('/')[-3]].append(f)
    files += [v[0] for v in by.values()]
    files += glob.glob(f'{O}hplusc_mva_4class_ctag2d_jecshifts_fschad/{era}/HPlusCharm*/base/*.parquet')[:1]
    files += glob.glob(f'{O}hplusc_mva_4class_ctag2d_jecshifts_fschad/{era}/Stitch3FS*/base/*.parquet')[:1]
bad = 0
for tag, c in cfgs.items():
    n = 0
    for f in files:
        era = f.split('/')[-4]; df = pd.read_parquet(f)
        if len(df) == 0: continue
        try:
            X = MVAPostProcessor(c, '', era=era).prepare_features(df); n += 1
            assert np.isfinite(X).all()
        except Exception as e:
            bad += 1; print(f'{tag} FAIL {f.split("outputs/")[1][:90]}: {type(e).__name__}: {str(e)[:150]}')
    print(f'{tag}: {n} files OK')
print('ALL OK' if bad == 0 else f'{bad} FAILURES')
