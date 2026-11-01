#!/usr/bin/env python3
"""2026-10-28 (user): balanced HcZZ training set as the production one, with the FS-stitched signal.

Targets (events with 90 <= m4l < 160, after the stitching filter; user 10-28):
  qqZZ 90k, ggZZ all available, Other_Higgs 150k, Signal = all train-side 4FS (HPlusCharm*, n_genc10 == 0, ~9.8k)
  + all train-side 3FS (Stitch3FS_cH*, n_genc10 >= 1, ~82k) (stitched max stats: 0c ~11% vs ~22% of the stitched yield)
Sources:
  backgrounds: the current split (filelists/hplusc_mva_4class_{train,test}.txt), train capped by random whole files
  4FS signal : the card production (..._ctag2d_jecshifts_fschad, has n_genc10), chunks of the current 4FS train / test
               split keep their side (no test events enter the training)
  3FS signal : the card production, new 80/20 file split (seed 42)
Test filelist: all current test files of the backgrounds + the test-side 4FS / 3FS files (stitching filter in the config).
Writes b-hive filelists hcZZ_fs_{train,test}_20261028.txt and the per-file counts (cached).
"""
import glob, json, os, re, sys, collections
import numpy as np, awkward as ak

# user 10-28: qqZZ 90k, Other_Higgs 150k, ggZZ all; signal = ALL 4FS 0c (train side) + ALL 3FS >=1c (train side)
TARGET = {'qqZZ': 90000, 'ggZZ': 10 ** 9, 'Other_Higgs': 150000, 'S4FS0c': 10 ** 9, 'S3FS1c': 10 ** 9}
B = '/eos/user/s/snandaku/b-hive_ttcc/filelists/'
O = '/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d_jecshifts_fschad/'
HERE = os.path.dirname(os.path.abspath(__file__)); CACHE = os.path.join(HERE, 'file_counts.json')
K = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_%2FEvents%3B1_\d+-\d+)")
key = lambda p: K.search(os.path.basename(p)).group(1)
cache = json.load(open(CACHE)) if os.path.exists(CACHE) else {}


def count(f, genc=None):
    ck = f + (f'|{genc}' if genc else '')
    if ck not in cache:
        cols = ['m4l'] + (['n_genc10'] if genc else [])
        e = ak.from_parquet(f, columns=cols)
        if len(e) == 0:
            cache[ck] = 0
        else:
            m = ak.to_numpy(ak.fill_none(ak.firsts(e.m4l) if e.m4l.ndim > 1 else e.m4l, np.nan))
            w = (m >= 90) & (m < 160)
            if genc == '0c': w &= ak.to_numpy(e.n_genc10) == 0
            if genc == '1c': w &= ak.to_numpy(e.n_genc10) >= 1
            cache[ck] = int(w.sum())
    return cache[ck]


def cls_of(f):
    if 'HPlusCharm' in f or 'SomeSMSignal' in f: return 'Signal'
    if 'ZZto4L' in f and 'Contin' not in f: return 'qqZZ'
    if 'Contin' in f: return 'ggZZ'
    return 'Other_Higgs'


tr = [l.strip() for l in open(B + 'hplusc_mva_4class_train.txt') if l.strip()]
te = [l.strip() for l in open(B + 'hplusc_mva_4class_test.txt') if l.strip()]
rng = np.random.default_rng(42)
train, test, got = [], [], collections.Counter()
for c in ('qqZZ', 'ggZZ', 'Other_Higgs'):
    fl = [f for f in tr if cls_of(f) == c]; rng.shuffle(fl)
    for f in fl:
        if got[c] >= TARGET[c]: break
        n = count(f)
        if n: train.append(f); got[c] += n
    test += [f for f in te if cls_of(f) == c]
# signal: 4FS 0c from the card production, current split side
trk = {key(f) for f in tr if cls_of(f) == 'Signal'}; tek = {key(f) for f in te if cls_of(f) == 'Signal'}
f4 = sorted(glob.glob(O + '*/HPlusCharm*/base/*.parquet')); rng.shuffle(f4)
for f in f4:
    k = key(f)
    if k in tek: test.append(f)
    elif k in trk and got['S4FS0c'] < TARGET['S4FS0c']:
        n = count(f, '0c')
        if n: train.append(f); got['S4FS0c'] += n
f3 = sorted(glob.glob(O + '*/Stitch3FS_cH*/base/*.parquet')); rng.shuffle(f3)
ntest3 = int(round(0.2 * len(f3))); test += f3[:ntest3]
for f in f3[ntest3:]:
    if got['S3FS1c'] >= TARGET['S3FS1c']: break
    n = count(f, '1c')
    if n: train.append(f); got['S3FS1c'] += n
json.dump(cache, open(CACHE, 'w'))
# 10-28: the same NanoAOD chunk can sit in two sample dirs (e.g. GluGluToContinto2Zto2Mu2Tau_1 and _2): drop the
# test copy of any chunk that is in the training
trk_all = {key(f) for f in train}; dropped = [f for f in test if key(f) in trk_all]
test = [f for f in test if key(f) not in trk_all]
print('test files dropped (chunk also in train):', len(dropped), [d.split('/')[-3] for d in dropped])
ov = {key(f) for f in train} & {key(f) for f in test}
if ov:
    print('train/test overlap:', len(ov)); [print('  ', f) for f in train + test if key(f) in ov]
    sys.exit(1)
open(B + 'hcZZ_fs_train_20261028.txt', 'w').write('\n'.join(train) + '\n')
open(B + 'hcZZ_fs_test_20261028.txt', 'w').write('\n'.join(test) + '\n')
print('train events (90-160, after stitching filter):', dict(got), 'Signal total', got['S4FS0c'] + got['S3FS1c'])
print('train files', len(train), 'test files', len(test), '-> filelists/hcZZ_fs_{train,test}_20261028.txt')
