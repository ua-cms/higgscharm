#!/usr/bin/env python3
"""Optimised p_sig binning for the HcZZ card (2026-10-21).

Fine grid: NF bins with equal shares of an S+B mix (normalised signal + normalised total
background, |w|). Dynamic programming merges contiguous fine bins into K bins and maximises
sum_i Z_i^2, with Z_i the Asimov significance including the background MC-stat uncertainty
(+ optional flat relative syst). Per merged bin: b >= BMIN, sqrt(var_b)/b <= RELMAX, Z+X >= 0, s >= 0.
Optional split: optimise on half of the events (random, seed 1), report the other half.
Usage: python3 optimise_binning.py <events.npz> <out.json> [--half]"""
import sys, json, zlib, numpy as np
NPZ, OUT = sys.argv[1:3]; HALF = "--half" in sys.argv; SMZX = "--smooth-zx" in sys.argv
ZXWIN = 25  # running-mean half-width (fine bins) for the smoothed Z+X used in the optimisation
NF, BMIN, RELMAX, DSYS, KMAX = 300, 1.0, 0.20, 0.0, 30
ev = np.load(NPZ)
BKG = ["qqZZ", "ggZZ", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB"]
rng = np.random.default_rng(1)
def get(p):
    s, w = (ev["zx_score"], ev["zx_weight"]) if p == "ZX" else (ev[p + "|score"], ev[p + "|weight"])
    return s, w
data = {p: get(p) for p in ["Signal", "ZX"] + BKG}
def split(d, part):
    out = {}
    for p, (s, w) in d.items():
        m = np.random.default_rng(zlib.crc32(p.encode())).random(len(s)) < 0.5
        m = m if part == 0 else ~m
        out[p] = (s[m], 2 * w[m])
    return out
opt = split(data, 1) if HALF else data
ev_ = split(data, 1) if HALF else data

def fine_edges(d):
    ss, ws = d["Signal"]; ws = np.abs(ws) / np.abs(ws).sum()
    bs = np.concatenate([d[p][0] for p in BKG + ["ZX"]]); bw = np.concatenate([np.abs(d[p][1]) for p in BKG + ["ZX"]]); bw /= bw.sum()
    s = np.concatenate([ss, bs]); w = np.concatenate([ws, bw]); o = np.argsort(s)
    c = np.cumsum(w[o]); c /= c[-1]
    e = np.interp(np.linspace(0, 1, NF + 1), c, s[o]); e[0], e[-1] = 0.0, 1.0
    return np.unique(e)

def fine_hists(d, e, smooth=True):
    H = lambda p, sq=False: np.histogram(d[p][0], bins=e, weights=d[p][1] ** 2 if sq else d[p][1])[0]
    s = H("Signal"); zx = H("ZX"); zv = H("ZX", True)
    if SMZX and smooth:
        # Z+X CR has n_eff 63 (92% negative weights): replace its fine shape by a running mean
        # so that the edges cannot follow its fluctuations; total yield and variance kept.
        k = np.ones(2 * ZXWIN + 1)
        num = np.convolve(zx, k, "same"); den = np.convolve(np.ones_like(zx), k, "same")
        zs = np.clip(num / den, 0, None); zs *= zx.sum() / zs.sum()
        vs = np.convolve(zv, k, "same") / den; vs *= zv.sum() / vs.sum()
        zx, zv = zs, vs
    b = zx + sum(H(p) for p in BKG); v = zv + sum(H(p, True) for p in BKG)
    return s, b, v, zx

def z2(s, b, v):
    if b <= 0 or s <= 0: return 0.0
    v = v + (DSYS * b) ** 2
    if v <= 0: return 2 * ((s + b) * np.log(1 + s / b) - s)
    t1 = (s + b) * np.log((s + b) * (b + v) / (b * b + (s + b) * v))
    t2 = b * b / v * np.log(1 + v * s / (b * (b + v)))
    return max(2 * (t1 - t2), 0.0)

def optimise(s, b, v, zx):
    n = len(s); C = lambda a: np.concatenate([[0], np.cumsum(a)])
    S, B, V, Z = C(s), C(b), C(v), C(zx)
    val = np.full((n + 1, n + 1), -np.inf)
    for i in range(n):
        for j in range(i + 1, n + 1):
            ss, bb, vv, zz = S[j] - S[i], B[j] - B[i], V[j] - V[i], Z[j] - Z[i]
            if bb >= BMIN and np.sqrt(max(vv, 0)) / bb <= RELMAX and zz >= 0 and ss >= 0:
                val[i, j] = z2(ss, bb, vv)
    best = np.full((KMAX + 1, n + 1), -np.inf); arg = np.zeros((KMAX + 1, n + 1), int); best[0, 0] = 0
    for k in range(1, KMAX + 1):
        for j in range(1, n + 1):
            cand = best[k - 1, :j] + val[:j, j]
            i = int(np.argmax(cand)); best[k, j] = cand[i]; arg[k, j] = i
    res = {}
    for k in range(1, KMAX + 1):
        if not np.isfinite(best[k, n]): continue
        cut = [n]; j = n
        for kk in range(k, 0, -1): j = arg[kk, j]; cut.append(j)
        res[k] = (best[k, n], cut[::-1])
    return res

e = fine_edges(opt); s, b, v, zx = fine_hists(opt, e)
res = optimise(s, b, v, zx)
se, be, ve, zxe = fine_hists(ev_, e, smooth=False)
def total(cut, s, b, v):
    return sum(z2(s[i:j].sum(), b[i:j].sum(), v[i:j].sum()) for i, j in zip(cut[:-1], cut[1:]))
out = {"smooth_zx": SMZX, "ZXWIN": ZXWIN, "NF": NF, "BMIN": BMIN, "RELMAX": RELMAX, "DSYS": DSYS, "half": HALF, "K": {}}
print(f"{'K':>3} {'sumZ2(opt)':>11} {'sumZ2(eval)':>11}  last-bin edge")
for k, (val, cut) in res.items():
    edges = [float(e[i]) for i in cut]
    tz = total(cut, se, be, ve)
    out["K"][k] = {"sumZ2_opt": val, "sumZ2_eval": tz, "edges": edges}
    print(f"{k:3d} {val:11.4e} {tz:11.4e}  {edges[-2]:.4f}")
# reference: nominal 20 qqZZ-quantile bins
qs, qw = data["qqZZ"]; o = np.argsort(qs); c = np.cumsum(np.abs(qw[o])); c /= c[-1]
qe = np.interp(np.linspace(0, 1, 21), c, qs[o]); qe[0], qe[-1] = 0, 1
sq, bq, vq, _ = fine_hists(ev_, qe, smooth=False)
ref = sum(z2(a, bb, cc) for a, bb, cc in zip(sq, bq, vq))
out["ref_qqZZ20_sumZ2_eval"] = ref; out["ref_edges"] = qe.tolist()
print(f"ref 20 qqZZ-quantile bins: sumZ2(eval) {ref:.4e}")
json.dump(out, open(OUT, "w"), indent=1)
