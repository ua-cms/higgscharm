#!/usr/bin/env python3
"""Joint (m4l, MVA score) binning for the HcZZ 2D template card (2026-10-27).

Extends optbin_2026_10_21/optimise_binning.py (1D, DP on sum_i Z_i^2) to two dimensions.
Input: events_2d.npz from dump_events_2d.py (score variable = (i_m4l_fine + s) / 220, 0.25 GeV grid).

Figure of merit (as 1D): sum over cells of the Asimov Z^2 with the background MC-stat variance;
per cell: b >= BMIN, sqrt(var_b)/b <= RELMAX, Z+X >= 0, s >= 0 (a layout with any invalid cell is rejected).
Z+X (CR, n_eff ~ 60, mostly negative weights) is smoothed for the optimisation: factorised into its m4l and
score marginals, each a running mean (total yield and variance kept), as --smooth-zx in 1D.

Fine grid: m4l in 1 GeV steps (55 bins, 105-160), score in NS quantiles of the S+B mix (normalised signal +
normalised background), as the 1D fine grid.

Two layouts:
  rect : rectangular Km x Ks grid. Coordinate ascent: exact DP over m4l edges for fixed score edges (a slab's value
         is the sum over its Ks cells), then exact DP over score edges for fixed m4l edges; repeat to convergence,
         from several starts (uniform m4l / equal-signal score, and the hand-picked 2D card edges); best kept.
  cat  : score categories, each with its own m4l binning (non-rectangular). For every score slab the best m4l
         partition with k bins (k <= KM) is found by DP; an outer DP splits the score axis into categories under a
         total bin budget T.
--half: optimise on a random half of the events (weights x2), evaluate the edges on the other half (overfitting check).

Usage: python3 optimise_2d.py <events_2d.npz> <out.json> [--half] [--ns 40] [--km 14] [--ks 8] [--budgets 10,15,...]
"""
import sys, json, zlib, argparse
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("npz"); ap.add_argument("out")
ap.add_argument("--half", action="store_true")
ap.add_argument("--ns", type=int, default=40)
ap.add_argument("--km", type=int, default=14, help="max m4l bins (rect) / per category (cat)")
ap.add_argument("--ks", type=int, default=8, help="max score bins (rect) / categories (cat)")
ap.add_argument("--budgets", default="10,15,20,25,30,36,45,60")
ap.add_argument("--bmin", type=float, default=1.0)
ap.add_argument("--relmax", type=float, default=0.20)
a = ap.parse_args()

NFINE, M0, M1 = 220, 105.0, 160.0
ME = np.arange(M0, M1 + 0.5, 1.0)            # 1 GeV fine m4l edges
NM = len(ME) - 1
BKG = ["qqZZ", "ggZZ", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB"]
ev = np.load(a.npz)


def decode(u):
    x = np.asarray(u, float) * NFINE
    i = np.floor(x).astype(int)
    s = x - i
    m = M0 + 0.25 * i + 0.125
    return m, s


def get(p):
    if p == "ZX":
        u, w = ev["zx_score"], ev["zx_weight"]
    else:
        u, w = ev[p + "|score"], ev[p + "|weight"]
    ok = u >= 0
    m, s = decode(u[ok])
    return m, s, np.asarray(w[ok], float)


data = {p: get(p) for p in ["Signal", "ZX"] + BKG if p == "ZX" or p + "|score" in ev.files}
BKG = [p for p in BKG if p in data]


def split(d, part):
    out = {}
    for p, (m, s, w) in d.items():
        k = np.random.default_rng(zlib.crc32(p.encode())).random(len(m)) < 0.5
        k = k if part == 0 else ~k
        out[p] = (m[k], s[k], 2 * w[k])
    return out


OPT = split(data, 0) if a.half else data
EVL = split(data, 1) if a.half else data


def score_fine_edges(d, ns):
    _, ss, ws = d["Signal"]; ws = np.abs(ws) / np.abs(ws).sum()
    bs = np.concatenate([d[p][1] for p in BKG + ["ZX"]])
    bw = np.concatenate([np.abs(d[p][2]) for p in BKG + ["ZX"]]); bw = bw / bw.sum()
    s = np.concatenate([ss, bs]); w = np.concatenate([ws, bw]); o = np.argsort(s)
    c = np.cumsum(w[o]); c /= c[-1]
    e = np.interp(np.linspace(0, 1, ns + 1), c, s[o]); e[0], e[-1] = 0.0, 1.0
    return np.unique(e)


SE = score_fine_edges(OPT, a.ns)
NS = len(SE) - 1


def h2(d, p, sq=False):
    m, s, w = d[p]
    return np.histogram2d(m, s, bins=[ME, SE], weights=w ** 2 if sq else w)[0]


def runmean(x, k):
    ker = np.ones(2 * k + 1)
    return np.convolve(x, ker, "same") / np.convolve(np.ones_like(x), ker, "same")


def hists(d, smooth):
    s = h2(d, "Signal")
    zx, zv = h2(d, "ZX"), h2(d, "ZX", True)
    if smooth:
        tot, vtot = zx.sum(), zv.sum()
        pm = np.clip(runmean(zx.sum(1), 5), 0, None); ps = np.clip(runmean(zx.sum(0), 4), 0, None)
        pm /= pm.sum(); ps /= ps.sum()
        zx = tot * np.outer(pm, ps); zv = vtot * np.outer(pm, ps)
    b = zx + sum(h2(d, p) for p in BKG)
    v = zv + sum(h2(d, p, True) for p in BKG)
    return s, b, v, zx


def z2v(s, b, v):
    """vectorised Asimov Z^2 with bkg variance v (optimise_binning.z2, DSYS = 0)"""
    s = np.asarray(s, float); b = np.asarray(b, float); v = np.asarray(v, float)
    out = np.zeros(np.broadcast(s, b, v).shape)
    ok = (b > 0) & (s > 0)
    with np.errstate(all="ignore"):
        sb = s + b
        t1 = sb * np.log(sb * (b + v) / (b * b + sb * v))
        t2 = b * b / v * np.log1p(v * s / (b * (b + v)))
        zz = 2 * (t1 - t2)
        zz0 = 2 * (sb * np.log1p(s / b) - s)
    zz = np.where(v > 0, zz, zz0)
    out[ok] = np.maximum(zz[ok], 0.0)
    return out


def valid(s, b, v, zx):
    with np.errstate(all="ignore"):
        return (b >= a.bmin) & (np.sqrt(np.maximum(v, 0)) / np.where(b > 0, b, np.inf) <= a.relmax) & (zx >= 0) & (s >= -1e-12)


def cell_value(s, b, v, zx):
    val = z2v(s, b, v)
    return np.where(valid(s, b, v, zx), val, -np.inf)


def C(x, axis):
    sh = list(x.shape); sh[axis] = 1
    return np.concatenate([np.zeros(sh), np.cumsum(x, axis=axis)], axis=axis)


def slab_matrix(H, axis, cuts_other):
    """value[i, j] of merging fine bins [i, j) along `axis`, summed over the groups `cuts_other` of the other axis"""
    s, b, v, zx = H
    if axis == 1:
        s, b, v, zx = s.T, b.T, v.T, zx.T
    # group the other axis
    g = lambda x: np.add.reduceat(x, cuts_other[:-1], axis=1)
    s, b, v, zx = g(s), g(b), g(v), g(zx)
    S, B, V, Z = C(s, 0), C(b, 0), C(v, 0), C(zx, 0)
    n = s.shape[0]
    d = lambda X: X[None, :, :] - X[:, None, :]          # [i, j, group]
    val = cell_value(d(S), d(B), d(V), d(Z)).sum(-1)       # -inf if any cell invalid
    iu = np.triu_indices(n + 1, 1)
    out = np.full((n + 1, n + 1), -np.inf); out[iu] = val[iu]
    return out


def dp(val, k):
    n = val.shape[0] - 1
    best = np.full((k + 1, n + 1), -np.inf); arg = np.zeros((k + 1, n + 1), int); best[0, 0] = 0
    for kk in range(1, k + 1):
        cand = best[kk - 1][:, None] + val                    # [i, j]
        arg[kk] = np.argmax(cand, axis=0); best[kk] = cand[arg[kk], np.arange(n + 1)]
    if not np.isfinite(best[k, n]):
        return -np.inf, None
    cut = [n]; j = n
    for kk in range(k, 0, -1):
        j = arg[kk, j]; cut.append(j)
    return best[k, n], cut[::-1]


def eval_cells(s, b, v, zx):
    """evaluation (no constraints): Z^2 of every cell with b clipped at 1e-3; also the number of cells that
    would fail the optimisation constraints (on raw, unsmoothed Z+X these can be negative)"""
    val = z2v(np.maximum(s, 0), np.maximum(b, 1e-3), np.maximum(v, 0))
    return float(val.sum()), int((~valid(s, b, v, zx)).sum())


def rect_value(H, cm, cs, strict=True):
    s, b, v, zx = H
    g = lambda x: np.add.reduceat(np.add.reduceat(x, cm[:-1], axis=0), cs[:-1], axis=1)
    if not strict:
        return eval_cells(g(s), g(b), g(v), g(zx))
    return float(cell_value(g(s), g(b), g(v), g(zx)).sum())


def eq_signal_cuts(H, k):
    sc = H[0].sum(0); c = np.cumsum(sc) / sc.sum()
    cut = sorted({0, NS} | {int(np.searchsorted(c, q)) + 1 for q in np.arange(1, k) / k})
    cut = [x for x in cut if 0 <= x <= NS]
    return cut if len(cut) == k + 1 else list(np.linspace(0, NS, k + 1).astype(int))


def m4l_cuts_from_edges(edges):
    return sorted({int(round(e - M0)) for e in edges})


def optimise_rect(H, km, ks):
    starts = [(list(np.linspace(0, NM, km + 1).round().astype(int)), eq_signal_cuts(H, ks))]
    hand = m4l_cuts_from_edges([105, 115, 119, 122, 124, 126, 128, 131, 140, 160])
    if len(hand) == km + 1:
        starts.append((hand, eq_signal_cuts(H, ks)))
    best = (-np.inf, None, None)
    for cm, cs in starts:
        cur = -np.inf
        for _ in range(20):
            vm, cm2 = dp(slab_matrix(H, 0, np.array(cs)), km)
            if cm2 is None:
                break
            vs, cs2 = dp(slab_matrix(H, 1, np.array(cm2)), ks)
            if cs2 is None:
                break
            cm, cs = cm2, cs2
            if vs <= cur + 1e-9:
                cur = vs; break
            cur = vs
        if cm is not None and cs is not None and cur > best[0]:
            best = (cur, cm, cs)
    return best


def optimise_cat(H, km, ks, budgets):
    s, b, v, zx = H
    # V[a, c, k]: best value of score slab [a, c) with an m4l partition into exactly k bins (k <= km)
    V = np.full((NS + 1, NS + 1, km + 1), -np.inf); CUT = {}
    for lo in range(NS):
        for hi in range(lo + 1, NS + 1):
            sl = lambda x: x[:, lo:hi].sum(1, keepdims=True)
            vm = slab_matrix((sl(s), sl(b), sl(v), sl(zx)), 0, np.array([0, 1]))
            n = NM
            best = np.full((km + 1, n + 1), -np.inf); arg = np.zeros((km + 1, n + 1), int); best[0, 0] = 0
            for kk in range(1, km + 1):
                cand = best[kk - 1][:, None] + vm
                arg[kk] = np.argmax(cand, axis=0); best[kk] = cand[arg[kk], np.arange(n + 1)]
                if np.isfinite(best[kk, n]):
                    V[lo, hi, kk] = best[kk, n]
                    cut = [n]; j = n
                    for k2 in range(kk, 0, -1):
                        j = arg[k2, j]; cut.append(j)
                    CUT[(lo, hi, kk)] = cut[::-1]
    res = {}
    for T in budgets:
        # F[c][t][q]: best for score [0, c) with t bins used and q categories
        F = np.full((NS + 1, T + 1, ks + 1), -np.inf); P = {}
        F[0, 0, 0] = 0
        for c in range(1, NS + 1):
            for lo in range(c):
                for kk in range(1, km + 1):
                    vv = V[lo, c, kk]
                    if not np.isfinite(vv):
                        continue
                    for t in range(kk, T + 1):
                        for q in range(1, ks + 1):
                            x = F[lo, t - kk, q - 1] + vv
                            if x > F[c, t, q]:
                                F[c, t, q] = x; P[(c, t, q)] = (lo, kk)
        # best over t <= T and q
        idx = np.unravel_index(np.argmax(F[NS]), F[NS].shape)
        t, q = int(idx[0]), int(idx[1])
        if not np.isfinite(F[NS, t, q]):
            continue
        cats = []; c = NS
        while c > 0:
            lo, kk = P[(c, t, q)]
            cats.append((lo, c, CUT[(lo, c, kk)])); c, t, q = lo, t - kk, q - 1
        res[T] = (float(F[NS][idx]), cats[::-1])
    return res


def cat_value(H, cats):
    s, b, v, zx = H
    tot, bad = 0.0, 0
    for lo, hi, cm in cats:
        g = lambda x: np.add.reduceat(x[:, lo:hi].sum(1), cm[:-1])
        t, n = eval_cells(g(s), g(b), g(v), g(zx)); tot += t; bad += n
    return tot, bad


HO = hists(OPT, smooth=True)
HE = hists(EVL, smooth=False)
out = {"args": vars(a), "score_fine_edges": SE.tolist(), "m4l_fine_edges": ME.tolist(), "rect": {}, "cat": {}}

# references: 1D S10-like (m4l integrated) and the hand-picked 2D layout of the card chain
ref_hand = m4l_cuts_from_edges([105, 115, 119, 122, 124, 126, 128, 131, 140, 160])
ref_cs = eq_signal_cuts(HO, 4)
out["ref_hand_2D"] = {"m4l_cuts": [int(x) for x in ref_hand], "score_cuts": [int(x) for x in ref_cs],
                      "sumZ2_opt": rect_value(HO, ref_hand, ref_cs), "sumZ2_eval": rect_value(HE, ref_hand, ref_cs, False)}
v1, c1 = None, None
for k1 in range(10, 0, -1):              # 10 bins as S10, fewer if infeasible
    v1, c1 = dp(slab_matrix(HO, 1, np.array([0, NM])), k1)
    if c1 is not None:
        break
out["ref_1D"] = {"nbins": k1, "score_cuts": [int(x) for x in c1], "sumZ2_opt": v1, "sumZ2_eval": rect_value(HE, [0, NM], c1, False)}
print(f"fine grid {NM} m4l x {NS} score; half={a.half}")
print(f"ref 1D {k1} bins       : opt {v1:.4e}  eval {out['ref_1D']['sumZ2_eval']}")
print(f"ref hand 2D 9x4      : opt {out['ref_hand_2D']['sumZ2_opt']:.4e}  eval {out['ref_hand_2D']['sumZ2_eval']}")

print("\nrect: Km x Ks -> sumZ2 (opt / eval)")
for km in range(2, a.km + 1):
    for ks in range(1, a.ks + 1):
        val, cm, cs = optimise_rect(HO, km, ks)
        if cm is None:
            continue
        ev_, bad = rect_value(HE, cm, cs, False)
        out["rect"][f"{km}x{ks}"] = {"sumZ2_opt": val, "sumZ2_eval": ev_, "eval_bad_cells": bad, "nbins": km * ks,
                                     "m4l_edges": [float(ME[i]) for i in cm], "score_edges": [float(SE[i]) for i in cs]}
        print(f"  {km:2d} x {ks}  ({km*ks:3d} bins): {val:.4e} / {ev_:.4e}  (eval cells failing constraints: {bad})")
        sys.stdout.flush()

print("\ncat: budget -> sumZ2 (opt / eval), categories")
budgets = [int(x) for x in a.budgets.split(",")]
for T, (val, cats) in optimise_cat(HO, a.km, a.ks, budgets).items():
    ev_, bad = cat_value(HE, cats)
    nb = sum(len(cm) - 1 for _, _, cm in cats)
    out["cat"][str(T)] = {"sumZ2_opt": val, "sumZ2_eval": ev_, "eval_bad_cells": bad, "nbins": nb,
                          "cats": [{"score": [float(SE[lo]), float(SE[hi])], "m4l_edges": [float(ME[i]) for i in cm]}
                                   for lo, hi, cm in cats]}
    print(f"  T={T:3d} ({nb:3d} bins, {len(cats)} cats): {val:.4e} / {ev_:.4e}  (eval bad cells {bad})")
    for lo, hi, cm in cats:
        print(f"      score [{SE[lo]:.3f},{SE[hi]:.3f}): m4l {[float(ME[i]) for i in cm]}")
    sys.stdout.flush()
json.dump(out, open(a.out, "w"), indent=1)
print(f"-> {a.out}")
