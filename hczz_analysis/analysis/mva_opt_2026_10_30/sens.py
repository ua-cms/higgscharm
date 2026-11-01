#!/usr/bin/env python3
"""2026-10-30 MVA optimisation: fast sensitivity proxy on the b-hive test set.
Weights: w_gen x xs x L_tot / sumw_gen(test chunks of the group) (card normalisation, eras pooled);
H+c: 4FS (0c, file filter) and 3FS (>=1c) each with its own sigma, as the card stitching.
Proxy: Asimov expected 95% CL limit on mu ~ 1.96 / sqrt(sum s^2 / (b + MCstat^2 + (f b)^2)) in (m4l x score) bins,
score = p_Signal, score edges = signal quantiles. Sculpting: ZZ fraction in 115-135 GeV at 5% ZZ efficiency (90-160).
usage: sens.py <pred.npz> [<pred2.npz> ...]"""
import sys, os, re, json, glob, collections
import numpy as np
sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts")
import create_datacards_ctag2d_100150 as dc
FL = "/eos/user/s/snandaku/b-hive_ttcc/filelists/hcZZ_fs_test_20261028.txt"
BR4L = 2.6381e-05 / 73.30e-3            # HplusC xs (pb) / sigma_total (pb): sigma x BR convention of the card
XS_SIG = {"4FS": 97.62e-3 * BR4L, "3FS": 55.0e-3 * BR4L}
PID = {1: None, 2: "GluGlutoContinto2Zto4E", 3: "GluGlutoContinto2Zto4Mu", 4: "GluGlutoContinto2Zto4Tau",
       5: "GluGluToContinto2Zto2E2Mu", 6: "GluGluToContinto2Zto2E2Tau", 7: "GluGluToContinto2Zto2Mu2Tau",
       8: "4FS", 9: "HplusB", 10: "HplusB", 11: "HplusB", 12: "HplusB", 13: "HplusB", 14: "VBFHto2Zto4L",
       15: "ZHto2Zto4L", 16: "WminusH_Hto2Zto4L", 17: "WplusH_Hto2Zto4L", 18: "TTH_Hto2Z", 20: "3FS"}
L_TOT = dc.TOTAL_LUMI_PB

def group_of(path):
    part = path.split("/")[-3]; base = re.sub(r"_\d+$", "", part)
    if base.startswith("HPlusCharm"): return "4FS"
    if base.startswith("Stitch3FS"): return "3FS"
    if "HPlusBottom" in base or base.endswith("HB"): return "HplusB"
    return base

_SW = None
def sumw_test():
    global _SW
    if _SW is None:
        _SW = collections.defaultdict(float)
        for p in open(FL).read().split():
            j = os.path.join(os.path.dirname(os.path.dirname(p)), "sumw", os.path.basename(p).replace(".parquet", ".json"))
            _SW[group_of(p)] += json.load(open(j))["sumw"]
    return _SW

def xs(group):
    if group in XS_SIG: return XS_SIG[group]
    return dc.group_xs_pb("2022postEE", group)

NPZ_DIR = "/eos/user/s/snandaku/b-hive_ttcc/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_fsextv2_disco1/hcZZ_fs_fsextv2disco1_20261028_test"
_PERW = None
def per_event_weight():
    """(process id, truth) -> physical yield per test event in 90-160 GeV (inference yields unit weights, so each
    group's yield sum(w_gen) x xs x L / sumw_gen is shared equally among its events)."""
    global _PERW
    if _PERW is None:
        sw = sumw_test(); Y = collections.defaultdict(float); N = collections.defaultdict(int)
        for f in glob.glob(NPZ_DIR + "/*.npz"):
            d = np.load(f, allow_pickle=True); m = d["lt_arr"]["m4l"]; win = (m > 90) & (m < 160)
            tr = np.argmax(np.stack([d["truth"][k] for k in d["truth"].dtype.names], 1), 1); pr = d["process"].astype(int)
            for pid in np.unique(pr[win]):
                for t in np.unique(tr[win & (pr == pid)]):
                    mm = win & (pr == pid) & (tr == t)
                    g = PID[pid] if pid != 1 else ("GluGluHtoZZto4L" if t == 3 else "ZZto4L")
                    Y[(pid, t)] += d["weight_nominal"][mm].sum() * xs(g) * L_TOT / sw[g]; N[(pid, t)] += mm.sum()
        _PERW = {k: Y[k] / N[k] for k in Y}
    return _PERW

def load(f):
    d = np.load(f, allow_pickle=True); cls = list(d["classes"].item().keys()); t = d["truth"]; pr = d["process"].astype(int)
    pw = per_event_weight(); w = np.array([pw.get((a, b), 0.0) for a, b in zip(pr, t)])
    return dict(s=d["pred"][:, cls.index("Signal")], m=d["m4l"], t=t, w=w, cls=cls)

ERAS = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]
def load_zx(zdir):
    """Z+X as the card reads it: 3P1F + 2P2F, zx_weight, m4l (first element), mva_score_Signal."""
    import pandas as pd
    S, M, Wz = [], [], []
    for era in ERAS:
        for cr in ("3p1f", "2p2f"):
            df = pd.read_parquet(f"{zdir}/{era}/zx_{cr}_{era}_mva.parquet", columns=["m4l", "mva_score_Signal", "zx_weight"])
            M.append(np.array([x[0] if hasattr(x, "__len__") and len(x) else (np.nan if hasattr(x, "__len__") else x) for x in df["m4l"].values], float))
            S.append(df["mva_score_Signal"].values); Wz.append(df["zx_weight"].values)
    return dict(s=np.concatenate(S), m=np.concatenate(M), w=np.concatenate(Wz))

def proxy(e, medges=(105, 115, 119, 122, 125, 128, 131, 135, 160), nsc=8, fsys=0.10, zx=0.0, zxe=None, bfloor=0.05):
    cls = e["cls"]; win = (e["m"] > 105) & (e["m"] < 160)
    sig = win & (e["t"] == cls.index("Signal")); bkg = win & ~sig
    q = np.quantile(e["s"][sig], np.linspace(0, 1, nsc + 1), method="inverted_cdf", weights=e["w"][sig]) if False else \
        np.interp(np.linspace(0, 1, nsc + 1), np.cumsum(e["w"][sig][np.argsort(e["s"][sig])]) / e["w"][sig].sum(), np.sort(e["s"][sig]))
    q[0], q[-1] = 0, 1.0000001
    H = lambda m, ww: np.histogram2d(e["m"][m], e["s"][m], bins=[medges, q], weights=ww)[0]
    S = H(sig, e["w"][sig]); Bk = H(bkg, e["w"][bkg]); B2 = H(bkg, e["w"][bkg] ** 2)
    if zx:   # Z+X approximated by the qqZZ shape, scaled to zx events
        z = win & (e["t"] == cls.index("qqZZ")); Z = H(z, e["w"][z]); Bk = Bk + Z / Z.sum() * zx
    if zxe is not None:   # real Z+X scored with the same model
        zm = (zxe["m"] > 105) & (zxe["m"] < 160)
        Zh = np.histogram2d(zxe["m"][zm], zxe["s"][zm], bins=[medges, q], weights=zxe["w"][zm])[0]
        B2 = B2 + np.histogram2d(zxe["m"][zm], zxe["s"][zm], bins=[medges, q], weights=zxe["w"][zm] ** 2)[0]
        # signed Z+X (3P1F - 2P2F): never let it push a bin below the MC background; 0.05-event floor per bin
        Bk = np.maximum(Bk + np.clip(Zh, 0, None), Bk)
        Bk = np.maximum(Bk, bfloor)
    var = Bk + B2 + (fsys * Bk) ** 2
    ok = Bk > 1e-6
    return 1.96 / np.sqrt((S[ok] ** 2 / var[ok]).sum()), S.sum(), Bk.sum()

def sculpt(e, eff=0.05):
    cls = e["cls"]; zz = (e["t"] == cls.index("qqZZ")) | (e["t"] == cls.index("ggZZ")); zz &= (e["m"] > 90) & (e["m"] < 160)
    s, w, m = e["s"][zz], e["w"][zz], e["m"][zz]; o = np.argsort(-s); c = np.cumsum(w[o]) / w.sum()
    cut = s[o][np.searchsorted(c, eff)]; sel = s >= cut
    return w[sel & (m > 115) & (m < 135)].sum() / w[sel].sum()

if __name__ == "__main__":
    for a in sys.argv[1:]:
        f, _, zdir = a.partition(":")
        e = load(f); cls = e["cls"]; win = (e["m"] > 105) & (e["m"] < 160)
        if zdir:
            z = load_zx(zdir); zm = (z["m"] > 105) & (z["m"] < 160)
            print(f"{os.path.basename(f):40s} proxy mu95 with real Z+X {proxy(e, zxe=z)[0]:7.1f}  (Z+X yield {z['w'][zm].sum():.1f})  sculpt5% {sculpt(e):.3f}")
            continue
        yields = {c: round(e["w"][win & (e["t"] == i)].sum(), 3) for i, c in enumerate(cls)}
        mu, S, B = proxy(e); mu_zx, _, _ = proxy(e, zx=35.9)
        print(f"{os.path.basename(f):40s} proxy mu95 {mu:7.1f}  with ZX~qqZZ {mu_zx:7.1f}  sculpt5% {sculpt(e):.3f}  yields {yields}")
