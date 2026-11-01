"""Check C. Gupta's H+c FS c-hadron weights against our independent rederivation (2026-10-02).
Inputs: our out/gg_<era>.json (derive_chad_n.py, official Run 3 H->gg 3FS / 4FS FxFx DAS lists) and his
/eos/user/c/cgupta/HToWW/fs_unc/chad/run3_<era>_T<T>.json + fs_shape/weights.json (run3_comb).
Compares, per era and T: sigma_incl, sigma(0c), sigma(>=1c), sigma3/sigma4 per c-hadron pT bin, and his
shape weight w = [s3(bin)/s3_incl]/[s4(bin)/s4_incl] (w0 for 0c); then the 4-era inverse-variance
combination against his published table. Pulls use our and his statistical errors added in quadrature
(conservative: the two are largely the same events, so real differences should be ~0)."""
import json, os, sys, numpy as np

W = "/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02"
C = "/eos/user/c/cgupta/HToWW/fs_unc"
ERAS = {"2022preEE": "run3_2022preEE", "2022postEE": "run3_2022postEE", "2023preBPix": "run3_2023", "2023postBPix": "run3_2023BPix"}


def ours(era, T):
    r = json.load(open(f"{W}/out/gg_{era}.json"))
    out = {}
    for fs in ("3FS", "4FS"):
        d = r[fs]["T"][str(T)]
        s = {c: np.array(d[c]["sigma_pb"]) for c in ("0", "1", "2")}
        e2 = {c: np.array(d[c]["err_pb"]) ** 2 for c in ("0", "1", "2")}
        ge1 = s["1"] + s["2"]; ege1 = np.sqrt(e2["1"] + e2["2"])
        out[fs] = dict(incl=r[fs]["sigma_incl_pb"], s0=s["0"].sum(), e0=np.sqrt(e2["0"].sum()),
                       bins=ge1[1:], ebins=ege1[1:], files=r[fs]["files"], skipped=r[fs]["skipped"], nev=r[fs]["nev"])
    return out


def his(era, T):
    r = json.load(open(f"{C}/chad/{ERAS[era]}_T{T}.json")); b = r["binned"]["chad1_pt"]
    out = {}
    for fs, k in (("3FS", "3"), ("4FS", "4")):
        a = r[fs]
        s0 = a["sigma_0c"]; e0 = s0 / np.sqrt(a["neff_0c"])
        out[fs] = dict(incl=a["sigma_incl"], s0=s0, e0=e0, bins=np.array(b["sigma" + k]), ebins=np.array(b["err" + k]),
                       files=a["files"], nev=a["nev"])
    return out


def weights(d):
    """w0 and w(bin) = [s3/s3_incl]/[s4/s4_incl], with stat errors."""
    a, b = d["3FS"], d["4FS"]
    f3, f4 = a["bins"] / a["incl"], b["bins"] / b["incl"]
    w = np.divide(f3, f4, out=np.full_like(f3, np.nan), where=f4 > 0)
    ew = np.abs(w) * np.hypot(np.divide(a["ebins"], a["bins"], out=np.zeros_like(f3), where=a["bins"] != 0),
                              np.divide(b["ebins"], b["bins"], out=np.zeros_like(f4), where=b["bins"] != 0))
    w0 = (a["s0"] / a["incl"]) / (b["s0"] / b["incl"])
    ew0 = w0 * np.hypot(a["e0"] / a["s0"], b["e0"] / b["s0"])
    return w0, ew0, w, ew


def combine(items):
    w0 = np.array([i[0] for i in items]); v0 = 1 / np.array([i[1] for i in items]) ** 2
    W_ = np.array([i[2] for i in items]); V = 1 / np.array([i[3] for i in items]) ** 2
    V = np.where(np.isfinite(W_) & np.isfinite(V), V, 0); W_ = np.nan_to_num(W_)
    return (w0 * v0).sum() / v0.sum(), 1 / np.sqrt(v0.sum()), (W_ * V).sum(0) / V.sum(0), 1 / np.sqrt(V.sum(0))


eras = [e for e in ERAS if os.path.exists(f"{W}/out/gg_{e}.json")]
print("eras available:", eras)
hw = json.load(open(f"{C}/fs_shape/weights.json"))
lines = []
for T in (5, 10):
    lines.append(f"\n## T = {T} GeV")
    comb_o, comb_h = [], []
    for era in eras:
        o, h = ours(era, T), his(era, T)
        lines.append(f"\n### {era}")
        for fs in ("3FS", "4FS"):
            lines.append(f"- {fs}: files ours {o[fs]['files']} (skipped {o[fs]['skipped']}) / his {h[fs]['files']}; events {o[fs]['nev']} / {h[fs]['nev']}; "
                         f"sigma_incl {o[fs]['incl']*1e3:.2f} / {h[fs]['incl']*1e3:.2f} fb; sigma(0c) {o[fs]['s0']*1e3:.2f} / {h[fs]['s0']*1e3:.2f} fb")
        wo, wh = weights(o), weights(h)
        comb_o.append(wo); comb_h.append(wh)
        ok = np.isfinite(wh[2]) & np.isfinite(wo[2])
        p = np.r_[(wo[0] - wh[0]) / np.hypot(wo[1], wh[1]), (wo[2][ok] - wh[2][ok]) / np.hypot(wo[3][ok], wh[3][ok])]
        lines.append(f"- w ours: {wo[0]:.3f} | " + " ".join(f"{x:.3f}" for x in wo[2][ok]))
        lines.append(f"- w his : {wh[0]:.3f} | " + " ".join(f"{x:.3f}" for x in wh[2][ok]))
        lines.append(f"- max |w ours - w his| = {np.max(np.abs(np.r_[wo[0]-wh[0], wo[2][ok]-wh[2][ok]])):.4f}; max |pull| = {np.max(np.abs(p)):.2f}")
    if len(eras) == 4:
        co, ch = combine(comb_o), combine(comb_h)
        pub = hw[str(T)]["run3_comb"]; pw = np.array(pub["w"]); pok = np.array(pub["ok"], bool)
        ok = pok & np.isfinite(co[2])
        lines.append(f"\n### 4-era combination")
        lines.append(f"- ours      : {co[0]:.3f} | " + " ".join(f"{x:.3f}" for x in co[2][ok]))
        lines.append(f"- his recomb: {ch[0]:.3f} | " + " ".join(f"{x:.3f}" for x in ch[2][ok]))
        lines.append(f"- published : {pub['w0']:.3f} | " + " ".join(f"{x:.3f}" for x in pw[ok]))
        lines.append(f"- max |ours - published| = {np.max(np.abs(np.r_[co[0]-pub['w0'], co[2][ok]-pw[ok]])):.4f}; "
                     f"max |his recomb - published| = {np.max(np.abs(np.r_[ch[0]-pub['w0'], ch[2][ok]-pw[ok]])):.4f}")
txt = "\n".join(lines)
print(txt)
open(f"{W}/compare_chirayu.md", "w").write("# Chirayu's FS c-hadron weights vs our rederivation\n" + txt + "\n")
