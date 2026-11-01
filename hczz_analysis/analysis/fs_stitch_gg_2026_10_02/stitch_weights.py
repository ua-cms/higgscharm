"""FS stitching weights for the 4FS FxFx H+c -> ZZ -> 4l signal, from OUR rederivation on the official
Run 3 H->gg 3FS / 4FS FxFx samples (out/gg_<era>.json, derive_chad_n.py), 2026-10-02.

Stitching as FS.pdf (T. Bevilacqua, slide 22) / LHCHWG-2026-007 sec. 4.4, with the GEN c-jet split replaced by
the decay-independent leading GEN c-hadron (as the HWW FS study), applied by reweighting the 4FS sample:
  stitched dsigma = K4 sigma4 [region from 4FS] + K3 sigma3 [region from 3FS], rescaled to sigma_NNLO.
A 4FS event in the 4FS region gets   w = S K4
A 4FS event in the 3FS region gets   w(b) = S K3 sigma3(b) / sigma4(b)    (b = leading c-hadron pT bin)
with S = sigma_NNLO / (K4 sigma4(4FS region) + K3 sigma3(3FS region)). The 4FS sample normalised to its own
sigma4 (XSDB 97.62 fb) times w reproduces the stitched cross section per bin and sigma_NNLO in total.

Variants (FS uncertainty):
  nominal      : T = 5 GeV, N = 0 from 4FS, N >= 1 from 3FS   ("massive 1c"; c-hadron 5 GeV ~ c-jet 10 GeV)
  massless1c   : T = 5 GeV, N <= 1 from 4FS, N >= 2 from 3FS   (FS.pdf slide 22 option)
  T10          : T = 10 GeV, N = 0 from 4FS, N >= 1 from 3FS   (split threshold)
Per variant the weight is a function of (n_chad<T>, gen_chad1_pt). Eras are combined with inverse-variance
weights per bin (the four Run 3 eras are one physics point, 13.6 TeV) after a compatibility check.
K3 = 1.31, K4 = 0.76 (bbH NNLO/NLO, FS.pdf slide 31); sigma_NNLO = 73.30 fb (MiNNLOPS 4FS at 13.6 TeV,
LHCHWG-2026-007 Table 3: 0.1664 fb incl. BR(H->gg) 2.270e-3). Was 68.2 fb until 2026-10-04: that is the
FS.pdf slide-18 value, which goes with 4FS-FxFx NLO 90.13 fb, i.e. not 13.6 TeV (our XSDB 97.62 fb = Table 3 0.2216/2.27e-3).
Writes stitch_weights.json (+ .md summary)."""
import json, os, numpy as np

W = "/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02"
ERAS = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]
K3, K4, NNLO_PB = 1.31, 0.76, 0.1664e-3 / 2.270e-3   # 73.30 fb, see docstring
VARIANTS = {"nominal": (5, 1), "massless1c": (5, 2), "T10": (10, 1)}   # (T, minimum N taken from 3FS)


def load(era):
    return json.load(open(f"{W}/out/gg_{era}.json"))


def era_weights(r, T, nmin):
    """Weights per class/bin for one era. Returns dict with w_4fs (scalar, all 4FS-region events),
    w_3fs[c][b] for classes c >= nmin, their stat errors, and S."""
    s = {fs: {c: np.array(r[fs]["T"][str(T)][str(c)]["sigma_pb"]) for c in (0, 1, 2)} for fs in ("3FS", "4FS")}
    e = {fs: {c: np.array(r[fs]["T"][str(T)][str(c)]["err_pb"]) for c in (0, 1, 2)} for fs in ("3FS", "4FS")}
    s4_reg4 = sum(s["4FS"][c].sum() for c in range(nmin))          # 4FS region: N < nmin
    s3_reg3 = sum(s["3FS"][c].sum() for c in range(nmin, 3))       # 3FS region: N >= nmin
    S = NNLO_PB / (K4 * s4_reg4 + K3 * s3_reg3)
    # 3FS-region weights: merge the classes >= nmin per leading-pT bin (the weight depends on pT only)
    n3 = sum(s["3FS"][c] for c in range(nmin, 3)); n4 = sum(s["4FS"][c] for c in range(nmin, 3))
    e3 = np.sqrt(sum(e["3FS"][c] ** 2 for c in range(nmin, 3))); e4 = np.sqrt(sum(e["4FS"][c] ** 2 for c in range(nmin, 3)))
    ok = (n4 > 0) & (n3 != 0)
    w = np.where(ok, S * K3 * n3 / np.where(n4 > 0, n4, 1), np.nan)
    ew = np.where(ok, np.abs(w) * np.hypot(e3 / np.where(n3 != 0, n3, 1), e4 / np.where(n4 > 0, n4, 1)), np.nan)
    return dict(S=S, w4=S * K4, w3=w, ew3=ew, s4_reg4=s4_reg4, s3_reg3=s3_reg3,
                frac4_reg4=s4_reg4 / r["4FS"]["sigma_incl_pb"], frac3_reg3=s3_reg3 / r["3FS"]["sigma_incl_pb"])


def main():
    eras = [e for e in ERAS if os.path.exists(f"{W}/out/gg_{e}.json")]
    R = {e: load(e) for e in eras}
    edges = R[eras[0]]["edges"]
    out = {"edges_gen_chad1_pt": edges, "K3": K3, "K4": K4, "sigma_NNLO_fb": NNLO_PB * 1e3,
           "eras_used": eras, "inputs": {e: {fs: dict(files=R[e][fs]["files"], skipped=R[e][fs]["skipped"],
                                                      sigma_incl_fb=R[e][fs]["sigma_incl_pb"] * 1e3) for fs in ("3FS", "4FS")} for e in eras},
           "variants": {}}
    md = ["# FS stitching weights for the 4FS H+c signal (from official Run 3 H->gg, our rederivation)\n",
          f"K3 = {K3}, K4 = {K4}, sigma_NNLO = {NNLO_PB*1e3} fb. Leading GEN c-hadron pT bins: {edges[1:-1]} GeV (+overflow).\n"]
    for v, (T, nmin) in VARIANTS.items():
        per = {e: era_weights(R[e], T, nmin) for e in eras}
        Wm = np.array([per[e]["w3"] for e in eras]); Em = np.array([per[e]["ew3"] for e in eras])
        V = np.where(np.isfinite(Wm) & np.isfinite(Em) & (Em > 0), 1 / Em ** 2, 0)
        comb = np.where(V.sum(0) > 0, (np.nan_to_num(Wm) * V).sum(0) / np.where(V.sum(0) > 0, V.sum(0), 1), np.nan)
        ecomb = np.where(V.sum(0) > 0, 1 / np.sqrt(np.where(V.sum(0) > 0, V.sum(0), 1)), np.nan)
        w4 = np.mean([per[e]["w4"] for e in eras])
        # era compatibility of the 3FS-region weights
        chi2 = float(np.nansum(np.where(V > 0, (Wm - comb) ** 2 * V, 0))); ndf = int((V > 0).sum() - (V.sum(0) > 0).sum())
        out["variants"][v] = dict(T=T, n_min_from_3FS=nmin, w_4FS_region=float(w4),
                                  w_3FS_region=[None if not np.isfinite(x) else float(x) for x in comb],
                                  err_3FS_region=[None if not np.isfinite(x) else float(x) for x in ecomb],
                                  S_per_era={e: per[e]["S"] for e in eras}, w4_per_era={e: per[e]["w4"] for e in eras},
                                  era_chi2=chi2, era_ndf=ndf)
        md.append(f"\n## {v}: T = {T} GeV, N >= {nmin} from 3FS")
        md.append(f"- 4FS-region events (N < {nmin}): w = S K4 = {w4:.4f}  (per era: " + ", ".join(f"{per[e]['w4']:.4f}" for e in eras) + ")")
        md.append(f"- S per era: " + ", ".join(f"{e} {per[e]['S']:.4f}" for e in eras))
        md.append(f"- 4FS region fraction of sigma4: " + ", ".join(f"{per[e]['frac4_reg4']:.3f}" for e in eras)
                  + f"; 3FS region fraction of sigma3: " + ", ".join(f"{per[e]['frac3_reg3']:.3f}" for e in eras))
        md.append(f"- era compatibility of the 3FS-region weights: chi2/ndf = {chi2:.1f}/{ndf}")
        md.append("\n| leading c-hadron pT [GeV] | " + " | ".join(e for e in eras) + " | combined |")
        md.append("|---|" + "---|" * (len(eras) + 1))
        for b in range(1, len(edges) - 1):
            if not np.isfinite(comb[b]):
                continue
            hi = "inf" if edges[b + 1] > 1e8 else f"{edges[b+1]:.0f}"
            md.append(f"| {edges[b]:.0f}-{hi} | " + " | ".join(
                f"{per[e]['w3'][b]:.3f} ± {per[e]['ew3'][b]:.3f}" if np.isfinite(per[e]['w3'][b]) else "-" for e in eras)
                + f" | **{comb[b]:.3f} ± {ecomb[b]:.3f}** |")
    json.dump(out, open(f"{W}/stitch_weights.json", "w"), indent=1)
    open(f"{W}/stitch_weights.md", "w").write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
