#!/usr/bin/env python3
"""
2026-11-03: Z+X normalisation uncertainty per final state, computed from our own inputs (HIG-24-013 sec. 8
lists the same three sources: CR statistics, fake-rate +-1 sigma, composition of the fake sources; it quotes
25-46% by final state, we compute ours).

Per era and final state (4e, 4mu, 2e2mu, 2mu2e), with the OS method of estimate_zx_background.py (same
functions, merged CR, ZZ correction, m4l window):
  stat   : sqrt(sum w^2) of the 3P1F (ZZ-corrected) and 2P2F weights (as the estimator)
  fr_e   : all electron fake rates shifted by +-1 sigma together (muon ones fixed), half the Up-Down spread
  fr_mu  : same for muons
No composition term: the fake-rate composition was studied and validated by pkatris (user, 2026-11-03), and
an SS-method comparison is not used (backup with it: zx_fs_uncertainty.py.bak_20261103_with_ss).
Combination: the FR maps are measured per era -> FR terms in quadrature across eras and between e and mu;
CR stat in quadrature. Card channels (4e, 4mu, 2e2mu = 2e2mu + 2mu2e): within an era the two mixed final
states share the FR maps -> their signed FR shifts add linearly, then quadrature over eras.
Output <out>/zx_fs_uncertainty.json: per era and per final state, plus "channels" (4e, 4mu, 2e2mu) and
"inclusive" with relative stat / fr / total, read by the card builders (HCZZ_ZX_UNC_JSON).
--summary-only recomputes the summary from an existing json (no CR reprocessing).

usage: python3 scripts/zx_fs_uncertainty.py --workflow hplusc_mva_4class_CR_merged_lepss \
          --merged-base <..._merged_perjet> --fr-dir /eos/user/p/pkatris/higgscharm/outputs/fake_rates \
          --mass-window 105 160 --out <dir>
"""
import argparse
import copy
import json
import os
import sys
from glob import glob

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import estimate_zx_background as Z  # noqa: E402

ERAS = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]
FS = ["4e", "4mu", "2e2mu", "2mu2e"]


def shifted_fr(fr, flavour, sign):
    out = copy.deepcopy(fr)
    f, e = out[flavour]["fake_rate"], np.array(out[flavour]["fake_rate_err"], dtype=float)
    e = np.where(e >= 1.0, 0.0, e)   # sentinel bins (no measurement) keep their value, as get_fake_rate
    out[flavour]["fake_rate"] = np.clip(f + sign * e, 0.0, 0.99)
    return out


def fs_labels(df, cat):
    return np.array([Z.get_final_state(a, b) for a, b in zip(df[f"z1_l1_pdgid_{cat}"].values, df[f"l3_pdgid_{cat}"].values)])


def os_yields(df3, df2, fs3, fs2, zz_corr, fr):
    w3 = zz_corr * Z.compute_weights_3p1f(df3, fr)
    w2 = Z.compute_weights_2p2f(df2, fr) if len(df2) else np.array([])
    y = {fs: float(w3[fs3 == fs].sum() - (w2[fs2 == fs].sum() if len(df2) else 0.0)) for fs in FS}
    s = {fs: float(np.sqrt((w3[fs3 == fs] ** 2).sum() + ((w2[fs2 == fs] ** 2).sum() if len(df2) else 0.0))) for fs in FS}
    return y, s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", required=True)
    ap.add_argument("--merged-base", required=True)
    ap.add_argument("--fr-dir", required=True)
    ap.add_argument("--mass-window", type=float, nargs=2, default=(105.0, 160.0))
    ap.add_argument("--out", required=True)
    ap.add_argument("--summary-only", action="store_true")
    a = ap.parse_args()
    js = os.path.join(a.out, "zx_fs_uncertainty.json")
    if a.summary_only:
        d = json.load(open(js))
        write_summary(d["per_era"], d, js)
        return
    Z.MERGED_BASE = a.merged_base
    Z.CR_BASE = f"/eos/user/s/snandaku/higgscharm/outputs/{a.workflow}"
    Z.SR_M4L_MIN, Z.SR_M4L_MAX = a.mass_window
    os.makedirs(a.out, exist_ok=True)

    res = {}
    for era in ERAS:
        fr_os = Z.load_fake_rates(os.path.join(a.fr_dir, "OS", era, "fake_rates.pkl"))
        fr_ss = Z.load_fake_rates(os.path.join(a.fr_dir, "SS", era, "fake_rates.pkl"))
        df3, df2 = Z.load_cr_data(era, "CR_3P1F"), Z.load_cr_data(era, "CR_2P2F")
        zz_corr, _, _ = Z.compute_zz_correction(era, category="CR_3P1F", df_data=df3)
        fs3 = fs_labels(df3, "3p1f")
        fs2 = fs_labels(df2, "2p2f") if len(df2) else np.array([])
        nom, stat = os_yields(df3, df2, fs3, fs2, zz_corr, fr_os)
        var = {}
        for flav, key in (("electron", "fr_e"), ("muon", "fr_mu")):
            up, _ = os_yields(df3, df2, fs3, fs2, zz_corr, shifted_fr(fr_os, flav, +1))
            dn, _ = os_yields(df3, df2, fs3, fs2, zz_corr, shifted_fr(fr_os, flav, -1))
            var[key] = {fs: 0.5 * abs(up[fs] - dn[fs]) for fs in FS}
            var[key + "_updown"] = {fs: (up[fs], dn[fs]) for fs in FS}
        res[era] = {"zz_correction": zz_corr, "nominal": nom, "stat": stat, "fr_e_updown": var["fr_e_updown"],
                    "fr_mu_updown": var["fr_mu_updown"]}
        print(f"\n{era}: ZZ corr {zz_corr:.4f}")
        for fs in FS:
            print(f"  {fs:<6} OS {nom[fs]:7.2f} stat {stat[fs]:5.2f} FR_e {var['fr_e'][fs]:5.2f} FR_mu {var['fr_mu'][fs]:5.2f}")

    write_summary(res, {"mass_window": list(a.mass_window), "workflow": a.workflow, "merged_base": a.merged_base,
                        "fr_dir": a.fr_dir}, js)


def write_summary(res, meta, js):
    groups = {"4e": ["4e"], "4mu": ["4mu"], "2e2mu": ["2e2mu", "2mu2e"], "inclusive": FS}

    def combine(fss):
        n = sum(res[e]["nominal"][fs] for e in ERAS for fs in fss)
        stat = np.sqrt(sum(res[e]["stat"][fs] ** 2 for e in ERAS for fs in fss))
        fr = {}
        for fl in ("fr_e", "fr_mu"):
            # signed half Up-Down shift; linear over final states of one era (same FR map), quadrature over eras
            fr[fl] = np.sqrt(sum(sum(0.5 * (res[e][fl + "_updown"][fs][0] - res[e][fl + "_updown"][fs][1]) for fs in fss) ** 2
                                 for e in ERAS))
        frt = np.sqrt(fr["fr_e"] ** 2 + fr["fr_mu"] ** 2)
        rel = {"stat": stat / n, "fr_e": fr["fr_e"] / n, "fr_mu": fr["fr_mu"] / n, "fr": frt / n}
        rel["total"] = float(np.sqrt(rel["stat"] ** 2 + rel["fr"] ** 2))
        return {"nominal": n, "rel": {k: float(v) for k, v in rel.items()}}

    out = {g: combine(f) for g, f in groups.items()}
    print("\nAll eras (relative):")
    print(f"  {'channel':<10} {'N':>7} {'stat':>7} {'FR_e':>6} {'FR_mu':>6} {'total':>7}")
    for g, v in out.items():
        r = v["rel"]
        print(f"  {g:<10} {v['nominal']:7.2f} {r['stat']:7.1%} {r['fr_e']:6.1%} {r['fr_mu']:6.1%} {r['total']:7.1%}")
    d = {k: v for k, v in meta.items() if k not in ("per_era", "all_eras", "channels", "inclusive")}
    d.update({"per_era": res, "channels": {g: out[g] for g in ("4e", "4mu", "2e2mu")}, "inclusive": out["inclusive"]})
    json.dump(d, open(js, "w"), indent=1, default=float)


if __name__ == "__main__":
    main()
