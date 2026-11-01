"""0c / >=1c scale and PDF factors of the 4FS FxFx H+c sample from the REBUILT (fixed-weight) 4FS H->ZZ gridpack (2026-10-13).
Same c-hadron definition as fs_stitch_gg_2026_10_02/derive_chad_n.py (status 2, open charm, weakly decaying, not from b,
|eta| < 2.5, class = number with pT > T, T = 10 GeV). Replaces the flat 0c factors that the stitched-card builder took from the
official H->gamgam 4FS (gg_<era>.json, 4FS/T/10/0/sigma_scale_pb), needed while our private 4FS LHEScaleWeight was ~2x broken.
Per class: sum w, sum w^2, 6 scale points (LHEScaleWeight 0,1,3,5,7,8) and the 101 LHEPdfWeight (325500, mc_hessian).
'floorfix': events with the FxFx mu_R floor signature (LHEScaleWeight[1] = 6.199, [7] = 0.308, see note 10-12 sec. 18) get
their mu_R-varied weights (0,1,2,6,7,8) set to nominal.
usage: scan_new4fs.py <out.json> <label>=<file1,file2,...> [...]"""
import sys, json, glob, numpy as np, uproot, awkward as ak

T = 10.0
SC = [0, 1, 3, 5, 7, 8]
MUR = [0, 1, 2, 6, 7, 8]


def is_chad(apid):
    openc = ((apid > 400) & (apid < 500)) | ((apid > 4000) & (apid < 5000)) | \
            ((apid > 10400) & (apid < 10500)) | ((apid > 20400) & (apid < 20500))
    return openc & (((apid // 10) % 100) != 44)


def is_bhad(apid):
    return ((apid > 500) & (apid < 600)) | ((apid > 5000) & (apid < 6000))


def scan(files):
    acc = {v: {c: {"n": 0, "s": 0., "s2": 0., "sc": np.zeros(9), "scv": np.zeros(9), "pdf": np.zeros(101), "pdfv": np.zeros(101)}
               for c in ("0", "1p")} for v in ("raw", "floorfix")}
    nfloor = 0
    for f in files:
        with uproot.open(f) as tf:
            a = tf["Events"].arrays(["genWeight", "GenPart_eta", "GenPart_status", "GenPart_pdgId", "GenPart_pt",
                                     "GenPart_genPartIdxMother", "LHEScaleWeight", "LHEPdfWeight"])
        w = ak.to_numpy(a.genWeight).astype(float)
        apid = abs(a.GenPart_pdgId); mom = a.GenPart_genPartIdxMother
        ch = is_chad(apid) & (a.GenPart_status == 2)
        idx = ak.local_index(apid)
        has_c_daughter = ak.any(idx[:, :, None] == mom[is_chad(apid)][:, None, :], axis=-1)
        mom_pid = ak.fill_none(apid[ak.mask(mom, mom >= 0)], 0)
        sel = ch & ~has_c_daughter & ~is_bhad(mom_pid) & (abs(a.GenPart_eta) < 2.5)
        n = ak.to_numpy(ak.sum(a.GenPart_pt[sel] > T, axis=1))
        sw = ak.to_numpy(a.LHEScaleWeight).astype(float)
        pw = ak.to_numpy(a.LHEPdfWeight).astype(float)
        floor = (np.abs(sw[:, 1] - 6.199) < 2e-3) & (np.abs(sw[:, 7] - 0.308) < 2e-3)
        nfloor += int(floor.sum())
        swf = sw.copy(); swf[np.ix_(floor, MUR)] = 1.0
        for v, s in (("raw", sw), ("floorfix", swf)):
            for c, m in (("0", n == 0), ("1p", n >= 1)):
                h = acc[v][c]; wm = w[m]
                h["n"] += int(m.sum()); h["s"] += wm.sum(); h["s2"] += (wm ** 2).sum()
                h["sc"] += (wm[:, None] * s[m]).sum(0)
                h["pdf"] += (wm[:, None] * pw[m]).sum(0)
                # for the ratio error: sum (w*var - r*w)^2 needs r, so keep sum (w*var)^2, sum w^2*var
                h.setdefault("a2", np.zeros(9)); h.setdefault("ab", np.zeros(9))
                h["a2"] += ((wm[:, None] * s[m]) ** 2).sum(0); h["ab"] += (wm[:, None] ** 2 * s[m]).sum(0)
    out = {"files": len(files), "n_floor_events": nfloor}
    for v in acc:
        out[v] = {}
        for c, h in acc[v].items():
            r = h["sc"] / h["s"]
            err = np.sqrt(np.maximum(h["a2"] - 2 * r * h["ab"] + r ** 2 * h["s2"], 0)) / abs(h["s"])
            rp = h["pdf"] / h["s"]
            out[v][c] = dict(n=h["n"], sumw=h["s"], scale_ratio=r.tolist(), scale_ratio_err=err.tolist(),
                             scale_ratio_SC=[r[i] for i in SC], pdf_member0=rp[0],
                             pdf_hessian=float(np.sqrt(np.sum((rp[1:] / rp[0] - 1) ** 2))))
        tot = acc[v]["0"]["s"] + acc[v]["1p"]["s"]
        out[v]["frac0"] = acc[v]["0"]["s"] / tot
    return out


res = {"T": T, "scale_indices": SC, "mur_indices_floorfix": MUR}
for arg in sys.argv[2:]:
    lab, fl = arg.split("=", 1)
    files = sorted(sum((glob.glob(x) for x in fl.split(",")), []))
    res[lab] = scan(files)
    for v in ("raw", "floorfix"):
        o = res[lab][v]["0"]
        print(f"{lab:8s} {v:8s} files {len(files)} n0 {o['n']} f0 {res[lab][v]['frac0']:.3f} floor-ev {res[lab]['n_floor_events']} | 0c SC ratios "
              + " ".join(f"{x:.3f}±{e:.3f}" for x, e in zip(o['scale_ratio_SC'], [o['scale_ratio_err'][i] for i in SC]))
              + f" | pdf hess 0c {o['pdf_hessian']*100:.1f}% >=1c {res[lab][v]['1p']['pdf_hessian']*100:.1f}%", flush=True)
json.dump(res, open(sys.argv[1], "w"), indent=1)
