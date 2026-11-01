"""GEN c-hadron spectra of H+c 3FS and 4FS FxFx, split by the NUMBER of c-hadrons (2026-10-02).
Extends the HWW derive_chad.py (C. Gupta, /eos/user/c/cgupta/HToWW/fs_unc/gen/) with the c-hadron
multiplicity, needed for the FS.pdf (T. Bevilacqua) stitching and its "massless 1c" variant:
  massive 1c (nominal): N = 0 from 4FS FxFx, N >= 1 from 3FS
  massless 1c         : N <= 1 from 4FS FxFx, N >= 2 from 3FS
c-hadron = NanoAOD GenPart, status 2, open charm (|pdgId| 4xx/4xxx/104xx/204xx, charmonium excluded),
weakly decaying (no c-hadron daughter), not from a b-hadron, |eta| < 2.5, pT > T (T = 5, 10 GeV).
Same definition as HWW, so it is independent of the Higgs decay (applies to H->ZZ->4l).
Per scheme, T and multiplicity class (0, 1, >=2): sum w, sum w^2 and 6 scale-varied sums
(LHEScaleWeight 0,1,3,5,7,8) in bins of the leading c-hadron pT.
Normalisation: 3FS sigma = Runs genEventSumw/genEventCount; 4FS FxFx sigma = XSDB (argv).
Each file is retried up to 3 times on read errors (transient xrootd "vector_read" failures); a file
that still fails is skipped consistently (neither its events nor its Runs sumw are counted).
usage: derive_chad_n.py <out_prefix> fl3.txt fl4FXFX.txt <xsdb4_pb> [maxfiles]"""
import sys, json, numpy as np, uproot, awkward as ak

out, fl3, fl4, XS4 = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
MAXF = int(sys.argv[5]) if len(sys.argv) > 5 else 10**6
TS = [5, 10]
EDGES = np.array([0, 5, 10, 15, 20, 30, 45, 60, 80, 110, 1e9], float)   # leading c-hadron pT; bin 0 = no c-hadron
NB = len(EDGES) - 1
SC = [0, 1, 3, 5, 7, 8]
NTRY = 3
COLS = ["genWeight", "GenPart_eta", "GenPart_status", "GenPart_pdgId", "GenPart_pt",
        "GenPart_genPartIdxMother", "LHEScaleWeight"]


def is_chad(apid):
    openc = ((apid > 400) & (apid < 500)) | ((apid > 4000) & (apid < 5000)) | \
            ((apid > 10400) & (apid < 10500)) | ((apid > 20400) & (apid < 20500))
    return openc & (((apid // 10) % 100) != 44)


def is_bhad(apid):
    return ((apid > 500) & (apid < 600)) | ((apid > 5000) & (apid < 6000))


def empty():
    return {T: {c: {"s": np.zeros(NB), "s2": np.zeros(NB), "sc": np.zeros((len(SC), NB))} for c in (0, 1, 2)} for T in TS}


def read_file(url):
    """All sums of one file; raises on any read error (nothing is committed for a partial file)."""
    with uproot.open(url, timeout=600) as tf:
        r = tf["Runs"].arrays(["genEventSumw", "genEventCount"])
        srun, crun = float(ak.sum(r.genEventSumw)), float(ak.sum(r.genEventCount))
        loc = {"n": 0, "nneg": 0, "sw": 0.0}; locH = empty()
        for a in tf["Events"].iterate(COLS, step_size="200 MB"):
            w = ak.to_numpy(a.genWeight).astype(float)
            loc["n"] += len(w); loc["nneg"] += int((w < 0).sum()); loc["sw"] += w.sum()
            apid = abs(a.GenPart_pdgId); mom = a.GenPart_genPartIdxMother
            ch = is_chad(apid) & (a.GenPart_status == 2)
            idx = ak.local_index(apid)
            ch_moms = mom[is_chad(apid)]
            has_c_daughter = ak.any(idx[:, :, None] == ch_moms[:, None, :], axis=-1)
            mom_pid = ak.fill_none(apid[ak.mask(mom, mom >= 0)], 0)
            sel = ch & ~has_c_daughter & ~is_bhad(mom_pid) & (abs(a.GenPart_eta) < 2.5)
            cpt = a.GenPart_pt[sel]
            lead = ak.to_numpy(ak.fill_none(ak.max(cpt, axis=1), -1.0))
            sw = ak.to_numpy(ak.fill_none(ak.pad_none(a.LHEScaleWeight, 9, clip=True), 1.0)).astype(float)
            for T in TS:
                n = ak.to_numpy(ak.sum(cpt > T, axis=1))
                cls = np.minimum(n, 2)
                x = np.where(n > 0, lead, 0.5)          # no c-hadron above T -> bin 0
                for c in (0, 1, 2):
                    m = cls == c
                    h = locH[T][c]
                    h["s"] += np.histogram(x[m], EDGES, weights=w[m])[0]
                    h["s2"] += np.histogram(x[m], EDGES, weights=w[m] ** 2)[0]
                    for k, i in enumerate(SC):
                        h["sc"][k] += np.histogram(x[m], EDGES, weights=w[m] * sw[m, i])[0]
    return srun, crun, loc, locH


def run(filelist):
    files = [l.strip() for l in open(filelist) if l.strip()][:MAXF]
    acc = {"n": 0, "nneg": 0, "sw": 0.0, "sumw_runs": 0.0, "count_runs": 0.0, "files": 0, "skipped": 0}
    H = empty()
    for f in files:
        url = f if f.startswith("root://") else "root://cms-xrd-global.cern.ch/" + f
        for attempt in range(1, NTRY + 1):
            try:
                srun, crun, loc, locH = read_file(url)
            except Exception as ex:
                if attempt < NTRY:
                    print("  retry %d %s : %s" % (attempt, f.split("/")[-1], str(ex)[:100]), flush=True)
                    continue
                acc["skipped"] += 1
                print("  SKIP %s : %s" % (f, str(ex)[:150]), flush=True)
                break
            acc["sumw_runs"] += srun; acc["count_runs"] += crun; acc["files"] += 1
            for k in loc: acc[k] += loc[k]
            for T in TS:
                for c in (0, 1, 2):
                    for q in ("s", "s2", "sc"): H[T][c][q] += locH[T][c][q]
            print("  ok %s  (events so far %d)" % (f.split("/")[-1], acc["n"]), flush=True)
            break
    return acc, H


data = {}
for fs, fl in (("3FS", fl3), ("4FS", fl4)):
    print("==", fs, fl, flush=True)
    data[fs] = run(fl)
res = {"edges": EDGES.tolist(), "scale_indices": SC,
       "definition": "leading weakly-decaying open-charm GEN hadron, not from b, |eta|<2.5; class = number of such hadrons with pT>T (0,1,>=2)"}
for fs in ("3FS", "4FS"):
    acc, H = data[fs]
    s_pre = acc["sumw_runs"] / acc["count_runs"]
    s_incl = s_pre if fs == "3FS" else XS4
    norm = s_incl / acc["sw"]          # pb per unit genWeight
    res[fs] = dict(files=acc["files"], skipped=acc["skipped"], nev=acc["n"], negfrac=acc["nneg"] / max(acc["n"], 1),
                   sigma_incl_pb=s_incl, sigma_sumw_over_n_pb=s_pre, events_vs_runs_sumw=acc["sw"] / acc["sumw_runs"],
                   T={str(T): {str(c): dict(sigma_pb=(H[T][c]["s"] * norm).tolist(), err_pb=(np.sqrt(H[T][c]["s2"]) * norm).tolist(),
                                            sigma_scale_pb=(H[T][c]["sc"] * norm).tolist()) for c in (0, 1, 2)} for T in TS})
    for T in TS:
        fr = [float(np.sum(H[T][c]["s"]) * norm / s_incl) for c in (0, 1, 2)]
        print(f"{fs} T={T}: sigma {s_incl*1e3:.2f} fb, fractions N=0/1/>=2: {fr[0]:.3f} {fr[1]:.3f} {fr[2]:.3f}", flush=True)
json.dump(res, open(f"{out}.json", "w"), indent=1)
print("written", f"{out}.json", flush=True)
