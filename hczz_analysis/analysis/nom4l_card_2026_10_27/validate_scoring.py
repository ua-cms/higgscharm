#!/usr/bin/env python3
"""
2026-10-27: validate a new MVA scoring of the HcZZ card inputs against the production scoring (scored_v1)
before any card is built (past problems: 2024 parquets without UParT columns -> all-L0 categories;
Z+X scored with era=None -> legacy c-tag boundaries).

usage: validate_scoring.py <out.json> --pair RAW NEW REF [--pair ...] --config CFG --model MODEL
                           [--ref-config CFG] [--ref-model MODEL] [--nsample 40]
  RAW = unscored production (input of the scoring), NEW = new scored dir, REF = production scored dir (scored_v1).

Checks (any failure -> exit 1):
 1. completeness: the set of parquet files (all eras, nominal + shift dirs) of NEW equals REF, and every file
    has the same number of rows (parquet metadata).
 2. content, on NSAMPLE random nominal files per pair: all non-score columns identical to REF (same events, same
    order); mva_score_{qqZZ,ggZZ,Signal,Other_Higgs} present, finite, in [0,1], summing to 1.
 3. reproducibility: the same files re-scored here from RAW with MVAPostProcessor(CONFIG, MODEL, era) give the
    stored NEW scores (|diff| < 1e-5).
 4. inputs: the feature matrix of CONFIG equals the production (REF config) one with exactly the dropped columns
    removed (column-by-column alignment); reports which production columns are missing (expected: m4l x2 and
    in_mass_window) -- c-tag categories, jets, leptons are then identical to the validated production inputs.
"""
import os, sys, json, random, argparse
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pyarrow.parquet as pq

H = "/eos/user/s/snandaku/Higgscharmnew/higgscharm"
sys.path.insert(0, H)
from analysis.postprocess.mva_inference import MVAPostProcessor

SCORES = ["mva_score_qqZZ", "mva_score_ggZZ", "mva_score_Signal", "mva_score_Other_Higgs"]
ERAS = ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]

ap = argparse.ArgumentParser()
ap.add_argument("out")
ap.add_argument("--pair", nargs=3, action="append", required=True, metavar=("RAW", "NEW", "REF"))
ap.add_argument("--config", required=True)
ap.add_argument("--model", required=True)
ap.add_argument("--ref-config", default=f"{H}/models/hc_zzto4l_mw_training_4class_nomass.yml")
ap.add_argument("--ref-model", default=f"{H}/models/best_model.pt")
ap.add_argument("--nsample", type=int, default=40)
a = ap.parse_args()

report = {"pairs": [], "ok": True}
fail = lambda msg: (report.setdefault("failures", []).append(msg), print("FAIL:", msg), report.__setitem__("ok", False))


def listing(root):
    out = {}
    for era in ERAS:
        for dp, _, fs in os.walk(os.path.join(root, era)):
            for f in fs:
                if f.endswith(".parquet"):
                    out[os.path.relpath(os.path.join(dp, f), root)] = os.path.join(dp, f)
    return out


def nrows(p):
    return pq.ParquetFile(p).metadata.num_rows


procs = {}  # (kind, era) -> processor


def proc(kind, era):
    if (kind, era) not in procs:
        cfg, mod = (a.config, a.model) if kind == "new" else (a.ref_config, a.ref_model)
        procs[(kind, era)] = MVAPostProcessor(bhive_config_path=cfg, bhive_model_path=mod, apply_mass_window=False, era=era)
    return procs[(kind, era)]


for raw, new, ref in a.pair:
    rec = {"raw": raw, "new": new, "ref": ref}
    print(f"\n=== {new}\n    ref {ref}")
    # 1. completeness
    ln, lr = listing(new), listing(ref)
    rec["n_files_new"], rec["n_files_ref"] = len(ln), len(lr)
    miss, extra = sorted(set(lr) - set(ln)), sorted(set(ln) - set(lr))
    if miss:
        fail(f"{new}: {len(miss)} files missing vs ref, e.g. {miss[:3]}")
    if extra:
        fail(f"{new}: {len(extra)} files not in ref, e.g. {extra[:3]}")
    common = sorted(set(ln) & set(lr))
    with ThreadPoolExecutor(32) as ex:
        rn = list(ex.map(nrows, [ln[k] for k in common]))
        rr = list(ex.map(nrows, [lr[k] for k in common]))
    bad = [k for k, x, y in zip(common, rn, rr) if x != y]
    rec["rows_new"], rec["rows_ref"], rec["n_row_mismatch"] = int(sum(rn)), int(sum(rr)), len(bad)
    if bad:
        fail(f"{new}: {len(bad)} files with different row counts, e.g. {bad[:3]}")
    print(f"  1. files {len(ln)} (ref {len(lr)}), rows {sum(rn)} (ref {sum(rr)}), row mismatches {len(bad)}")

    # 2-4 on a sample of nominal files (base/<file>.parquet, not shift dirs)
    nominal = [k for k in common if os.path.basename(os.path.dirname(k)) == "base"]
    random.Random(42).shuffle(nominal)
    sample, i = [], 0
    for k in nominal:          # prefer files with events
        if nrows(ln[k]) > 0:
            sample.append(k)
        if len(sample) >= a.nsample:
            break
    rec["sample"] = []
    for k in sample:
        era = k.split(os.sep)[0]
        dn = pq.read_table(ln[k]).to_pandas()
        dr = pq.read_table(lr[k]).to_pandas()
        r = {"file": k, "rows": len(dn)}
        other = [c for c in dr.columns if not c.startswith("mva_")]
        diffcols = []
        for c in other:
            if c not in dn.columns:
                diffcols.append(c + " (missing)")
                continue
            x, y = dn[c].values, dr[c].values
            try:
                same = np.array_equal(np.asarray(x, float), np.asarray(y, float), equal_nan=True)
            except (TypeError, ValueError):
                same = all(str(p) == str(q) for p, q in zip(x, y))
            if not same:
                diffcols.append(c)
        if diffcols:
            fail(f"{k}: non-score columns differ from ref: {diffcols[:5]}")
        missing = [c for c in SCORES if c not in dn.columns]
        if missing:
            fail(f"{k}: score columns missing {missing}")
            continue
        S = dn[SCORES].values.astype(float)
        if not np.isfinite(S).all() or (S < 0).any() or (S > 1).any():
            fail(f"{k}: scores not finite / outside [0,1]")
        if np.abs(S.sum(1) - 1).max() > 1e-4:
            fail(f"{k}: class scores do not sum to 1 (max dev {np.abs(S.sum(1) - 1).max():.2e})")
        # 3. reproducibility from the raw input
        rawdf = pq.read_table(os.path.join(raw, k)).to_pandas()
        pn = proc("new", era)
        Xn = pn.prepare_features(rawdf)
        pred = pn.predict(Xn)
        names = list(pn.class_names)
        dmax = max(float(np.abs(pred["scores"][:, names.index(c.replace("mva_score_", ""))] - dn[c].values).max())
                   for c in SCORES)
        r["rescore_maxdiff"] = dmax
        if dmax > 1e-5:
            fail(f"{k}: re-scored values differ from stored by {dmax:.2e}")
        # 4. inputs vs production
        Xp = proc("ref", era).prepare_features(rawdf)
        # 2026-10-27: mass-invariant configs divide some inputs by m4l -> undo it for the alignment (exact x m4l
        # can differ in the last float32 bit, so the comparison below is np.allclose with rtol 1e-6 for those)
        pn = proc("new", era)
        scaled_cols = []
        if pn.config.get("mass_scaled_features"):
            import itertools
            lay = list(pn.config.get("global_features", []))
            for grp, nk in (("cpf_candidates", "n_cpf_candidates"), ("npf_candidates", "n_npf_candidates"),
                            ("vtx_features", "n_vtx_candidates"), ("lt_candidates", "n_lt_candidates")):
                lay += [f for _ in range(int(pn.config.get(nk, 1))) for f in pn.config.get(grp, [])]
            scaled_cols = [i for i, f in enumerate(lay) if f in set(pn.config["mass_scaled_features"])]
            m4l_raw = rawdf["zz_mass_inclusive"].values.astype(np.float64)
            Xn = Xn.astype(np.float64).copy(); Xn[:, scaled_cols] *= m4l_raw[:, None]
            r["mass_scaled_cols"] = scaled_cols
        same = lambda a, b, c: (np.allclose(a, b, rtol=1e-5, atol=1e-6, equal_nan=True) if c in scaled_cols
                                else np.array_equal(a, b, equal_nan=True))
        # 2026-10-28: configs with the new HcZZ inputs (hczz_extra_features: angles + 4l/jet kinematics, not in
        # production) and importance-pruned inputs -> set-based alignment: every NON-new column must equal some
        # (unused) production column, new columns must be finite; the number of dropped production columns is
        # reported, not required (pruning). The new features themselves were checked vs b-hive (check_scorer.py, 77/77).
        from analysis.postprocess.mva_inference import HCZZ_EXTRA_FEATURES
        lay = list(pn.config.get("global_features", []))
        for grp, nk in (("cpf_candidates", "n_cpf_candidates"), ("npf_candidates", "n_npf_candidates"),
                        ("vtx_features", "n_vtx_candidates"), ("lt_candidates", "n_lt_candidates")):
            lay += [f for _ in range(int(pn.config.get(nk, 1))) for f in pn.config.get(grp, [])]
        extra_cols = [i for i, f in enumerate(lay) if f in set(HCZZ_EXTRA_FEATURES)] if len(lay) == Xn.shape[1] else []
        if extra_cols:
            r["extra_cols"] = extra_cols
            if not np.isfinite(Xn[:, extra_cols].astype(np.float64)).all():
                fail(f"{k}: new HcZZ input columns not finite")
            used, nomatch = set(), []
            for col in range(Xn.shape[1]):
                if col in extra_cols:
                    continue
                hit = next((jj for jj in range(Xp.shape[1]) if jj not in used and same(Xp[:, jj], Xn[:, col], col)), None)
                if hit is None:
                    nomatch.append(col)
                else:
                    used.add(hit)
            if nomatch:
                fail(f"{k}: new input columns {nomatch} ({[lay[c] for c in nomatch]}) match no production input")
            unmatched = [jj for jj in range(Xp.shape[1]) if jj not in used]
        else:
            j, unmatched = 0, []
            for col in range(Xn.shape[1]):
                while j < Xp.shape[1] and not same(Xp[:, j], Xn[:, col], col):
                    unmatched.append(j)
                    j += 1
                if j == Xp.shape[1]:
                    fail(f"{k}: new input column {col} not found in production inputs (in order)")
                    break
                j += 1
            unmatched += list(range(j, Xp.shape[1]))
        m4l = rawdf["zz_mass_inclusive"].values.astype(np.float32)
        r["dropped_prod_cols"] = unmatched
        r["dropped_is_m4l"] = [bool(np.allclose(Xp[:, u], m4l, rtol=1e-6)) for u in unmatched]
        r["n_inputs"] = [int(Xn.shape[1]), int(Xp.shape[1])]
        if not extra_cols and len(unmatched) != Xp.shape[1] - Xn.shape[1]:
            fail(f"{k}: input alignment found {len(unmatched)} dropped columns, expected {Xp.shape[1] - Xn.shape[1]}")
        r["mean_scores"] = {c: float(dn[c].mean()) for c in SCORES}
        rec["sample"].append(r)
        print(f"  {k}: rows {len(dn)}, rescore maxdiff {dmax:.1e}, dropped prod cols {unmatched} "
              f"(m4l: {r['dropped_is_m4l']})")
    report["pairs"].append(rec)

json.dump(report, open(a.out, "w"), indent=1)
print("\nVALIDATION", "PASSED" if report["ok"] else "FAILED", "->", a.out)
sys.exit(0 if report["ok"] else 1)
