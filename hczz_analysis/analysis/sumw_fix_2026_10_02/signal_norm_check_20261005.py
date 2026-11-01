import sys, os, json, numpy as np
sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts")
sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm")
sys.argv = [sys.argv[0]]
import eos_io as E
import create_datacards_ctag2d_100150 as dc
OUT = "/eos/user/s/snandaku/higgscharm/outputs"
BR = 0.02641 * 0.10099**2
def fam(part):
    return "H" if part.startswith("HPlusCharm") else ("S" if part.startswith("SomeSMSignal") else None)
res = {}
for tag, sumwdir, scored in [("old", f"{OUT}/hplusc_mva_4class_ctag2d", f"{OUT}/hplusc_mva_4class_ctag2d_scored_v8"),
                             ("new", f"{OUT}/hplusc_mva_4class_ctag2d_jecshifts_fschad", f"{OUT}/hplusc_mva_4class_ctag2d_fschad_scored_v1")]:
    sw = dc.load_sumw(sumwdir, key_fn=lambda era, part: (era, fam(part)) if fam(part) else None)
    for era in dc.ERAS:
        for f in ("H", "S"):
            win = tot = 0.0; n = 0; nfiles = 0
            for part in E.listdir(f"{scored}/{era}"):
                if fam(part) != f: continue
                for p in E.files(f"{scored}/{era}/{part}/base", ".parquet"):
                    df = E.read_parquet(p, ["zz_mass_inclusive", "mva_score_Signal", "weight_nominal"]).to_pandas()
                    w = df.weight_nominal.fillna(0).values; nfiles += 1
                    m = (df.zz_mass_inclusive >= 100) & (df.zz_mass_inclusive < 150) & (df.mva_score_Signal >= 0)
                    win += w[m.values].sum(); tot += w.sum(); n += int(m.sum())
            s = sw.get((era, f), 0.0)
            res[(tag, era, f)] = dict(sumw=s, win=win, tot=tot, n=n, nfiles=nfiles)
            if nfiles or s:
                y = 97.9e-3 * BR * dc.LUMINOSITY_PB[era] * win / s if s else float("nan")
                print(f"{tag} {era:13s} {f} files={nfiles:4d} n_win={n:6d} sumw={s:12.1f} sum_w_win={win:10.2f} eff={win/s if s else 0:.5f} yield@97.9={y:.5f}", flush=True)
json.dump({"|".join(k): v for k, v in res.items()}, open("chk.json", "w"), indent=1)
