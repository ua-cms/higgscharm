#!/usr/bin/env python3
"""
POST-jet-veto version of sumw_table.json (2026-10-19, user decision for the presentation:
normalise MC as base.py does, i.e. with sumw taken after apply_jetvetomaps).

sumw_table_postveto[kind][era][group] = sumw_table[kind][era][group] * eps(era, group), with
  eps(era, group) = sum(sidecar post-veto 'sumw') / sum(pre-veto chunk sumw from chunk_sumw.json)
over the nominal-production chunks that have a sidecar. Using one eps per (era, group) keeps
  - the coverage-gap fix (chunks without a sidecar are still counted, at the group's eps),
  - the JES/JER per-shift chunk lists (the jecshifts productions have no sidecars of their own).
Output: sumw_table_postveto.json, veto_eff.json (eps, chunk coverage per group).
"""
import os, re, sys, json, collections
sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts")
import eos_io as E
import create_datacards_ctag2d_100150 as dc

D = os.path.dirname(os.path.abspath(__file__))
PROD = "/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d"   # nominal production (sidecars)
CH = re.compile(r"^(.+?)_%2FEvents%3B1_(\d+)-(\d+)\.json$")
pre = json.load(open(f"{D}/chunk_sumw.json"))
table = json.load(open(f"{D}/sumw_table.json"))

acc = collections.defaultdict(lambda: [0.0, 0.0, 0, 0])   # (era, g) -> [post, pre, n matched, n sidecars]
for era in dc.ERAS:
    for s in E.listdir(os.path.join(PROD, era)):
        proc = dc.get_process_from_path(s)
        if proc is None or proc == "Signal":
            continue
        d = os.path.join(PROD, era, s, "sumw")
        if not E.isdir(d):
            continue
        g = dc.norm_group(s)
        for jf in E.files(d, ".json"):
            m = CH.match(os.path.basename(jf))
            if not m:
                continue
            a = acc[(era, g)]
            a[3] += 1
            v = pre.get(f"{m.group(1)}|{m.group(2)}-{m.group(3)}")
            if v is None:
                continue
            a[0] += E.read_json(jf)["sumw"]
            a[1] += v
            a[2] += 1

eps = {}
for (era, g), (po, pr, n, ns) in sorted(acc.items()):
    if n and pr:
        eps[(era, g)] = po / pr
        print(f"{era:<14} {g:<36} eps {po / pr:.4f}  ({n}/{ns} sidecars matched)")

out, miss = {}, []
for k, eras in table.items():
    for era, groups in eras.items():
        for g, rec in groups.items():
            e = eps.get((era, g))
            if e is None:
                if k.startswith("sig"):
                    e = 1.0          # signal: unused in stitched mode (sidecar post-veto sumw used)
                else:
                    miss.append((k, era, g))
                    continue
            out.setdefault(k, {}).setdefault(era, {})[g] = {**rec, "sumw": rec["sumw"] * e, "veto_eff": e}
if miss:
    raise SystemExit(f"no veto efficiency for {len(set((e, g) for _, e, g in miss))} (era, group): "
                     f"{sorted(set((e, g) for _, e, g in miss))}")
json.dump(out, open(f"{D}/sumw_table_postveto.json", "w"), indent=1)
json.dump({f"{e}|{g}": {"eps": v, "post": acc[(e, g)][0], "pre": acc[(e, g)][1], "n_matched": acc[(e, g)][2],
                        "n_sidecars": acc[(e, g)][3]} for (e, g), v in eps.items()},
          open(f"{D}/veto_eff.json", "w"), indent=1)
print("wrote sumw_table_postveto.json, veto_eff.json")
