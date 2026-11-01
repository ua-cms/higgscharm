# 2026-10-25: rewrite condor/<wf>/2024/<ds>/partitions.json with site-pinned URLs (probe_reused.json + probe_new.json);
# unresolved files (private H+c at IIHE: xrdfs auth fails from lxplus) keep their cms-xrd-global URL.
import json, sys, collections
P = "/eos/home-s/snandaku/Analysis/hczz_2024_prod_upart_2026_10_25"
m = json.load(open(f"{P}/probe_reused.json")); m.update({k: v for k, v in json.load(open(f"{P}/probe_new.json")).items() if v})
part = sys.argv[1]
p = json.load(open(part))
c = collections.Counter()
for j, d in p.items():
    for ds, fl in d.items():
        d[ds] = [m.get(f, f) for f in fl]
        c.update("pinned" if f in m else "global" for f in fl)
json.dump(p, open(part, "w"), indent=4)
print(part.split("/")[-2], dict(c))
