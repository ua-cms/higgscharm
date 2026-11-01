import json, os, shutil, re, collections, sys
TAG = "rerun_20261030"
BASE = "/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/condor"
S = os.path.dirname(os.path.abspath(__file__))
m = json.load(open(f"{S}/probe_result.json"))   # 2026-10-30 lepss: probe of the failed jobs inputs
unres = [k for k, v in m.items() if v is None]
if unres:
    sys.exit(f"{len(unres)} unresolved files, stop")
jobs = collections.defaultdict(list)
for line in open(f"{S}/missing.txt"):
    wf, era, ds, j = line.split()
    jobs[f"{wf}/{era}/{ds}"].append(j)
subs = []
for d, js in sorted(jobs.items()):
    src = f"{BASE}/{d}"
    dst = f"{src}/{TAG}"
    os.makedirs(dst, exist_ok=True)
    p = json.load(open(f"{src}/partitions.json"))
    for j in js:
        p[j] = {ds: [m[f] for f in fl] for ds, fl in p[j].items()}
    json.dump(p, open(f"{dst}/partitions.json", "w"), indent=4)
    shutil.copy(f"{src}/arguments.json", f"{dst}/arguments.json")
    open(f"{dst}/jobnum.txt", "w").write("\n".join(sorted(js, key=int)) + "\n")
    orig = [f for f in os.listdir(src) if f.endswith(".sub") and not f.startswith("resub")][0]
    sub = open(f"{src}/{orig}").read()
    sub = sub.replace(f"{src}/partitions.json", f"{dst}/partitions.json") \
             .replace(f"{src}/arguments.json", f"{dst}/arguments.json")
    sub = re.sub(rf"{re.escape(src)}/jobnum\.txt", f"{dst}/jobnum.txt", sub)
    assert sub.count(dst) == 4, (d, sub.count(dst))
    open(f"{dst}/{TAG}.sub", "w").write(sub)
    subs.append(f"{dst}/{TAG}.sub")
open(f"{S}/rerun_subs.txt", "w").write("\n".join(subs) + "\n")
print(len(subs), "datasets,", sum(map(len, jobs.values())), "jobs")
