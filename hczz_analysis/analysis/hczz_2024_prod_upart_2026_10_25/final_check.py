# 2026-10-25: completeness of the 2024 SR rerun with UParT columns (copy of hczz_2024_prod_2026_10_23/final_check.py).
# Also: per dataset, the first nominal parquet must contain jet_btagUParTAK4CvL.
# Per job: <ds>_<N>.coffea marker; MC only: distinct UUIDs of <ds>_<N>/sumw/*.json == number of input files.
import json, os, sys
B = "/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/condor"
O = "/eos/user/s/snandaku/higgscharm/outputs"
P = os.path.dirname(os.path.abspath(__file__))
DATA = ("EGamma", "Muon")
bad, nok, ntot = [], 0, 0
upart_checked = set()
import pyarrow.parquet as pq
for line in open(f"{P}/submitted.txt"):
    wf, ds = line.split()[:2]
    d = f"{wf}/2024/{ds}"
    have = set(os.listdir(f"{O}/{d}")) if os.path.isdir(f"{O}/{d}") else set()
    p = json.load(open(f"{B}/{d}/partitions.json"))
    for j in open(f"{B}/{d}/jobnum.txt").read().split():
        ntot += 1
        if f"{ds}_{j}.coffea" not in have:
            bad.append(f"{d} {j} nomarker"); continue
        if not ds.startswith(DATA):
            nin = sum(len(v) for v in p[j].values())
            sd = f"{O}/{wf}/2024/{ds}_{j}/sumw"
            u = {f.split("_")[0] for f in os.listdir(sd)} if os.path.isdir(sd) else set()
            if len(u) != nin:
                bad.append(f"{d} {j} sumw_uuids {len(u)}/{nin}"); continue
        nok += 1
        if (wf, ds) not in upart_checked:
            upart_checked.add((wf, ds))
            bd = f"{O}/{wf}/2024/{ds}_{j}/base"
            fs = sorted(f for f in os.listdir(bd) if f.endswith(".parquet")) if os.path.isdir(bd) else []
            if fs and "jet_btagUParTAK4CvL" not in pq.read_schema(f"{bd}/{fs[0]}").names:
                bad.append(f"{d} {j} NO_UPART_COLUMNS")
open(f"{P}/final_check.txt", "w").write("\n".join(bad) + "\n")
print(f"{nok}/{ntot} complete, {len(bad)} problems (final_check.txt)")
for b in bad[:30]: print(" ", b)
