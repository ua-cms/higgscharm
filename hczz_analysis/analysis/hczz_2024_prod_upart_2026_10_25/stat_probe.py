# 2026-10-23 night: fast replacement for probe.py (uproot read would take ~7 h for 8457 files): xrdfs stat per site.
# DAS: 2024 NanoAOD v15 files are at T2_DE_DESY; fall back to KIT, FNAL, CERN.
import sys, json, subprocess, concurrent.futures as cf
from collections import Counter
SITES = [("dcache-cms-xrootd.desy.de:1094", "root://dcache-cms-xrootd.desy.de:1094/", ""),
         ("cmsdcache-kit-disk.gridka.de:1094", "root://cmsdcache-kit-disk.gridka.de:1094/", ""),
         ("cmsxrootd.fnal.gov", "root://cmsxrootd.fnal.gov/", ""),
         ("eoscms.cern.ch", "root://eoscms.cern.ch//eos/cms", "/eos/cms")]
def probe(url):
    lfn = "/store" + url.split("/store", 1)[1]
    for host, pre, ppre in SITES:
        try:
            r = subprocess.run(["xrdfs", host, "stat", ppre + lfn], capture_output=True, text=True, timeout=30)
            if r.returncode == 0 and "IsReadable" in r.stdout:
                return url, pre + lfn
        except subprocess.TimeoutExpired:
            pass
    return url, None
files = [l.strip() for l in open(sys.argv[1]) if l.strip()]
res = {}
with cf.ThreadPoolExecutor(64) as ex:
    for i, (k, v) in enumerate(ex.map(probe, files)):
        res[k] = v
        if i % 500 == 0: print(i, flush=True)
json.dump(res, open(sys.argv[2], "w"), indent=1)
print(Counter((v or "NONE").split("/store")[0] for v in res.values()))
print("done", sum(v is None for v in res.values()), "unresolved")
