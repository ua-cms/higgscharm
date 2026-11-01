import sys, json, concurrent.futures as cf, subprocess
SITES = ["root://dcache-cms-xrootd.desy.de:1094/", "root://cmsdcache-kit-disk.gridka.de:1094/",
         "root://eoscms.cern.ch//eos/cms", "root://maite.iihe.ac.be:1094/",
         "root://cmsxrootd.fnal.gov/", "root://xrootd-cms.infn.it/"]
CHK = r'''
import sys, uproot
f = uproot.open(sys.argv[1], timeout=40)
t = f["Events"]; n = t.num_entries
t["run"].array(entry_stop=min(n, 1000))
print(n)
'''
def probe(url):
    lfn = "/store" + url.split("/store", 1)[1]
    for s in SITES:
        u = s + lfn
        try:
            r = subprocess.run([sys.executable, "-c", CHK, u], capture_output=True, text=True, timeout=90)
            if r.returncode == 0:
                return url, u
        except subprocess.TimeoutExpired:
            pass
    return url, None
files = [l.strip() for l in open(sys.argv[1]) if l.strip()]
res = {}
with cf.ThreadPoolExecutor(32) as ex:
    for i, (k, v) in enumerate(ex.map(probe, files)):
        res[k] = v
        if i % 100 == 0: print(i, flush=True)
json.dump(res, open(sys.argv[2], "w"), indent=1)
print("done", sum(v is None for v in res.values()), "unresolved")
