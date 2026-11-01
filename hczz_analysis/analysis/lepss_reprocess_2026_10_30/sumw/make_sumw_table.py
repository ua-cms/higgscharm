#!/usr/bin/env python3
"""
2026-10-23 copy of Analysis/sumw_fix_2026_10_02/make_sumw_table.py for the V4/V3 background production
(hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1); only change: that workflow added to WFS. Cache copied from 10-02.

Pre-selection, PRE-jet-veto sumw for exactly the chunks the datacard builder reads (2026-10-02).

Fixes, in one table:
  - jet veto before sumw: the sidecar sumw is computed after apply_jetvetomaps drops events,
    so every MC yield is ~5% high (memory hczz_jetveto_before_sumw);
  - sidecar coverage gap: scored files whose chunk has no sidecar are counted in the
    numerator but not in the denominator (hczz_sumw_coverage_gap);
  - JES/JER templates reusing the nominal production's sumw although their own
    production may cover a different set of chunks.

For every parquet chunk <guid>_%2FEvents%3B1_<start>-<stop>.parquet in
  nom : <scored>/<era>/<sample>/base/*.parquet                       (backgrounds)
  jec : <jecshifts>/<era>/<sample>/base/<syst>_<year><Dir>/*.parquet  (backgrounds)
  sig : <fschad>/<era>/HPlusCharm*/base/*.parquet and its shift dirs  (4FS signal)
the NanoAOD file with ROOT UUID == guid is opened and sum(genWeight[start:stop]) taken.
Candidate files come from the productions' own condor partitions.json.

Output (out dir): chunk_sumw.json  {guid|start|stop: sumw}
                  sumw_table.json  {kind: {era: {group: {sumw, n_chunks, n_missing}}}}
                  with kind = nom, sig, and jec/<syst>_<year><Dir> / sig_jec/<...>.
"""
import os, re, sys, json, glob, time, argparse, collections
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import uproot

sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts")
import eos_io as E
import create_datacards_ctag2d_100150 as dc

OUT = "/eos/user/s/snandaku/higgscharm/outputs"
p = argparse.ArgumentParser()
p.add_argument("--scored-dir", default=f"{OUT}/hplusc_mva_4class_ctag2d_scored_v8")
p.add_argument("--jecshifts-dir", default=f"{OUT}/hplusc_mva_4class_ctag2d_jecshifts_scored_v2")
p.add_argument("--fs-scored-dir", default=f"{OUT}/hplusc_mva_4class_ctag2d_fschad_scored_v1")
p.add_argument("--out", default="/eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02")
p.add_argument("--workers", type=int, default=16)
args = p.parse_args()

CONDOR = ["/eos/user/s/snandaku/Higgscharmnew/higgscharm/condor",
          "/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/condor"]
WFS = ["hplusc_mva_4class_ctag2d_lepss_bkg", "hplusc_mva_4class_ctag2d_lepss_fschad",  # 2026-10-30 lepss reprocessing
       "hplusc_mva_4class_ctag2d_jecshifts_bkg",  # 2026-10-23: V4/V3 background reprocessing (10-22)
       "hplusc_mva_4class_ctag2d", "hplusc_mva_4class_ctag2d_jecshifts", "hplusc_mva_4class_ctag2d_jecshifts_fschad",
       "hplusc_mva_4class"]
CHUNK = re.compile(r"^(.+?)_%2FEvents%3B1_(\d+)-(\d+)\.parquet$")
base_of = lambda s: re.sub(r"_\d+$", "", s)


def chunks_in(d):
    out = []
    for f in E.files(d, ".parquet"):
        m = CHUNK.match(os.path.basename(f))
        if not m:
            raise RuntimeError(f"unexpected parquet name: {f}")
        out.append((m.group(1), int(m.group(2)), int(m.group(3))))
    return out


# --- 1. which chunks does the builder read --------------------------------------------------
# reads[kind][(era, group)] = list of (guid, start, stop) WITH multiplicity (a chunk read twice
# is counted twice in the numerator, so it must be counted twice in the denominator too)
reads = collections.defaultdict(lambda: collections.defaultdict(list))
need = collections.defaultdict(set)          # (era, sample base) -> guids
t0 = time.time()
for era in dc.ERAS:
    yr = dc.era_year(era)
    for kind, root in (("nom", args.scored_dir), ("jec", args.jecshifts_dir), ("sig", args.fs_scored_dir)):
        for s in E.listdir(os.path.join(root, era)):
            proc = dc.get_process_from_path(s)
            if proc is None:
                continue
            if kind == "sig" and not s.startswith("HPlusCharm"):
                continue
            if kind != "sig" and proc == "Signal":
                continue
            if kind == "jec" and proc not in dc.JECSHIFTS_USABLE_PROCS:
                continue
            g = dc.norm_group(s)
            sd = os.path.join(root, era, s, "base")
            if not E.isdir(sd):
                continue
            dirs = []
            if kind in ("nom", "sig"):
                dirs.append((kind, sd))
            if kind in ("jec", "sig"):
                for syst in dc.JECSHIFTS_SYSTEMATICS:
                    for d in ("Up", "Down"):
                        cd = os.path.join(sd, f"{syst}_{yr}{d}")
                        if E.isdir(cd):
                            dirs.append((f"{'sig_jec' if kind == 'sig' else 'jec'}/{syst}_{yr}{d}", cd))
            for k, dd in dirs:
                ch = chunks_in(dd)
                reads[k][(era, g)].extend(ch)
                need[(era, base_of(s))].update(c[0] for c in ch)
    print(f"listed {era} ({time.time() - t0:.0f}s)", flush=True)

ranges = collections.defaultdict(set)        # guid -> {(start, stop)}
for k in reads:
    for lst in reads[k].values():
        for gid, a, b in lst:
            ranges[gid].add((a, b))
print(f"{len(ranges)} distinct guids, {sum(len(v) for v in ranges.values())} distinct chunks", flush=True)

# --- 2. candidate NanoAOD files per (era, sample base) --------------------------------------
cand = collections.defaultdict(set)
for r in CONDOR:
    for wf in WFS:
        for pj in glob.glob(f"{r}/{wf}/*/*/partitions.json"):
            era = pj.split("/")[-3]
            for part in json.load(open(pj)).values():
                for k, v in part.items():
                    cand[(era, base_of(k))].update(v)

# 2026-10-05: the Feb-2026 private CRAB outputs /store/user/snandaku/hplus{c,b}_htozz/ (no "1") are
# purged (rc 54 at every endpoint); they are still listed in the old hplusc_mva_4class partitions.json.
# The productions read the hplus{c,b}_htozz1 copies (DAS prod/phys03, T2_BE_IIHE). Dropping the purged
# LFNs is safe: any chunk whose file is then not found is still reported as "chunk reads without sumw".
PURGED = re.compile(r"/store/user/snandaku/hplus[cb]_htozz/")
n_drop = 0
for key in cand:
    keep = {u for u in cand[key] if not PURGED.search(u)}
    n_drop += len(cand[key]) - len(keep)
    cand[key] = keep
print(f"dropped {n_drop} candidate URLs under the purged hplus{{c,b}}_htozz/ dirs", flush=True)

# --- 3. open files, match UUID, sum genWeight per chunk ------------------------------------
chunk_sumw, file_info, errors = {}, {}, []


def lfn_of(url):
    """/store/... for grid files (any redirector), else the URL itself (private eosuser files)."""
    m = re.match(r"^root://[^/]+/+(?:eos/cms/+)?(store/.*)$", url)
    return "/" + m.group(1) if m else url


def scan(urls):
    """urls: alternative locations of one file; the global redirector is tried first."""
    err = None
    for attempt in range(2):
      for path in urls:
        try:
            with uproot.open(path, timeout=300) as f:
                gid = str(f.file.uuid)
                if gid not in ranges:
                    return path, gid, None, None
                gw = f["Events"]["genWeight"].array(library="np").astype(np.float64)
                runs = float(np.sum(f["Runs"]["genEventSumw"].array(library="np")))
                return path, gid, {f"{a}-{b}": float(gw[a:b].sum()) for a, b in ranges[gid]}, (len(gw), float(gw.sum()), runs)
        except Exception as e:
            err = e
      time.sleep(5)
    return urls[0], None, None, repr(err)


# per-file checkpoint (2026-10-04): the 10-02 pass died after ~3200/5064 files (xrootd socket
# timeouts + EOS quota full) and kept nothing. Every success is appended to scan_cache.jsonl.
CACHE = f"{args.out}/scan_cache.jsonl"
cache = {}
if os.path.exists(CACHE):
    for line in open(CACHE):
        try:
            r = json.loads(line)
        except ValueError:
            continue                         # torn last line of a killed pass
        cache[r["key"]] = r


def cached(key):
    r = cache.get(key)
    if r is None:
        return None
    if r["gid"] not in ranges:
        return r["path"], r["gid"], None, None
    want = {f"{a}-{b}" for a, b in ranges[r["gid"]]}
    if not want <= set(r["sums"]):
        return None
    return r["path"], r["gid"], {k: r["sums"][k] for k in want}, tuple(r["info"])


by_lfn = collections.defaultdict(set)
for key, fs in cand.items():
    if key in need:
        for f in fs:
            by_lfn[lfn_of(f)].add(f)
todo = []
for l, us in sorted(by_lfn.items()):
    first = [f"root://cms-xrd-global.cern.ch/{l}"] if l.startswith("/store/") else []
    todo.append(first + sorted(u for u in us if u not in first))
print(f"opening {len(todo)} distinct files (by LFN) with {args.workers} workers", flush=True)
done, results, rescan = 0, [], []
for f in todo:
    c = cached(f[0])
    (results.append(c) if c else rescan.append(f))
print(f"{len(results)} files from {CACHE}, {len(rescan)} to open", flush=True)
with open(CACHE, "a") as cf, ThreadPoolExecutor(args.workers) as ex:
    futs = {ex.submit(scan, f): f[0] for f in rescan}
    for fut in as_completed(futs):
        r = fut.result()
        if r[1] is not None:
            cf.write(json.dumps({"key": futs[fut], "path": r[0], "gid": r[1], "sums": r[2] or {},
                                 "info": list(r[3]) if r[2] is not None else None}) + "\n")
            cf.flush()
        results.append(r)
        if r[1] is None and sum(1 for x in results if x[1] is None) <= 10:
            print(f"  ERROR {r[0]}: {r[3]}", flush=True)
        done += 1
        if done % 200 == 0:
            print(f"  {done}/{len(rescan)} opened, {sum(1 for x in results if x[1] is None)} errors ({time.time() - t0:.0f}s)", flush=True)
for path, gid, sums, info in results:
        if gid is None:
            errors.append((path, info))
        elif sums is not None:
            for r, v in sums.items():
                chunk_sumw[f"{gid}|{r}"] = v
            file_info[gid] = {"path": path, "n": info[0], "sumw_events": info[1], "sumw_runs": info[2]}

# --- 4. aggregate per kind / era / group -----------------------------------------------------
table, missing = {}, []
for k, per in reads.items():
    for (era, g), lst in per.items():
        sw, nmiss = 0.0, 0
        for gid, a, b in lst:
            v = chunk_sumw.get(f"{gid}|{a}-{b}")
            if v is None:
                nmiss += 1
                missing.append((k, era, g, gid, a, b))
            else:
                sw += v
        table.setdefault(k, {}).setdefault(era, {})[g] = {"sumw": sw, "n_chunks": len(lst), "n_missing": nmiss}

os.makedirs(args.out, exist_ok=True)
json.dump(chunk_sumw, open(f"{args.out}/chunk_sumw.json", "w"))
json.dump(file_info, open(f"{args.out}/file_info.json", "w"), indent=0)
json.dump(table, open(f"{args.out}/sumw_table.json", "w"), indent=1)
json.dump({"errors": errors, "missing": missing}, open(f"{args.out}/problems.json", "w"), indent=0)
full = sum(1 for i in file_info.values() if abs(i["sumw_events"] - i["sumw_runs"]) > 1e-6 * max(1.0, abs(i["sumw_runs"])))
print(f"\nmatched {len(file_info)} files; {len(errors)} open errors; {len(missing)} chunk reads without sumw; "
      f"{full} files where sum(genWeight) != Runs.genEventSumw")
print(f"DONE {time.time() - t0:.0f}s")
