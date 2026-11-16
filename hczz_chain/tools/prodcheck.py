#!/usr/bin/env python3
"""Production completeness: every job of every dataset wrote its <dataset key>.coffea (single-job keys without _N write _1).
Expected jobs come from the partitions.json of each condor submit dir (HCZZ_CONDOR_DIRS); output-based, so reruns of failed jobs
from any cluster count. Same rule as the 2026-11 chain gate (condor_hczz_2026_11_09_final/prodgate.py).
  prodcheck.py --workflows <wf...> --eras 2223|2024|<era...> [--wait]   (--wait: poll every 5 min, give up after 48 h)"""
import argparse, glob, json, os, re, sys, time
ap = argparse.ArgumentParser()
ap.add_argument("--workflows", nargs="+", required=True)
ap.add_argument("--eras", nargs="+", required=True)
ap.add_argument("--wait", action="store_true")
a = ap.parse_args()
alias = {"2223": ["2022preEE", "2022postEE", "2023preBPix", "2023postBPix"]}
eras = [e for x in a.eras for e in alias.get(x, [x])]
out = os.environ.get("HCZZ_OUTPUTS", "/eos/user/s/snandaku/higgscharm/outputs")
roots = [r for r in os.environ.get("HCZZ_CONDOR_DIRS", "").split(":") if r]
expect = []
for w in a.workflows:
    for e in eras:
        found = [r for r in roots if glob.glob(f"{r}/{w}/{e}/*/partitions.json")]
        if not found:
            continue  # workflow not run for this era (e.g. the 2022/23 signal workflow for 2024)
        for pj in sorted(glob.glob(f"{found[0]}/{w}/{e}/*/partitions.json")):
            d = pj.split("/")[-2]
            if ".bak" in d:
                continue  # backup submit dirs of replaced samples (e.g. bbH_Hto2Zto4L.bak_20261113_v2)
            for v in json.load(open(pj)).values():
                for k in v:
                    k = k if re.search(r"_[0-9]+$", k) else k + "_1"
                    expect.append(f"{out}/{w}/{e}/{d}/{k}.coffea")
print(len(expect), "expected .coffea files", flush=True)
if not expect:
    sys.exit("no jobs found: check HCZZ_CONDOR_DIRS / workflows / eras")
for it in range(576 if a.wait else 1):
    miss = [f for f in expect if not os.path.exists(f)]
    print(time.strftime("%H:%M"), f"missing {len(miss)}/{len(expect)}", flush=True)
    for f in miss[:5]:
        print("  ", f)
    if not miss:
        print("COMPLETE"); sys.exit(0)
    if a.wait:
        time.sleep(300)
sys.exit(1)
