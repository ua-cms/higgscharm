#!/usr/bin/env python3
"""
Inspect the .coffea metadata files and yaml configs to understand
the sumw/lumi/xsec structure needed for lumi_weight computation.
"""
import glob
from pathlib import Path
from coffea.util import load

BASE      = "/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/"
SAMPLE    = "SomeSMSignal"
coffea_dir = BASE + SAMPLE + "/"

# 1. Find all .coffea files for this sample
coffea_files = sorted(glob.glob(coffea_dir + "*.coffea"))
print(f"Found {len(coffea_files)} .coffea files:")
for f in coffea_files:
    print(f"  {f}")

# 2. Load first one and inspect top-level keys
print("\n=== First .coffea file structure ===")
acc = load(coffea_files[0])
print(f"type: {type(acc)}")

# Handle both dict-like and accumulator-like
if hasattr(acc, "keys"):
    print(f"Top-level keys: {list(acc.keys())}")
    for k in list(acc.keys())[:10]:
        v = acc[k]
        print(f"  [{k}] type={type(v).__name__}", end="")
        if hasattr(v, "keys"):
            print(f"  sub-keys={list(v.keys())[:8]}")
        elif hasattr(v, "shape"):
            print(f"  shape={v.shape}")
        elif hasattr(v, "__len__"):
            print(f"  len={len(v)}")
        else:
            print(f"  value={v}")
else:
    print(acc)

# 3. Try to find sumw specifically
print("\n=== Searching for 'sumw' ===")
def search_sumw(obj, path=""):
    if hasattr(obj, "keys"):
        for k in obj.keys():
            search_sumw(obj[k], path + f"[{k}]")
    elif "sumw" in str(path).lower():
        print(f"  {path} = {obj}")

search_sumw(acc)

# 4. Accumulate sumw across ALL coffea files
print("\n=== sumw across all .coffea files ===")
total_sumw = 0.0
for cf in coffea_files:
    a = load(cf)
    # Try common locations
    for key_path in [["metadata", "sumw"], ["sumw"], ["cutflow", "sumw"]]:
        try:
            val = a
            for k in key_path:
                val = val[k]
            print(f"  {cf.split('/')[-1]}: sumw={val} (path={key_path})")
            total_sumw += float(val)
            break
        except (KeyError, TypeError):
            continue
print(f"  TOTAL sumw = {total_sumw}")

# 5. Inspect yaml configs
print("\n=== luminosity.yaml ===")
import yaml
lumi_path = Path.cwd() / "analysis" / "postprocess" / "luminosity.yaml"
if lumi_path.exists():
    with open(lumi_path) as f:
        lumi = yaml.safe_load(f)
    print(lumi)
else:
    print(f"  Not found at {lumi_path}")
    # Try a few other locations
    for p in Path.cwd().rglob("luminosity.yaml"):
        print(f"  Found at: {p}")

print("\n=== dataset config (looking for xsec) ===")
for p in Path.cwd().rglob("*.yaml"):
    if "dataset" in p.name.lower() or "samples" in p.name.lower() or "config" in p.name.lower():
        print(f"  {p}")
        try:
            with open(p) as f:
                cfg = yaml.safe_load(f)
            if isinstance(cfg, dict):
                # Look for SomeSMSignal or xsec keys
                for k, v in list(cfg.items())[:5]:
                    print(f"    {k}: {str(v)[:80]}")
        except Exception as e:
            print(f"    Error: {e}")