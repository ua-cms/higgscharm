#!/usr/bin/env python3
"""Read every histogram of a ROOT file with ROOT (catches corrupt compressed records, seen on EOS 10-30)."""
import sys
import ROOT
ROOT.gErrorIgnoreLevel = ROOT.kFatal
bad = 0
for fn in sys.argv[1:]:
    f = ROOT.TFile.Open(fn)
    keys = [k.GetName() for k in f.GetListOfKeys()]
    nb = sum(1 for k in keys if not f.Get(k))
    bad += nb
    print(f"{fn}: {len(keys)} keys, {nb} unreadable")
sys.exit(1 if bad else 0)
