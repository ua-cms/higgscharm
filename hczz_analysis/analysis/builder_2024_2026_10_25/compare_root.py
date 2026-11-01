# 2026-10-25: compare every histogram of histograms_mva_with_zx.root between two card dirs (regression test)
import sys, uproot, numpy as np
a, b = (uproot.open(f"{d}/histograms_mva_with_zx.root") for d in sys.argv[1:3])
ka = {k.split(";")[0] for k in a.keys() if a[k].classname.startswith("TH1")}
kb = {k.split(";")[0] for k in b.keys() if b[k].classname.startswith("TH1")}
print(f"histograms: ref {len(ka)}, new {len(kb)}, only ref {sorted(ka - kb)[:10]}, only new {sorted(kb - ka)[:10]}")
worst = (0.0, None)
for k in sorted(ka & kb):
    va, vb = a[k].values(), b[k].values()
    ea, eb = a[k].errors(), b[k].errors()
    d = max(np.max(np.abs(va - vb) / np.maximum(np.abs(va), 1e-12), initial=0), np.max(np.abs(ea - eb) / np.maximum(np.abs(ea), 1e-12), initial=0))
    if d > worst[0]:
        worst = (d, k)
print(f"max relative difference (values and errors): {worst[0]:.3e} in {worst[1]}")
print("ROOT IDENTICAL" if worst[0] < 1e-12 and ka == kb else "ROOT DIFFERS")
