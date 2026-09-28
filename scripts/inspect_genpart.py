import numpy as np
import awkward as ak
import pyarrow.dataset as ds

BASE     = "/eos/user/p/pkatris/higgscharm/outputs/hplusc/2022postEE/"
sig_file = BASE + "SomeSMSignal/base"

print("Loading...")
table = ds.dataset(sig_file, format="parquet").to_table()
arr   = ak.from_arrow(table)

print("\n=== Events with NO hard-process charm quark (|eta|<10) ===")
no_charm_events = []

for i in range(len(arr)):
    raw_pdg = arr["genpart_pdgId"][i]
    if raw_pdg is None or len(raw_pdg) == 0:
        no_charm_events.append(i)
        print(f"\nevt {i}: empty GenPart array")
        continue

    pdg     = np.array(raw_pdg, dtype=int)
    is_hard = np.array(arr["genpart_isHardProcess"][i], dtype=bool)
    eta     = np.array(arr["genpart_eta"][i], dtype=float)
    pt      = np.array(arr["genpart_pt"][i], dtype=float)

    hard_charm_idx = np.where(
        (np.abs(pdg) == 4) & is_hard & (np.abs(eta) < 10)
    )[0]

    if len(hard_charm_idx) == 0:
        no_charm_events.append(i)
        phi        = np.array(arr["genpart_phi"][i], dtype=float)
        mother_idx = np.array(arr["genpart_genPartIdxMother"][i], dtype=int)
        is_last    = np.array(arr["genpart_isLastCopy"][i], dtype=bool)

        all_hard_charm_idx = np.where((np.abs(pdg) == 4) & is_hard)[0]

        print(f"\nevt {i}: NO hard charm passing cuts | Total GenPart: {len(pdg)}")

        if len(all_hard_charm_idx) > 0:
            print(f"  [charm quarks present but FAILING cuts — ALL GenPart entries:]")
        else:
            print(f"  [no charm quarks at all — ALL GenPart entries:]")

        for j in range(len(pdg)):
            print(f"    idx={j:3d}  pdg={pdg[j]:5d}  pt={pt[j]:7.2f}  eta={eta[j]:7.3f}"
                  f"  phi={phi[j]:7.3f}  isHard={is_hard[j]}  isLast={is_last[j]}  mother={mother_idx[j]:3d}")

print(f"\n=== Summary ===")
print(f"  Total events:                          {len(arr)}")
print(f"  Events with no hard charm (|eta|<10):  {len(no_charm_events)}")
print(f"  Event indices (first 50):              {no_charm_events[:50]}")