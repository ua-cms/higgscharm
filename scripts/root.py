from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import awkward as ak

events = NanoEventsFactory.from_root(
    "/eos/user/p/pkatris/signal.root",
    schemaclass=NanoAODSchema
).events()

gen = events.GenPart
n_events = len(gen.pdgId)  # only loop over available events

for i in range(1000):
    pdg        = ak.to_list(gen.pdgId[i])
    mother_idx = ak.to_list(gen.genPartIdxMother[i])  # raw from ROOT
    is_hard    = ak.to_list((gen.statusFlags[i] & (1 << 7))  != 0)
    is_last    = ak.to_list((gen.statusFlags[i] & (1 << 13)) != 0)
    eta        = ak.to_list(gen.eta[i])
    phi        = ak.to_list(gen.phi[i])

    print(f"\n=== Event {i} — Total GenPart: {len(pdg)} ===")
    for j in range(len(pdg)):
        print(f"    idx={j:3d}  pdg={pdg[j]:5d}  isHard={is_hard[j]}  isLast={is_last[j]}"
              f"  mother={mother_idx[j]:3d}  eta={eta[j]:7.3f}  phi={phi[j]:7.3f}")