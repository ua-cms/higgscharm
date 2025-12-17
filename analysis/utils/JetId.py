import awkward as ak

def compute_jetid_bits(events, puid_thr=0.60):
    """
    Returns an int8 per-jet bitmask:
      bit2 (2) = tight
      bit3 (4) = tightLepVeto

    If NanoAOD provides Jet.jetId, reuse it.
    Else (v15+), build an on-the-fly proxy from fractions & multiplicities,
    and apply PU-ID discriminator for pt<50 if available (Jet_puIdDisc).
    """
    jets = events.Jet
    fields = set(jets.fields)

    # 0) If the stored bits exist, just reuse them
    if "jetId" in fields:
        return ak.values_astype(jets.jetId, "int8")

    # 1) Base quality (very standard cleaning)
    base = ak.full_like(jets.pt, True, dtype=bool)
    if "neHEF" in fields:          base = base & (jets.neHEF < 0.90)
    if "neEmEF" in fields:         base = base & (jets.neEmEF < 0.90)
    if "nConstituents" in fields:  base = base & (jets.nConstituents > 1)

    # 2) Central region refinements (|eta|<2.4) if info exists
    central = (abs(jets.eta) < 2.4)
    central_req = ak.full_like(central, True, dtype=bool)
    if "chHEF" in fields:           central_req = central_req & (jets.chHEF > 0.0)
    if "chMultiplicity" in fields:  central_req = central_req &(jets.chMultiplicity > 0)
    tight = base * ak.where(central, central_req, ak.full_like(central, True, dtype=bool))

    # 3) Tight-lepton-veto refinements if info exists
    lep_req = ak.full_like(jets.pt, True, dtype=bool)
    if "chEmEF" in fields:  lep_req = lep_req & (jets.chEmEF < 0.80)
    if "muEF"   in fields:  lep_req = lep_req & (jets.muEF   < 0.80)
    tightlepveto = tight * ak.where(central, lep_req, ak.full_like(central, True, dtype=bool))

    # 4) PU-ID discriminator for pt<50 if available
    if "puIdDisc" in fields:
        lowpt   = jets.pt < 50.0
        pass_pu = jets.puIdDisc > puid_thr
        pu_mask = ak.where(lowpt, pass_pu, ak.full_like(lowpt, True, dtype=bool))
        tight        = tight & pu_mask
        tightlepveto = tightlepveto & pu_mask

    # Pack into bits: 2 (tight) | 4 (tightLepVeto)
    return (ak.values_astype(tight, "int8") * 2) | (ak.values_astype(tightlepveto, "int8") * 4)
