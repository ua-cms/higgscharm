import awkward as ak
from coffea.nanoevents.methods import candidate


def delta_r_higher(first, second, threshold=0.4):
    # select objects from 'first' which are at least 'threshold' away from all objects in 'second'.
    mval = first.metric_table(second)
    return ak.all(mval > threshold, axis=-1)

def delta_r_lower(first, second, threshold=0.4):
    # select objects from 'first' which are at least 'threshold' within from all objects in 'second'.
    mval = first.metric_table(second)
    return ak.all(mval <= threshold, axis=-1)

def select_dileptons(objects, key):
    leptons = ak.zip(
        {
            "pt": objects[key].pt,
            "eta": objects[key].eta,
            "phi": objects[key].phi,
            "mass": objects[key].mass,
            "charge": objects[key].charge,
        },
        with_name="PtEtaPhiMCandidate",
        behavior=candidate.behavior,
    )
    # make sure they are sorted by transverse momentum
    leptons = leptons[ak.argsort(leptons.pt, axis=1)]
    # create pair combinations with all muons
    dileptons = ak.combinations(leptons, 2, fields=["l1", "l2"])
    # add dimuon 4-momentum field
    dileptons["z"] = dileptons.l1 + dileptons.l2
    dileptons["pt"] = dileptons.z.pt
    return dileptons


def select_zzto4l_zz_candidates(ll_pairs):
    zz_pairs = ak.combinations(ll_pairs, 2, fields=["z1", "z2"])
    zz_pairs = ak.zip(
        {
            "z1": ak.zip(
                {
                    "l1": zz_pairs.z1.l1,
                    "l2": zz_pairs.z1.l2,
                    "p4": zz_pairs.z1.l1 + zz_pairs.z1.l2,
                }
            ),
            "z2": ak.zip(
                {
                    "l1": zz_pairs.z2.l1,
                    "l2": zz_pairs.z2.l2,
                    "p4": zz_pairs.z2.l1 + zz_pairs.z2.l2,
                }
            ),
        }
    )
    # ghost removal: ∆R(η, φ) > 0.02 between each of the four leptons
    ghost_removal_mask = (
        (zz_pairs.z1.l1.delta_r(zz_pairs.z1.l2) > 0.02)
        & (zz_pairs.z1.l1.delta_r(zz_pairs.z2.l1) > 0.02)
        & (zz_pairs.z1.l1.delta_r(zz_pairs.z2.l2) > 0.02)
        & (zz_pairs.z1.l2.delta_r(zz_pairs.z2.l1) > 0.02)
        & (zz_pairs.z1.l2.delta_r(zz_pairs.z2.l2) > 0.02)
        & (zz_pairs.z2.l1.delta_r(zz_pairs.z2.l2) > 0.02)
    )
    # Lepton pT: two of the four selected leptons should pass pT,i > 20 GeV and pT,j > 10
    lepton_pt_mask = (
        (
            ak.any(zz_pairs.z1.l1.pt > 20, axis=-1)
            & ak.any(zz_pairs.z1.l2.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z1.l1.pt > 20, axis=-1)
            & ak.any(zz_pairs.z2.l1.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z1.l1.pt > 20, axis=-1)
            & ak.any(zz_pairs.z2.l2.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z1.l2.pt > 20, axis=-1)
            & ak.any(zz_pairs.z1.l1.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z1.l2.pt > 20, axis=-1)
            & ak.any(zz_pairs.z2.l1.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z1.l2.pt > 20, axis=-1)
            & ak.any(zz_pairs.z2.l2.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z2.l1.pt > 20, axis=-1)
            & ak.any(zz_pairs.z1.l1.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z2.l1.pt > 20, axis=-1)
            & ak.any(zz_pairs.z1.l2.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z2.l1.pt > 20, axis=-1)
            & ak.any(zz_pairs.z2.l2.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z2.l2.pt > 20, axis=-1)
            & ak.any(zz_pairs.z1.l1.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z2.l2.pt > 20, axis=-1)
            & ak.any(zz_pairs.z1.l2.pt > 10, axis=-1)
        )
        | (
            ak.any(zz_pairs.z2.l2.pt > 20, axis=-1)
            & ak.any(zz_pairs.z2.l1.pt > 10, axis=-1)
        )
    )
    # QCD suppression: all four opposite-sign pairs that can be built with the four leptons (regardless of lepton flavor) must satisfy m > 4 GeV
    qcd_condition_mask = (
        (zz_pairs.z1.p4.mass > 4)
        & (zz_pairs.z2.p4.mass > 4)
        & (
            (zz_pairs.z1.l1.charge + zz_pairs.z2.l1.charge != 0)
            | (
                (zz_pairs.z1.l1.charge + zz_pairs.z2.l1.charge == 0)
                & ((zz_pairs.z1.l1 + zz_pairs.z2.l1).mass > 4)
            )
        )
        & (
            (zz_pairs.z1.l1.charge + zz_pairs.z2.l2.charge != 0)
            | (
                (zz_pairs.z1.l1.charge + zz_pairs.z2.l2.charge == 0)
                & ((zz_pairs.z1.l1 + zz_pairs.z2.l2).mass > 4)
            )
        )
        & (
            (zz_pairs.z1.l2.charge + zz_pairs.z2.l1.charge != 0)
            | (
                (zz_pairs.z1.l2.charge + zz_pairs.z2.l1.charge == 0)
                & ((zz_pairs.z1.l2 + zz_pairs.z2.l1).mass > 4)
            )
        )
        & (
            (zz_pairs.z1.l2.charge + zz_pairs.z2.l2.charge != 0)
            | (
                (zz_pairs.z1.l2.charge + zz_pairs.z2.l2.charge == 0)
                & ((zz_pairs.z1.l2 + zz_pairs.z2.l2).mass > 4)
            )
        )
    )
    # Z1 mass > 40 GeV
    mass_mask = zz_pairs.z1.p4.mass > 40

    mask = ghost_removal_mask & lepton_pt_mask & qcd_condition_mask & mass_mask
    return zz_pairs[ak.fill_none(mask, False)]
