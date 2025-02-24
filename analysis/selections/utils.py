import numba
import numpy as np
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


def transverse_mass(lepton, met):
    return np.sqrt(
        2.0
        * lepton.pt
        * met.pt
        * (ak.ones_like(met.pt) - np.cos(lepton.delta_phi(met)))
    )


@numba.njit
def unique_numba(arr):
    """Returns unique elements, inverse indices, and counts (Numba-compatible)"""
    sorted_idx = np.argsort(arr)
    sorted_arr = arr[sorted_idx]
    unique_values = []
    counts = np.zeros(arr.shape[0], dtype=np.int32)
    inverse_indices = np.zeros(arr.shape[0], dtype=np.int32)

    count = 0
    for i in range(arr.shape[0]):
        if i == 0 or sorted_arr[i] != sorted_arr[i - 1]:
            unique_values.append(sorted_arr[i])
            count += 1
        counts[count - 1] += 1
        inverse_indices[sorted_idx[i]] = count - 1

    return np.array(unique_values), inverse_indices, counts[:count]


@numba.njit(parallel=True)
def remove_duplicates(array, dr_array):
    """Removes duplicates by replacing the duplicate with highest dR value with -1"""
    if len(array) == 0:
        return array

    unique_elements, inverse_indices, counts = unique_numba(array)

    mask = counts[inverse_indices] > 1
    repeated_indices = np.flatnonzero(mask)
    repeated_values = array[repeated_indices]

    max_dr = -1.0
    max_id = -1

    for i in range(len(repeated_indices)):
        idx = repeated_indices[i]
        closest = int(repeated_values[i])

        idr = dr_array[idx, closest]
        if idr > max_dr:
            max_dr = idr
            max_id = idx

    if max_id != -1:
        array[max_id] = -1

    return array


# 'inspired' by:
# https://github.com/piperov/coffea-hmumu-demonstrator/blob/master/python/corrections.py#L217
# https://github.com/CJLST/ZZAnalysis/blob/Run3/NanoAnalysis/python/lepFiller.py
@numba.njit(parallel=True)
def fsr_matching(
    fsr_offsets,
    muon_offsets,
    electron_offsets,
    muon_pt,
    muon_eta,
    muon_phi,
    muon_iso,
    electron_eta,
    electron_phi,
    fsr_pt,
    fsr_eta,
    fsr_phi,
):
    """
    evaluates Final State Radiation (FSR) in an event-by-event manner.

    This function associates FSR photons to the closest muon or electron based on distance
    and updates their isolation variables accordingly.

    Parameters:
    -----------
    fsr_offsets : numpy array
        Index offsets defining FSR photons per event.
    muon_offsets : numpy array
        Index offsets defining muons per event.
    electron_offsets : numpy array
        Index offsets defining electrons per event.
    muon_pt, muon_eta, muon_phi, muon_iso : numpy array
        Muon properties.
    electron_eta, electron_phi, electron_iso : numpy array
        Electron properties.
    fsr_pt, fsr_eta, fsr_phi : numpy array
        FSR photon properties.
    good_muons, good_electrons : numpy array
        Boolean arrays indicating whether a muon/electron passes selection.
    """
    # arrays to store indices of FSR photons associated with muons/electrons
    fsrPhoton_myMuonIdx = np.full_like(fsr_pt, -1)
    fsrPhoton_myElectronIdx = np.full_like(fsr_pt, -1)

    # arrays to store the index of the FSR photon assigned to each muon/electron
    muFsrPhotonIdx = np.full_like(muon_pt, -1)
    eleFsrPhotonIdx = np.full_like(electron_eta, -1)

    # loop over all events in parallel
    for iev in numba.prange(len(muon_offsets) - 1):
        # get the range of FSR photons, muons, and electrons in this event
        fsr_first, fsr_last = fsr_offsets[iev], fsr_offsets[iev + 1]
        mu_first, mu_last = muon_offsets[iev], muon_offsets[iev + 1]
        ele_first, ele_last = electron_offsets[iev], electron_offsets[iev + 1]

        # initialize arrays for tracking closest FSR matches in this event
        eventfsrPhoton_myMuonIdx = np.full(fsr_last - fsr_first, -1, dtype=np.float64)
        eventmuFsrPhotonIdx = np.full(mu_last - mu_first, -1, dtype=np.float64)
        eventfsrPhoton_myElectronIdx = np.full(
            fsr_last - fsr_first, -1, dtype=np.float64
        )
        eventeleFsrPhotonIdx = np.full(ele_last - ele_first, -1, dtype=np.float64)
        # initialize dR values for muons and electrons in this event
        muPhotondR = np.full(
            (fsr_last - fsr_first, mu_last - mu_first), np.inf, dtype=np.float64
        )
        elePhotondR = np.full(
            (fsr_last - fsr_first, ele_last - ele_first), np.inf, dtype=np.float64
        )
        # initialize dR/Et² values for muons and electrons in this event
        muPhotondREt2 = np.full(
            (fsr_last - fsr_first, mu_last - mu_first), 1e3, dtype=np.float64
        )
        elePhotondREt2 = np.full(
            (fsr_last - fsr_first, ele_last - ele_first), 1e3, dtype=np.float64
        )
        # loop over all FSR photons in this event
        for ifsr in range(fsr_first, fsr_last):
            dRmin = 0.5
            closestMu = -1
            closestEle = -1
            rel_fsr_index = ifsr - fsr_first
            # loop over all electrons/muons in this event to find the closest to the FSR
            for ie in range(ele_first, ele_last):
                rel_ele_index = ie - ele_first
                deta = electron_eta[ie] - fsr_eta[ifsr]
                dphi = (
                    np.mod(electron_phi[ie] - fsr_phi[ifsr] + np.pi, 2 * np.pi) - np.pi
                )
                dR = np.sqrt(deta**2 + dphi**2)
                dREt2 = dR / fsr_pt[ifsr] ** 2
                elePhotondR[rel_fsr_index, rel_ele_index] = dR
                if (dR < dRmin) and (dR > 0.001) and (dREt2 < 0.012):
                    dRmin = dR
                    closestEle = rel_ele_index
                    closestEleIdx = ie

            for imu in range(mu_first, mu_last):
                rel_mu_index = imu - mu_first
                deta = muon_eta[imu] - fsr_eta[ifsr]
                dphi = np.mod(muon_phi[imu] - fsr_phi[ifsr] + np.pi, 2 * np.pi) - np.pi
                dR = np.sqrt(deta**2 + dphi**2)
                dREt2 = dR / fsr_pt[ifsr] ** 2
                muPhotondR[rel_fsr_index, rel_mu_index] = dR
                if (dR < dRmin) and (dR > 0.001) and (dREt2 < 0.012):
                    dRmin = dR
                    closestMu = rel_mu_index
                    closestMuIdx = imu
                    closestEle = -1

            # assign indices to matched FSR/leptons in the event
            if (closestMu >= 0) or (closestEle >= 0):
                dREt2 = dRmin / fsr_pt[ifsr] ** 2
                dR = dRmin
                if closestMu >= 0:
                    eventfsrPhoton_myMuonIdx[rel_fsr_index] = closestMu
                    if dREt2 < muPhotondREt2[rel_fsr_index, closestMu]:
                        muPhotondREt2[rel_fsr_index, closestMu] = dREt2
                        eventmuFsrPhotonIdx[closestMu] = rel_fsr_index
                if closestEle >= 0:
                    eventfsrPhoton_myElectronIdx[rel_fsr_index] = closestEle
                    if dREt2 < elePhotondREt2[rel_fsr_index, closestEle]:
                        elePhotondREt2[rel_fsr_index, closestEle] = dREt2
                        eventeleFsrPhotonIdx[closestEle] = rel_fsr_index

        # copy assigned event indices to index arrays
        fsrPhoton_myMuonIdx[fsr_first:fsr_last] = remove_duplicates(
            eventfsrPhoton_myMuonIdx, muPhotondR
        )
        muFsrPhotonIdx[mu_first:mu_last] = eventmuFsrPhotonIdx
        fsrPhoton_myElectronIdx[fsr_first:fsr_last] = remove_duplicates(
            eventfsrPhoton_myElectronIdx, elePhotondR
        )
        eleFsrPhotonIdx[ele_first:ele_last] = eventeleFsrPhotonIdx

        # recompute muon isolation by removing all selected FSR
        has_fsr = (fsrPhoton_myMuonIdx > -1) | (fsrPhoton_myElectronIdx > -1)
        for imu in range(mu_first, mu_last):
            combRelIsoPFFSRCorr = muon_iso[imu]
            for ifsr, is_fsr in enumerate(has_fsr):
                if is_fsr:
                    deta = muon_eta[imu] - fsr_eta[ifsr]
                    dphi = (
                        np.mod(muon_phi[imu] - fsr_phi[ifsr] + np.pi, 2 * np.pi) - np.pi
                    )
                    dR = np.sqrt(deta**2 + dphi**2)
                    if dR > 0.01 and dR < 0.4:
                        combRelIsoPFFSRCorr = max(
                            0.0, muon_iso[imu] - (fsr_pt[ifsr] / muon_pt[imu])
                        )
            muon_iso[imu] = combRelIsoPFFSRCorr

    return muFsrPhotonIdx, eleFsrPhotonIdx, fsrPhoton_myMuonIdx, fsrPhoton_myElectronIdx


def add_p4_field(leptons, fsr_photons):
    # get lepton and fsr match idx
    if hasattr(leptons, "muFsrPhotonIdx"):
        lepton_fsr_idx = leptons.muFsrPhotonIdx
        fsr_lepton_idx = fsr_photons.MuonIdx
    else:
        lepton_fsr_idx = leptons.eleFsrPhotonIdx
        fsr_lepton_idx = fsr_photons.ElectronIdx

    # select leptons with and without matched fsr photons
    has_matched_fsr_photons = (
        lepton_fsr_idx > -1
    ) & (ak.num(leptons, axis=1) > 0) & (ak.num(fsr_photons, axis=1) > 0)
    leptons_with_matched_fsrphotons = ak.pad_none(leptons[has_matched_fsr_photons], 1)
    leptons_without_matched_fsrphotons = ak.pad_none(
        leptons[~has_matched_fsr_photons], 1
    )

    # select fsr photons with matched leptons
    has_matched_leptons = (
        fsr_lepton_idx > -1
    ) & (ak.num(leptons, axis=1) > 0) & (ak.num(fsr_photons, axis=1) > 0)
    fsr_with_matched_leptons = ak.pad_none(fsr_photons[has_matched_leptons], 1)

    # add 'p4' field by adding matched fsr photon
    leptons_with_matched_fsrphotons["p4"] = (
        leptons_with_matched_fsrphotons + fsr_with_matched_leptons
    )
    leptons_with_matched_fsrphotons.p4 = ak.zip(
        {
            "pt": leptons_with_matched_fsrphotons.p4.pt,
            "eta": leptons_with_matched_fsrphotons.p4.eta,
            "phi": leptons_with_matched_fsrphotons.p4.phi,
            "mass": leptons_with_matched_fsrphotons.p4.mass,
            "charge": leptons_with_matched_fsrphotons.p4.charge,
            "pdgId": leptons_with_matched_fsrphotons.pdgId,
        },
        with_name="PtEtaPhiMCandidate",
        behavior=candidate.behavior,
    )
    leptons_without_matched_fsrphotons["p4"] = ak.zip(
        {
            "pt": leptons_without_matched_fsrphotons.pt,
            "eta": leptons_without_matched_fsrphotons.eta,
            "phi": leptons_without_matched_fsrphotons.phi,
            "mass": leptons_without_matched_fsrphotons.mass,
            "charge": leptons_without_matched_fsrphotons.charge,
            "pdgId": leptons_without_matched_fsrphotons.pdgId,
        },
        with_name="PtEtaPhiMCandidate",
        behavior=candidate.behavior,
    )
    leptons_with_matched_fsrphotons["p4_orig"] = ak.zip(
        {
            "pt": leptons_with_matched_fsrphotons.pt,
            "eta": leptons_with_matched_fsrphotons.eta,
            "phi": leptons_with_matched_fsrphotons.phi,
            "mass": leptons_with_matched_fsrphotons.mass,
            "charge": leptons_with_matched_fsrphotons.charge,
            "pdgId": leptons_with_matched_fsrphotons.pdgId,
        },
        with_name="PtEtaPhiMCandidate",
        behavior=candidate.behavior,
    )
    leptons_without_matched_fsrphotons["p4_orig"] = ak.zip(
        {
            "pt": leptons_without_matched_fsrphotons.pt,
            "eta": leptons_without_matched_fsrphotons.eta,
            "phi": leptons_without_matched_fsrphotons.phi,
            "mass": leptons_without_matched_fsrphotons.mass,
            "charge": leptons_without_matched_fsrphotons.charge,
            "pdgId": leptons_without_matched_fsrphotons.pdgId,
        },
        with_name="PtEtaPhiMCandidate",
        behavior=candidate.behavior,
    )
    return ak.concatenate(
        [leptons_with_matched_fsrphotons, leptons_without_matched_fsrphotons], axis=1
    )
