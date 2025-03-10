import inspect
import numpy as np
import awkward as ak
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.working_points import working_points
from analysis.selections import (
    closest,
    delta_r_higher,
    delta_r_lower,
    select_dileptons,
    transverse_mass,
)


class ObjectSelector:

    def __init__(self, object_selection_config, year):
        self.object_selection_config = object_selection_config
        self.year = year

    def select_objects(self, events):
        self.objects = {}
        self.events = events
        for obj_name, obj_config in self.object_selection_config.items():
            # check if object field is read from events or from user defined function
            if "events" in obj_config["field"]:
                self.objects[obj_name] = eval(obj_config["field"])
            else:
                selection_function = getattr(self, obj_config["field"])
                parameters = inspect.signature(selection_function).parameters.keys()
                if "cuts" in parameters:
                    selection_function(obj_config["cuts"])
                    break
                else:
                    selection_function()
            if "cuts" in obj_config:
                selection_mask = self.get_selection_mask(
                    events=events, obj_name=obj_name, cuts=obj_config["cuts"]
                )
                self.objects[obj_name] = self.objects[obj_name][selection_mask]
        return self.objects

    def get_selection_mask(self, events, obj_name, cuts):
        # bring 'objects' and to local scope
        objects = self.objects
        # initialize selection mask
        selection_mask = ak.ones_like(self.objects[obj_name].pt, dtype=bool)
        # iterate over all cuts
        for selection, str_mask in cuts.items():
            # check if 'str_mask' contains 'events' or 'objects' and evaluate string expression
            if "events" in str_mask or "objects" in str_mask:
                mask = eval(str_mask)
            # read the mask from the working points function
            else:
                signature = inspect.signature(getattr(working_points, selection))
                parameters = signature.parameters.keys()
                if "year" in parameters:
                    mask = getattr(working_points, selection)(
                        self.events, str_mask, self.year
                    )
                else:
                    mask = getattr(working_points, selection)(self.events, str_mask)
            # update selection mask
            selection_mask = np.logical_and(selection_mask, mask)
        return selection_mask

    # --------------------------------------------------------------------------------
    # ZToLL
    # --------------------------------------------------------------------------------
    def select_dimuons(self):
        if "muons" not in self.objects:
            raise ValueError(f"'muons' object has not been defined!")
        self.objects["dimuons"] = select_dileptons(self.objects, "muons")

    def select_dielectrons(self):
        if "electrons" not in self.objects:
            raise ValueError(f"'electrons' object has not been defined!")
        self.objects["dielectrons"] = select_dileptons(self.objects, "electrons")

    # --------------------------------------------------------------------------------
    # ZZTo4L
    # --------------------------------------------------------------------------------
    def select_zzto4l_leptons(self):
        # get leptons before iso correction
        helper_leptons = ak.concatenate(
            [self.objects["muons"], self.objects["electrons"]], axis=1
        )
        helper_leptons = helper_leptons[
            ak.argsort(helper_leptons.pt, axis=1, ascending=False)
        ]
        helper_leptons["idx"] = ak.local_index(helper_leptons, axis=1)
        helper_leptons = ak.zip(
            {
                "pt": helper_leptons.pt,
                "eta": helper_leptons.eta,
                "phi": helper_leptons.phi,
                "mass": helper_leptons.mass,
                "charge": helper_leptons.charge,
                "pdgId": helper_leptons.pdgId,
                "idx": helper_leptons.idx,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # get matched FSR index
        fsr_photons = self.objects["fsr_photons"]
        fsr_photons["idx"] = ak.local_index(fsr_photons, axis=1)
        fsr_leptons_closest = closest(fsr_photons, helper_leptons, apply_dREt2=True)
        fsr_photons["lepton_idx"] = ak.fill_none(fsr_leptons_closest.idx, -1)
        fsr_photons.lepton_idx = ak.pad_none(fsr_photons.lepton_idx, 1)

        # get matched leptons index
        closest_fsr = closest(fsr_photons, helper_leptons, apply_dREt2=False)
        fsr_lepton_cartesian = ak.cartesian(
            {"leptons": helper_leptons, "fsr": fsr_photons}, nested=True, axis=1
        )
        helper_lepton_fsr_idx = ak.where(
            fsr_lepton_cartesian.fsr.lepton_idx == fsr_lepton_cartesian.leptons.idx,
            closest_fsr.idx,
            -1,
        )
        lepton_fsr_idx = ak.where(
            ak.any(helper_lepton_fsr_idx > -1, axis=-1), closest_fsr.idx, -1
        )
        helper_leptons["fsr_idx"] = ak.fill_none(lepton_fsr_idx, -1)

        # For each FSR photon that was selected, we exclude that photon from the isolation sum of all the leptons in the event
        # This concerns the photons that are in the isolation cone and outside the isolation veto of said leptons dR < 0.4 AND dR > 0.01
        selected_fsr_photons = fsr_photons[fsr_photons.lepton_idx > -1]
        muons_matchedfsr_cartesian = ak.cartesian(
            {"muon": self.objects["muons"], "fsr": selected_fsr_photons},
            nested=True,
            axis=1,
        )
        muons_matchedfsr_cartesian_dr = muons_matchedfsr_cartesian.muon.delta_r(
            muons_matchedfsr_cartesian.fsr
        )
        muons_matchedfsr_cartesian_dr_valid = (muons_matchedfsr_cartesian_dr > 0.01) & (
            muons_matchedfsr_cartesian_dr < 0.4
        )
        valid_matchedfsr_pt = ak.sum(
            muons_matchedfsr_cartesian.fsr.mask[muons_matchedfsr_cartesian_dr_valid].pt,
            axis=-1,
        )
        muon_corrected_iso = self.objects["muons"].pfRelIso03_all - (
            valid_matchedfsr_pt / self.objects["muons"].pt
        )
        # update pfRelIso03_all field with corrected pfRelIso03_all
        self.objects["muons"]["pfRelIso03_all"] = ak.where(
            muon_corrected_iso > 0, muon_corrected_iso, 0.0
        )
        self.objects["electrons"]["pfRelIso03_all"] = ak.zeros_like(
            self.objects["electrons"].pt
        )
        # concatenate muons and electrons with corrected iso
        leptons = ak.concatenate(
            [self.objects["muons"], self.objects["electrons"]], axis=1
        )
        leptons = leptons[ak.argsort(leptons.pt, axis=1, ascending=False)]
        leptons = ak.zip(
            {
                "pt": leptons.pt,
                "eta": leptons.eta,
                "phi": leptons.phi,
                "mass": leptons.mass,
                "charge": leptons.charge,
                "pdgId": leptons.pdgId,
                "pfRelIso03_all": leptons.pfRelIso03_all,
                "idx": helper_leptons.idx,
                "fsr_idx": helper_leptons.fsr_idx,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # select leptons (muons) such that relIso < 0.35
        leptons = leptons.mask[leptons.pfRelIso03_all < 0.35]

        # remove excluded muons
        leptons.fsr_idx = ak.fill_none(leptons.fsr_idx, -99)
        leptons = leptons[leptons.fsr_idx >= -1]

        # remove FSR lepton_idx associated with the excluded muons
        index_still_present = ak.any(
            (fsr_photons.lepton_idx[:, None] == ak.fill_none(leptons.idx, -1)), axis=1
        )
        fsr_photons = fsr_photons[index_still_present]

        helper_leptons = ak.pad_none(leptons, 1)
        helper_fsr = ak.pad_none(fsr_photons, 1)

        # in some events, multiple photons are associated with the same lepton. For these events, choose the one that matches the leptons.idx
        is_duplicate_lepton_fsr_idx = ~ak.fill_none(
            ak.sum(helper_leptons.idx == helper_fsr.lepton_idx[:, None], axis=-1) > 1,
            False,
        )
        duplicate_fsr_idx = ak.where(
            is_duplicate_lepton_fsr_idx,
            ak.full_like(ak.pad_none(helper_leptons.fsr_idx, 1), -1),
            ak.pad_none(helper_leptons.fsr_idx, 1),
        )

        is_duplicate_lepton_fsr_idx_in_fsr_photons_idx = ak.fill_none(
            ak.any(duplicate_fsr_idx[:, None] == helper_fsr.idx, axis=-1), False
        )
        duplicate_idx = ak.any(
            ak.pad_none(fsr_photons, 1).lepton_idx.mask[
                is_duplicate_lepton_fsr_idx_in_fsr_photons_idx
            ][:, None]
            == fsr_photons.lepton_idx,
            axis=-1,
        )
        duplicate_idx_to_remove = ak.pad_none(duplicate_idx, 1) & ~ak.pad_none(
            is_duplicate_lepton_fsr_idx_in_fsr_photons_idx, 1
        )
        fsr_lepton_idx_for_duplicates = ak.where(
            duplicate_idx_to_remove, -1, ak.pad_none(helper_fsr, 1).lepton_idx
        )

        new_fsr_photons_lepton_idx = ak.where(
            ak.any(is_duplicate_lepton_fsr_idx_in_fsr_photons_idx, axis=1),
            ak.pad_none(fsr_lepton_idx_for_duplicates, 1),
            ak.pad_none(helper_fsr, 1).lepton_idx,
        )
        fsr_photons.lepton_idx = new_fsr_photons_lepton_idx

        # add p4 field for FSR photons and leptons
        fsr_photons["mass"] = 0
        fsr_photons["charge"] = 0
        fsr_photons["p4"] = ak.zip(
            {
                "pt": fsr_photons.pt,
                "eta": fsr_photons.eta,
                "phi": fsr_photons.phi,
                "mass": fsr_photons.mass,
                "charge": fsr_photons.charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        leptons["p4"] = ak.zip(
            {
                "pt": leptons.pt,
                "eta": leptons.eta,
                "phi": leptons.phi,
                "mass": leptons.mass,
                "charge": leptons.charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # select leptons with and without matched FSR photons
        has_matched_fsr_photons = leptons.fsr_idx > -1
        leptons_with_matched_fsrphotons = ak.pad_none(
            leptons[has_matched_fsr_photons], 1
        )
        leptons_without_matched_fsrphotons = ak.pad_none(
            leptons[~has_matched_fsr_photons], 1
        )
        # select FSR photons with matched leptons
        has_matched_leptons = fsr_photons.lepton_idx > -1
        fsr_with_matched_leptons = ak.pad_none(fsr_photons[has_matched_leptons], 1)
        # add matched FSR photons and leptons
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
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # keep original lepton p4 for QCD supression
        leptons_with_matched_fsrphotons["p4_orig"] = ak.zip(
            {
                "pt": leptons_with_matched_fsrphotons.pt,
                "eta": leptons_with_matched_fsrphotons.eta,
                "phi": leptons_with_matched_fsrphotons.phi,
                "mass": leptons_with_matched_fsrphotons.mass,
                "charge": leptons_with_matched_fsrphotons.charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # concatenate leptons with and without matched FSR photons after FSR recovery
        leptons_without_matched_fsrphotons["p4"] = ak.zip(
            {
                "pt": leptons_without_matched_fsrphotons.pt,
                "eta": leptons_without_matched_fsrphotons.eta,
                "phi": leptons_without_matched_fsrphotons.phi,
                "mass": leptons_without_matched_fsrphotons.mass,
                "charge": leptons_without_matched_fsrphotons.charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        leptons_without_matched_fsrphotons["p4_orig"] = (
            leptons_without_matched_fsrphotons.p4
        )
        leptons = ak.concatenate(
            [leptons_with_matched_fsrphotons, leptons_without_matched_fsrphotons],
            axis=1,
        )
        leptons = leptons[ak.argsort(leptons.pt, axis=1, ascending=False)]
        self.objects["leptons"] = leptons

        
    def select_zzto4l_ll_pairs(self):
        self.objects["ll_pairs"] = ak.combinations(
            self.objects["leptons"], 2, fields=["l1", "l2"]
        )
        self.objects["ll_pairs"].pt = (
            self.objects["ll_pairs"].l1.pt + self.objects["ll_pairs"].l2.pt
        )

        
    def select_zzto4l_zzpairs(self):
        zz_pairs = ak.combinations(self.objects["ll_pairs"], 2, fields=["z1", "z2"])
        zz_pairs = ak.zip(
            {
                "z1": ak.zip(
                    {
                        "l1": zz_pairs.z1.l1,
                        "l2": zz_pairs.z1.l2,
                        "p4": zz_pairs.z1.l1 + zz_pairs.z1.l2,
                        "p4_orig": zz_pairs.z1.l1.p4_orig + zz_pairs.z1.l2.p4_orig,
                    }
                ),
                "z2": ak.zip(
                    {
                        "l1": zz_pairs.z2.l1,
                        "l2": zz_pairs.z2.l2,
                        "p4": zz_pairs.z2.l1 + zz_pairs.z2.l2,
                        "p4_orig": zz_pairs.z2.l1.p4_orig + zz_pairs.z2.l2.p4_orig,
                    }
                ),
            }
        )
        # sort zz pairs by they proximity to Z mass
        zmass = 91.1876
        dist_from_z1_to_zmass = np.abs(zz_pairs.z1.p4.mass - zmass)
        dist_from_z2_to_zmass = np.abs(zz_pairs.z2.p4.mass - zmass)
        z1 = ak.where(
            dist_from_z1_to_zmass > dist_from_z2_to_zmass,
            zz_pairs.z2,
            zz_pairs.z1,
        )
        z2 = ak.where(
            dist_from_z1_to_zmass < dist_from_z2_to_zmass,
            zz_pairs.z2,
            zz_pairs.z1,
        )
        zz_pairs = ak.zip(
            {
                "z1": ak.zip(
                    {
                        "l1": z1.l1,
                        "l2": z1.l2,
                        "p4": z1.l1 + z1.l2,
                        "p4_orig": z1.l1.p4_orig + z1.l2.p4_orig,
                    }
                ),
                "z2": ak.zip(
                    {
                        "l1": z2.l1,
                        "l2": z2.l2,
                        "p4": z2.l1 + z2.l2,
                        "p4_orig": z2.l1.p4_orig + z2.l2.p4_orig,
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
            (zz_pairs.z1.p4_orig.mass > 4)
            & (zz_pairs.z2.p4_orig.mass > 4)
            & (
                (zz_pairs.z1.l1.charge + zz_pairs.z2.l1.charge != 0)
                | (
                    (zz_pairs.z1.l1.charge + zz_pairs.z2.l1.charge == 0)
                    & ((zz_pairs.z1.l1.p4_orig + zz_pairs.z2.l1.p4_orig).mass > 4)
                )
            )
            & (
                (zz_pairs.z1.l1.charge + zz_pairs.z2.l2.charge != 0)
                | (
                    (zz_pairs.z1.l1.charge + zz_pairs.z2.l2.charge == 0)
                    & ((zz_pairs.z1.l1.p4_orig + zz_pairs.z2.l2.p4_orig).mass > 4)
                )
            )
            & (
                (zz_pairs.z1.l2.charge + zz_pairs.z2.l1.charge != 0)
                | (
                    (zz_pairs.z1.l2.charge + zz_pairs.z2.l1.charge == 0)
                    & ((zz_pairs.z1.l2.p4_orig + zz_pairs.z2.l1.p4_orig).mass > 4)
                )
            )
            & (
                (zz_pairs.z1.l2.charge + zz_pairs.z2.l2.charge != 0)
                | (
                    (zz_pairs.z1.l2.charge + zz_pairs.z2.l2.charge == 0)
                    & ((zz_pairs.z1.l2.p4_orig + zz_pairs.z2.l2.p4_orig).mass > 4)
                )
            )
        )
        # Z1 mass > 40 GeV
        mass_mask = zz_pairs.z1.p4.mass > 40
        mask = ghost_removal_mask & lepton_pt_mask & qcd_condition_mask & mass_mask
        zz_pairs = zz_pairs[ak.fill_none(mask, False)]

        # get alternative pairing Z candidates:
        # select same flavor zz pairs
        sf_pairs = np.abs(zz_pairs.z1.l1.pdgId) == np.abs(zz_pairs.z2.l1.pdgId)
        zz_pairs_sf = zz_pairs.mask[sf_pairs]
        # get initial alternative pairs
        ops = zz_pairs_sf.z1.l1.pdgId == -zz_pairs_sf.z2.l1.pdgId
        za0 = ak.where(
            ops,
            zz_pairs_sf.z1.l1 + zz_pairs_sf.z2.l1,
            zz_pairs_sf.z1.l1 + zz_pairs_sf.z2.l2,
        )
        zb0 = ak.where(
            ops,
            zz_pairs_sf.z1.l2 + zz_pairs_sf.z2.l2,
            zz_pairs_sf.z1.l2 + zz_pairs_sf.z2.l1,
        )
        # get final alternative pairs selecting Za as the one closest to the Z mass
        zmass = 91.1876
        dist_from_za_to_zmass = np.abs(za0.mass - zmass)
        dist_from_zb_to_zmass = np.abs(zb0.mass - zmass)
        za = ak.where(
            dist_from_zb_to_zmass > dist_from_za_to_zmass,
            za0,
            zb0,
        )
        zb = ak.where(
            dist_from_zb_to_zmass < dist_from_za_to_zmass,
            za0,
            zb0,
        )
        smart_cut = ~(
            (np.abs(za.mass - zmass) < np.abs(zz_pairs.z1.p4.mass - zmass))
            & (zb.mass < 12)
        )
        smart_cut = ak.fill_none(smart_cut, True)

        self.objects["zz_pairs"] = zz_pairs[smart_cut]
        self.objects["zz_pairs"].pt = (
            self.objects["zz_pairs"].z1.p4.pt + self.objects["zz_pairs"].z2.p4.pt
        )

    def select_zzto4l_zzcandidate(self):
        """
        selects best zz candidate as the one with Z1 closest in mass to nominal Z boson mass
        and Z2 from the candidates whose lepton give higher pT sum
        """
        # get mask of Z1's closest to Z
        zmass = 91.1876
        z1_dist_to_z = np.abs(self.objects["zz_pairs"].z1.p4.mass - zmass)
        min_z1_dist_to_z = ak.min(z1_dist_to_z, axis=1)
        closest_z1_mask = z1_dist_to_z == min_z1_dist_to_z
        # get mask of Z2's with higher pT sum
        z2_pt_sum = (
            self.objects["zz_pairs"].z2.l1.pt + self.objects["zz_pairs"].z2.l2.pt
        )
        max_z2_pt_sum = ak.max(z2_pt_sum[closest_z1_mask], axis=1)
        best_candidate_mask = (z2_pt_sum == max_z2_pt_sum) & closest_z1_mask
        # select best candidate from zz_pairs
        self.objects["zz_candidate"] = self.objects["zz_pairs"][best_candidate_mask]

        
    # --------------------------------------------------------------------------------
    # HWW
    # --------------------------------------------------------------------------------
    def select_hww_leptons(self):
        # set 'leptons' by concatenating electrons and muons
        leptons = ak.concatenate(
            [self.objects["muons"], self.objects["electrons"]], axis=1
        )
        leptons = leptons[ak.argsort(leptons.pt, axis=1)]
        self.objects["leptons"] = ak.zip(
            {
                "pt": leptons.pt,
                "eta": leptons.eta,
                "phi": leptons.phi,
                "mass": leptons.mass,
                "charge": leptons.charge,
                "pdgId": leptons.pdgId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

    def select_hww_ll_pairs(self):
        self.objects["ll_pairs"] = ak.combinations(
            self.objects["leptons"], 2, fields=["l1", "l2"]
        )
        self.objects["ll_pairs"].pt = (
            self.objects["ll_pairs"].l1.pt + self.objects["ll_pairs"].l2.pt
        )

    def select_hww_mll(self):
        self.objects["mll"] = transverse_mass(
            self.objects["ll_pairs"].l1 + self.objects["ll_pairs"].l2,
            self.objects["met"],
        )

    def select_hww_ml1(self):
        self.objects["ml1"] = transverse_mass(
            self.objects["ll_pairs"].l1, self.objects["met"]
        )

    def select_hww_ml2(self):
        self.objects["ml2"] = transverse_mass(
            self.objects["ll_pairs"].l2, self.objects["met"]
        )

    def select_candidate_cjet(self):
        self.objects["candidate_cjet"] = self.objects["cjets"][
            ak.argmax(self.objects["cjets"].btagDeepFlavCvL, axis=1)
            == ak.local_index(self.objects["cjets"], axis=1)
        ]
