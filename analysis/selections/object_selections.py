import inspect
import numpy as np
import awkward as ak
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.working_points import working_points
from analysis.selections import (
    delta_r_higher,
    delta_r_lower,
    select_dileptons,
    transverse_mass,
    get_closest_lepton,
    assign_lepton_fsr_idx,
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
        # add loose, relaxed and tight selection masks to muons and electrons.
        # tight leptons are used for SR and CRs. loose leptons are used for Z+X CR.
        is_loose_muon = (
            (self.objects["muons"].pt > 5)
            & (np.abs(self.objects["muons"].eta) < 2.4)
            & (np.abs(self.objects["muons"].dxy) < 0.5)
            & (np.abs(self.objects["muons"].dz) < 1)
            & (self.objects["muons"].isGlobal | self.objects["muons"].isTracker)
        )
        is_relaxed_muon = is_loose_muon & (np.abs(self.objects["muons"].sip3d) < 4)
        is_tight_muon = (
            is_relaxed_muon
            & (
                ((self.objects["muons"].pt < 200) & self.objects["muons"].tightId)
                | (
                    (self.objects["muons"].pt >= 200)
                    & (
                        (self.objects["muons"].tightId)
                        | (self.objects["muons"].highPtId == 1)
                    )
                )
            )
        )
        muons = self.objects["muons"]
        muons["is_loose"] = is_loose_muon
        muons["is_relaxed"] = is_relaxed_muon
        muons["is_tight"] = is_tight_muon

        is_loose_electron = (
            (self.objects["electrons"].pt > 7)
            & (np.abs(self.objects["electrons"].eta) < 2.5)
            & (np.abs(self.objects["electrons"].dxy) < 0.5)
            & (np.abs(self.objects["electrons"].dz) < 1)
            & (delta_r_higher(self.objects["electrons"], self.objects["muons"], 0.05))
        )
        is_relaxed_electron = is_loose_electron & (self.objects["electrons"].sip3d < 4)
        is_tight_electron = is_relaxed_electron & (
            working_points.electron_id(self.events, "bdt")
        )
        electrons = self.objects["electrons"]
        electrons["is_loose"] = is_loose_electron
        electrons["is_relaxed"] = is_relaxed_electron
        electrons["is_tight"] = is_tight_electron

        # get leptons before FSR recovery/iso correction
        helper_leptons = ak.concatenate([muons, electrons], axis=1)
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
                "is_loose": helper_leptons.is_loose,
                "is_relaxed": helper_leptons.is_relaxed,
                "is_tight": helper_leptons.is_tight,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # select FSR photons
        fsr_photons = self.objects["fsr_photons"]
        fsr_photons["mass"] = 0
        fsr_photons["charge"] = 0
        fsr_photons["idx"] = ak.local_index(fsr_photons, axis=1)
        # find closest lepton
        closest_lepton, dr_et2 = get_closest_lepton(fsr_photons, helper_leptons)
        fsr_photons["helper_idx"] = ak.local_index(closest_lepton, axis=1)
        fsr_photons["lepton_idx"] = ak.fill_none(closest_lepton.idx, -1)
        # if the FSR photon is matched to multiple leptons, choose the one with lowest dREt2
        fsr_cartesian = ak.cartesian(
            {"fsr1": fsr_photons, "fsr2": fsr_photons}, nested=True, axis=1
        )
        is_duplicate = (
            ak.sum(
                fsr_cartesian.fsr1.lepton_idx == fsr_cartesian.fsr2.lepton_idx, axis=-1
            )
            > 1
        )
        min_dr_et2 = ak.argmin(dr_et2.mask[is_duplicate], axis=-1, keepdims=True)
        is_min = ak.fill_none(
            ak.flatten(min_dr_et2[:, None] == fsr_photons.helper_idx, axis=-1), False
        )
        new_lepton_idx = ak.where(
            ~is_duplicate | (is_duplicate & is_min), fsr_photons.lepton_idx, -1
        )
        fsr_photons["lepton_idx"] = new_lepton_idx

        # add fsr_idx field to leptons
        helper_leptons = assign_lepton_fsr_idx(fsr_photons, helper_leptons)

        # for each FSR photon that was selected, we exclude that photon from the isolation sum of all the leptons in the event
        # This concerns the photons that are in the isolation cone and outside the isolation veto of said leptons dR < 0.4 AND dR > 0.01
        selected_fsr_photons = fsr_photons[fsr_photons.lepton_idx > -1]
        muons_matchedfsr_cartesian = ak.cartesian(
            {"muon": muons, "fsr": selected_fsr_photons},
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
        muon_corrected_iso = muons.pfRelIso03_all - (valid_matchedfsr_pt / muons.pt)
        # update pfRelIso03_all field with corrected isolation
        muons["pfRelIso03_all"] = ak.where(
            muon_corrected_iso > 0, muon_corrected_iso, 0.0
        )
        electrons["pfRelIso03_all"] = ak.zeros_like(electrons.pt)
        # concatenate muons and electrons with corrected iso
        leptons = ak.concatenate([muons, electrons], axis=1)
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
                "is_loose": leptons.is_loose,
                "is_relaxed": leptons.is_relaxed,
                "is_tight": leptons.is_tight,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # select leptons (muons) such that relIso < 0.35 and update lepton index
        leptons = leptons[leptons.pfRelIso03_all < 0.35]
        leptons["idx"] = ak.local_index(leptons, axis=1)
        
        # assign -1 to FSR lepton_idx associated with the excluded leptons
        index_still_present = ak.any(
            fsr_photons.idx == leptons.fsr_idx[:, None], axis=-1
        )
        new_lepton_idx = ak.where(index_still_present, fsr_photons.lepton_idx, -1)
        fsr_photons["lepton_idx"] = new_lepton_idx

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

        # add p4 field to leptons adding the matched FSR photons
        leptons_with_matched_fsrphotons["p4"] = ak.zip(
            {
                "pt": (leptons_with_matched_fsrphotons + fsr_with_matched_leptons).pt,
                "eta": (leptons_with_matched_fsrphotons + fsr_with_matched_leptons).eta,
                "phi": (leptons_with_matched_fsrphotons + fsr_with_matched_leptons).phi,
                "mass": (
                    leptons_with_matched_fsrphotons + fsr_with_matched_leptons
                ).mass,
                "charge": (
                    leptons_with_matched_fsrphotons + fsr_with_matched_leptons
                ).charge,
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
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # concatenate leptons with and without matched FSR photons after FSR recovery
        leptons = ak.concatenate(
            [leptons_with_matched_fsrphotons, leptons_without_matched_fsrphotons],
            axis=1,
        )
        self.objects["leptons"] = leptons

        
    def select_zzto4l_zcandidates(self):
        self.objects["zcandidates"] = ak.combinations(
            self.objects["leptons"], 2, fields=["l1", "l2"]
        )
        self.objects["zcandidates"]["p4"] = (
            self.objects["zcandidates"].l1.p4 + self.objects["zcandidates"].l2.p4
        )
        self.objects["zcandidates"]["pt"] = self.objects["zcandidates"].p4.pt

        
    def select_zplusx_best_zcandidate(self):
        zmass = 91.1876
        self.objects["zcandidates"]["idx"] = ak.local_index(
            self.objects["zcandidates"], axis=1
        )
        best_zcandidate_idx = ak.argmin(
            np.abs(self.objects["zcandidates"].p4.mass - zmass), axis=1
        )
        best_zcandidate = self.objects["zcandidates"][
            best_zcandidate_idx == self.objects["zcandidates"].idx
        ]
        self.objects["best_zcandidate"] = best_zcandidate

        
    def select_zplusx_loose_leptons(self):
        # select loose leptons
        loose_leptons = self.objects["leptons"][self.objects["leptons"].is_loose]

        # select loose leptons whose idx are different to best Z candidate lepton's idx
        loose_leptons_bestzl1_idx = loose_leptons.idx != best_zcandidate.l1.idx[:, None]
        loose_leptons_bestzl2_idx = loose_leptons.idx != best_zcandidate.l2.idx[:, None]
        loose_leptons_bestz_idx_mask = ak.flatten(
            loose_leptons_bestzl1_idx & loose_leptons_bestzl2_idx, axis=-1
        )
        loose_leptons = loose_leptons[loose_leptons_bestz_idx_mask]

        # select loose leptons that are away dR > 0.02 from best Z candidate lepton's
        loose_leptons_bestzl1_dr = loose_leptons.metric_table(best_zcandidate.l1)
        loose_leptons_bestzl2_dr = loose_leptons.metric_table(best_zcandidate.l2)
        loose_leptons_bestz_dr_mask = ak.flatten(
            (loose_leptons_bestzl1_dr > 0.02) & (loose_leptons_bestzl2_dr > 0.02),
            axis=-1,
        )
        loose_leptons = loose_leptons[loose_leptons_bestz_dr_mask]

        # QCD suppression cut: invariant mass of loose lepton and the opposite sign tight lepton from the best Z candidate should satisfy m2l > 4 GeV
        loose_leptons_bestzl1_opposite_charge = ak.flatten(
            loose_leptons.charge != best_zcandidate.l1.charge[:, None], axis=-1
        )
        loose_leptons_bestzl2_opposite_charge = ak.flatten(
            loose_leptons.charge != best_zcandidate.l2.charge[:, None], axis=-1
        )
        loose_leptons_bestzl1_cartesian = ak.cartesian(
            {"lepton": loose_leptons.p4, "zl1": best_zcandidate.l1.p4},
            nested=True,
            axis=1,
        )
        loose_leptons_bestzl1_mass = ak.flatten(
            (
                loose_leptons_bestzl1_cartesian.lepton
                + loose_leptons_bestzl1_cartesian.zl1
            ).mass,
            axis=-1,
        )
        loose_leptons_bestzl1_mass_mask = loose_leptons_bestzl1_mass > 4
        loose_leptons_bestzl2_cartesian = ak.cartesian(
            {"lepton": loose_leptons.p4, "zl2": best_zcandidate.l2.p4},
            nested=True,
            axis=1,
        )
        loose_leptons_bestzl2_mass = ak.flatten(
            (
                loose_leptons_bestzl2_cartesian.lepton
                + loose_leptons_bestzl2_cartesian.zl2
            ).mass,
            axis=-1,
        )
        loose_leptons_bestzl2_mass_mask = loose_leptons_bestzl2_mass > 4
        qcd_suppression_mask = (
            loose_leptons_bestzl1_opposite_charge & loose_leptons_bestzl1_mass_mask
        ) | (loose_leptons_bestzl2_opposite_charge & loose_leptons_bestzl2_mass_mask)
        loose_leptons = loose_leptons[qcd_suppression_mask]

        self.objects["loose_leptons"] = loose_leptons

        
    def select_zzto4l_zzcandidates(self):
        zzcandidates = ak.combinations(
            self.objects["zcandidates"], 2, fields=["z1", "z2"]
        )
        zzcandidates = ak.zip(
            {
                "z1": ak.zip(
                    {
                        "l1": zzcandidates.z1.l1,
                        "l2": zzcandidates.z1.l2,
                        "p4": zzcandidates.z1.l1.p4 + zzcandidates.z1.l2.p4,
                    }
                ),
                "z2": ak.zip(
                    {
                        "l1": zzcandidates.z2.l1,
                        "l2": zzcandidates.z2.l2,
                        "p4": zzcandidates.z2.l1.p4 + zzcandidates.z2.l2.p4,
                    }
                ),
            }
        )
        # sort ZZ candidates by they proximity to the Z mass
        zmass = 91.1876
        dist_from_z1_to_zmass = np.abs(zzcandidates.z1.p4.mass - zmass)
        dist_from_z2_to_zmass = np.abs(zzcandidates.z2.p4.mass - zmass)
        z1 = ak.where(
            dist_from_z1_to_zmass > dist_from_z2_to_zmass,
            zzcandidates.z2,
            zzcandidates.z1,
        )
        z2 = ak.where(
            dist_from_z1_to_zmass < dist_from_z2_to_zmass,
            zzcandidates.z2,
            zzcandidates.z1,
        )
        zzcandidates = ak.zip(
            {
                "z1": ak.zip(
                    {
                        "l1": z1.l1,
                        "l2": z1.l2,
                        "p4": z1.p4,
                    }
                ),
                "z2": ak.zip(
                    {
                        "l1": z2.l1,
                        "l2": z2.l2,
                        "p4": z2.p4,
                    }
                ),
            }
        )
        # chech that Z1 mass > 40 GeV
        z1_mass_g40_mask = zzcandidates.z1.p4.mass > 40
        # check that the Zs are mutually exclusive (not sharing the same lepton)
        share_same_lepton_mask = (
            (zzcandidates.z1.l1.idx == zzcandidates.z2.l1.idx)
            | (zzcandidates.z1.l2.idx == zzcandidates.z2.l2.idx)
            | (zzcandidates.z1.l2.idx == zzcandidates.z2.l1.idx)
            | (zzcandidates.z1.l2.idx == zzcandidates.z2.l2.idx)
        )
        # ghost removal: ∆R(η, φ) > 0.02 between each of the four leptons (to protect against split tracks)
        ghost_removal_mask = (
            (zzcandidates.z1.l1.delta_r(zzcandidates.z1.l2) > 0.02)
            & (zzcandidates.z1.l1.delta_r(zzcandidates.z2.l1) > 0.02)
            & (zzcandidates.z1.l1.delta_r(zzcandidates.z2.l2) > 0.02)
            & (zzcandidates.z1.l2.delta_r(zzcandidates.z2.l1) > 0.02)
            & (zzcandidates.z1.l2.delta_r(zzcandidates.z2.l2) > 0.02)
            & (zzcandidates.z2.l1.delta_r(zzcandidates.z2.l2) > 0.02)
        )
        # trigger acceptance: two of the four selected leptons should pass pT,i > 20 GeV and pT,j > 10 (FSR photons are used)
        trigger_acceptance_mask = (
            ((zzcandidates.z1.l1.p4.pt > 20) & (zzcandidates.z1.l2.p4.pt > 10))
            | ((zzcandidates.z1.l1.p4.pt > 20) & (zzcandidates.z2.l1.p4.pt > 10))
            | ((zzcandidates.z1.l1.p4.pt > 20) & (zzcandidates.z2.l2.p4.pt > 10))
            | ((zzcandidates.z1.l2.p4.pt > 20) & (zzcandidates.z1.l1.p4.pt > 10))
            | ((zzcandidates.z1.l2.p4.pt > 20) & (zzcandidates.z2.l1.p4.pt > 10))
            | ((zzcandidates.z1.l2.p4.pt > 20) & (zzcandidates.z2.l2.p4.pt > 10))
            | ((zzcandidates.z2.l1.p4.pt > 20) & (zzcandidates.z1.l1.p4.pt > 10))
            | ((zzcandidates.z2.l1.p4.pt > 20) & (zzcandidates.z1.l2.p4.pt > 10))
            | ((zzcandidates.z2.l1.p4.pt > 20) & (zzcandidates.z2.l2.p4.pt > 10))
            | ((zzcandidates.z2.l2.p4.pt > 20) & (zzcandidates.z1.l1.p4.pt > 10))
            | ((zzcandidates.z2.l2.p4.pt > 20) & (zzcandidates.z1.l2.p4.pt > 10))
            | ((zzcandidates.z2.l2.p4.pt > 20) & (zzcandidates.z2.l1.p4.pt > 10))
        )
        # QCD suppression: all four opposite-sign pairs that can be built with the four leptons (regardless of lepton flavor) must satisfy m > 4 GeV
        # FSR photons are not used since a QCD-induced low mass dilepton (eg. Jpsi) may have photons nearby (e.g. from π0).
        qcd_suppression_mask = (
            (
                (zzcandidates.z1.l1.charge != zzcandidates.z1.l2.charge)
                & ((zzcandidates.z1.l1 + zzcandidates.z1.l2).mass > 4)
            )
            | (
                (zzcandidates.z1.l1.charge != zzcandidates.z2.l1.charge)
                & ((zzcandidates.z1.l1 + zzcandidates.z2.l1).mass > 4)
            )
            | (
                (zzcandidates.z1.l1.charge != zzcandidates.z2.l2.charge)
                & ((zzcandidates.z1.l1 + zzcandidates.z2.l2).mass > 4)
            )
            | (
                (zzcandidates.z1.l2.charge != zzcandidates.z2.l1.charge)
                & ((zzcandidates.z1.l2 + zzcandidates.z2.l1).mass > 4)
            )
            | (
                (zzcandidates.z1.l2.charge != zzcandidates.z2.l2.charge)
                & ((zzcandidates.z1.l2 + zzcandidates.z2.l2).mass > 4)
            )
            | (
                (zzcandidates.z2.l1.charge != zzcandidates.z2.l2.charge)
                & ((zzcandidates.z2.l1 + zzcandidates.z2.l2).mass > 4)
            )
        )
        # select good ZZ candidates
        full_mask = (
            z1_mass_g40_mask
            & ~share_same_lepton_mask
            & ghost_removal_mask
            & trigger_acceptance_mask
            & qcd_suppression_mask
        )
        zzcandidates = zzcandidates[ak.fill_none(full_mask, False)]

        # get alternative pairing for same-sign candidates (FSR photons are used)
        # select same flavor pairs
        sf_pairs = np.abs(zzcandidates.z1.l1.pdgId) == np.abs(zzcandidates.z2.l1.pdgId)
        zzcandidates_sf = zzcandidates.mask[sf_pairs]
        # get initial alternative pairs
        ops = zzcandidates_sf.z1.l1.pdgId == -zzcandidates_sf.z2.l1.pdgId
        za0 = ak.where(
            ops,
            zzcandidates_sf.z1.l1.p4 + zzcandidates_sf.z2.l1.p4,
            zzcandidates_sf.z1.l1.p4 + zzcandidates_sf.z2.l2.p4,
        )
        zb0 = ak.where(
            ops,
            zzcandidates_sf.z1.l2.p4 + zzcandidates_sf.z2.l2.p4,
            zzcandidates_sf.z1.l2.p4 + zzcandidates_sf.z2.l1.p4,
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
            (np.abs(za.mass - zmass) < np.abs(zzcandidates.z1.p4.mass - zmass))
            & (zb.mass < 12)
        )
        smart_cut = ak.fill_none(smart_cut, True)

        self.objects["zzcandidates"] = zzcandidates[smart_cut]
        self.objects["zzcandidates"]["p4"] = (
            self.objects["zzcandidates"].z1.p4 + self.objects["zzcandidates"].z2.p4
        )
        self.objects["zzcandidates"]["pt"] = self.objects["zzcandidates"].p4.pt

        
    def select_zzto4l_best_zzcandidate(self):
        """
        selects best zz candidate as the one with Z1 closest in mass to nominal Z boson mass
        and Z2 from the candidates whose lepton give higher pT sum
        """
        # get mask of Z1's closest to Z
        zmass = 91.1876
        z1_dist_to_z = np.abs(self.objects["zzcandidates"].z1.p4.mass - zmass)
        min_z1_dist_to_z = ak.min(z1_dist_to_z, axis=1)
        closest_z1_mask = z1_dist_to_z == min_z1_dist_to_z
        # get mask of Z2's with higher pT sum
        z2_pt_sum = (
            self.objects["zzcandidates"].z2.l1.p4.pt
            + self.objects["zzcandidates"].z2.l2.p4.pt
        )
        max_z2_pt_sum = ak.max(z2_pt_sum[closest_z1_mask], axis=1)
        best_candidate_mask = (z2_pt_sum == max_z2_pt_sum) & closest_z1_mask
        # select best candidate from zzcandidates
        self.objects["best_zzcandidate"] = self.objects["zzcandidates"][
            best_candidate_mask
        ]

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

    def select_hww_zcandidates(self):
        self.objects["zcandidates"] = ak.combinations(
            self.objects["leptons"], 2, fields=["l1", "l2"]
        )
        self.objects["zcandidates"].pt = (
            self.objects["zcandidates"].l1.pt + self.objects["zcandidates"].l2.pt
        )

    def select_hww_mll(self):
        self.objects["mll"] = transverse_mass(
            self.objects["zcandidates"].l1 + self.objects["zcandidates"].l2,
            self.objects["met"],
        )

    def select_hww_ml1(self):
        self.objects["ml1"] = transverse_mass(
            self.objects["zcandidates"].l1, self.objects["met"]
        )

    def select_hww_ml2(self):
        self.objects["ml2"] = transverse_mass(
            self.objects["zcandidates"].l2, self.objects["met"]
        )

    def select_candidate_cjet(self):
        self.objects["candidate_cjet"] = self.objects["cjets"][
            ak.argmax(self.objects["cjets"].btagDeepFlavCvL, axis=1)
            == ak.local_index(self.objects["cjets"], axis=1)
        ]
