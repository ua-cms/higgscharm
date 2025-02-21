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
    def fsr_recovery(self, leptons: str = "electrons"):
        # add index field
        self.events["FsrPhoton", "idx"] = ak.local_index(self.events.FsrPhoton, axis=1)
        self.objects[leptons]["idx"] = ak.local_index(self.objects[leptons], axis=1)
        # save original lepton 4-vectors
        orig_leptons = ak.zip(
            {
                "pt": self.objects[leptons].pt,
                "eta": self.objects[leptons].eta,
                "phi": self.objects[leptons].phi,
                "charge": self.objects[leptons].charge,
                "mass": self.objects[leptons].mass,
                "pdgId": self.objects[leptons].pdgId,
                "idx": self.objects[leptons].idx,
                "fsrPhotonIdx": self.objects[leptons].fsrPhotonIdx,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # select initial leptons with matched fsr photons
        leptons_with_matched_fsrphotons = self.objects[leptons][
            self.objects[leptons].fsrPhotonIdx > -1
        ]
        # impose some conditions on fsr photons
        # and select further the leptons matched to the remaining fsr photons
        fsr_photons = self.events.FsrPhoton[
            leptons_with_matched_fsrphotons.fsrPhotonIdx
        ]
        fsr_photons = fsr_photons[
            (fsr_photons.pt > 2)
            & (np.abs(fsr_photons.eta) < (2.5 if leptons == "electrons" else 2.4))
            & (fsr_photons.relIso03 < 1.8)
            & (fsr_photons.dROverEt2 < 0.012)
            & (delta_r_lower(fsr_photons, leptons_with_matched_fsrphotons, 0.5))
        ]
        # get leptonIdx from fsr photons
        if leptons == "muons":
            frs_index = ak.fill_none(ak.pad_none(fsr_photons.muonIdx, 1), -99)
        else:
            frs_index = ak.fill_none(ak.pad_none(fsr_photons.electronIdx, 1), -99)
        # select leptons with and without matched fsr photons within selected fsr photons
        helper_index = ak.where(
            self.objects[leptons].fsrPhotonIdx > -1, self.objects[leptons].idx, -1
        )
        has_matched_fsrphotons = ak.any(helper_index[:, None] == frs_index, axis=-2)
        leptons_with_matched_fsrphotons = self.objects[leptons][has_matched_fsrphotons]
        leptons_without_matched_fsrphotons = self.objects[leptons][
            ~has_matched_fsrphotons
        ]
        # select fsr photons with any matched leptons within selected leptons
        has_matched_leptons = ak.any(
            frs_index[:, None] == leptons_with_matched_fsrphotons.idx, axis=-2
        )
        fsr_photons = fsr_photons[has_matched_leptons]
        # pad none values to be able to make operations on empty entries
        fsr_photons = ak.pad_none(fsr_photons, 1)
        leptons_with_matched_fsrphotons = ak.pad_none(
            leptons_with_matched_fsrphotons, 1
        )
        # add fsr photons and leptons
        fsr_photons["mass"] = 0
        fsr_photons["charge"] = 0
        leptons_plus_fsrphotons = ak.zip(
            {
                "pt": (leptons_with_matched_fsrphotons + fsr_photons).pt,
                "eta": (leptons_with_matched_fsrphotons + fsr_photons).eta,
                "phi": (leptons_with_matched_fsrphotons + fsr_photons).phi,
                "charge": (leptons_with_matched_fsrphotons + fsr_photons).charge,
                "mass": (leptons_with_matched_fsrphotons + fsr_photons).mass,
                "pdgId": leptons_with_matched_fsrphotons.pdgId,
                "idx": leptons_with_matched_fsrphotons.idx,
                "fsrPhotonIdx": leptons_with_matched_fsrphotons.fsrPhotonIdx,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # concatenate leptons with and without matched fsr photons
        self.objects[leptons] = ak.where(
            ak.any(has_matched_fsrphotons, axis=1)
            & ak.any(has_matched_leptons, axis=1),
            ak.concatenate(
                [leptons_plus_fsrphotons, leptons_without_matched_fsrphotons], axis=1
            ),
            leptons_without_matched_fsrphotons,
        )
        self.objects[leptons] = ak.zip(
            {
                "pt": self.objects[leptons].pt,
                "eta": self.objects[leptons].eta,
                "phi": self.objects[leptons].phi,
                "charge": self.objects[leptons].charge,
                "mass": self.objects[leptons].mass,
                "pdgId": self.objects[leptons].pdgId,
                "idx": self.objects[leptons].idx,
                "fsrPhotonIdx": self.objects[leptons].fsrPhotonIdx,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # add original lepton 4-vectors as new field
        self.objects[leptons]["orig"] = orig_leptons[self.objects[leptons].idx]

    def select_zzto4l_leptons(self):
        # do fsr recovery for electrons and muons
        self.fsr_recovery("electrons")
        self.fsr_recovery("muons")
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
                "orig": leptons.orig,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

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
                    }
                ),
                "z2": ak.zip(
                    {
                        "l1": z2.l1,
                        "l2": z2.l2,
                        "p4": z2.l1 + z2.l2,
                    }
                ),
            }
        )
        # get zz pairs with original lepton 4-vectors
        zz_pairs_orig = ak.zip(
            {
                "z1": ak.zip(
                    {
                        "l1": zz_pairs.z1.l1.orig,
                        "l2": zz_pairs.z1.l2.orig,
                        "p4": zz_pairs.z1.l1.orig + zz_pairs.z1.l2.orig,
                    }
                ),
                "z2": ak.zip(
                    {
                        "l1": zz_pairs.z2.l1.orig,
                        "l2": zz_pairs.z2.l2.orig,
                        "p4": zz_pairs.z2.l1.orig + zz_pairs.z2.l2.orig,
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
            (zz_pairs_orig.z1.p4.mass > 4)
            & (zz_pairs_orig.z2.p4.mass > 4)
            & (
                (zz_pairs_orig.z1.l1.charge + zz_pairs_orig.z2.l1.charge != 0)
                | (
                    (zz_pairs_orig.z1.l1.charge + zz_pairs_orig.z2.l1.charge == 0)
                    & ((zz_pairs_orig.z1.l1 + zz_pairs_orig.z2.l1).mass > 4)
                )
            )
            & (
                (zz_pairs_orig.z1.l1.charge + zz_pairs_orig.z2.l2.charge != 0)
                | (
                    (zz_pairs_orig.z1.l1.charge + zz_pairs_orig.z2.l2.charge == 0)
                    & ((zz_pairs_orig.z1.l1 + zz_pairs_orig.z2.l2).mass > 4)
                )
            )
            & (
                (zz_pairs_orig.z1.l2.charge + zz_pairs_orig.z2.l1.charge != 0)
                | (
                    (zz_pairs_orig.z1.l2.charge + zz_pairs_orig.z2.l1.charge == 0)
                    & ((zz_pairs_orig.z1.l2 + zz_pairs_orig.z2.l1).mass > 4)
                )
            )
            & (
                (zz_pairs_orig.z1.l2.charge + zz_pairs_orig.z2.l2.charge != 0)
                | (
                    (zz_pairs_orig.z1.l2.charge + zz_pairs_orig.z2.l2.charge == 0)
                    & ((zz_pairs_orig.z1.l2 + zz_pairs_orig.z2.l2).mass > 4)
                )
            )
        )
        # Z1 mass > 40 GeV
        mass_mask = zz_pairs.z1.p4.mass > 40
        zz_pairs = zz_pairs[
            ak.fill_none(
                ghost_removal_mask & lepton_pt_mask & qcd_condition_mask & mass_mask,
                False,
            )
        ]

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