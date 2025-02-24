import inspect
import numpy as np
import awkward as ak
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.working_points import working_points
from analysis.selections import (
    delta_r_higher,
    delta_r_lower,
    fsr_matching,
    add_p4_field,
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
        """
        # FSR matching takes numpy arrays as inputs
        muon_offsets = np.concatenate(([0], np.cumsum(ak.num(self.objects["muons"], axis=1))))
        muon_pt = np.array(ak.flatten(self.objects["muons"].pt), dtype=float)
        muon_eta = np.array(ak.flatten(self.objects["muons"].eta), dtype=float)
        muon_phi = np.array(ak.flatten(self.objects["muons"].phi), dtype=float)
        muon_iso = np.array(ak.flatten(self.objects["muons"].pfRelIso03_all), dtype=float)
        electron_offsets = np.concatenate(([0], np.cumsum(ak.num(self.objects["electrons"], axis=1))))
        electron_eta = np.array(ak.flatten(self.objects["electrons"].eta), dtype=float)
        electron_phi = np.array(ak.flatten(self.objects["electrons"].phi), dtype=float)
        fsr_offsets = np.concatenate(([0], np.cumsum(ak.num(self.objects["fsr_photons"], axis=1))))
        fsr_pt = np.array(ak.flatten(self.objects["fsr_photons"].pt), dtype=float)
        fsr_eta = np.array(ak.flatten(self.objects["fsr_photons"].eta), dtype=float)
        fsr_phi = np.array(ak.flatten(self.objects["fsr_photons"].phi), dtype=float)
        # perform FSR matching
        muFsrPhotonIdx, eleFsrPhotonIdx, fsrPhoton_myMuonIdx, fsrPhoton_myElectronIdx = fsr_matching(
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
        )
        # add FSR/lepton ids
        nmuon = ak.num(self.objects["muons"], axis=1)
        nelectron = ak.num(self.objects["electrons"], axis=1)
        nfsr = ak.num(self.objects["fsr_photons"], axis=1)
        self.objects["muons"]["muFsrPhotonIdx"] = ak.unflatten(muFsrPhotonIdx, nmuon)
        self.objects["electrons"]["eleFsrPhotonIdx"] = ak.unflatten(eleFsrPhotonIdx, nelectron)
        self.objects["fsr_photons"]["MuonIdx"] = ak.unflatten(fsrPhoton_myMuonIdx, nfsr)
        self.objects["fsr_photons"]["ElectronIdx"] = ak.unflatten(fsrPhoton_myElectronIdx, nfsr)
        # apply iso cut on muons
        self.objects["muons"]["pfRelIso03_all"] = ak.unflatten(muon_iso, nmuon)
        self.objects["muons"] = self.objects["muons"][self.objects["muons"].pfRelIso03_all < 0.35]
        # assign mass and charge fields to FSR to be able to compute 4-vector
        self.objects["fsr_photons"]["mass"] = 0
        self.objects["fsr_photons"]["charge"] = 0
        # add 'p4' by adding matched FSR. Also add 'p4_orig' for QCD supression
        self.objects["electrons"] = add_p4_field(self.objects["electrons"], self.objects["fsr_photons"])
        self.objects["muons"] = add_p4_field(self.objects["muons"], self.objects["fsr_photons"])
        """
        self.objects["muons"] = self.objects["muons"][
            self.objects["muons"].pfRelIso03_all < 0.35
        ]
        self.objects["muons"]["p4"] = ak.zip(
            {
                "pt": self.objects["muons"].pt,
                "eta": self.objects["muons"].eta,
                "phi": self.objects["muons"].phi,
                "mass": self.objects["muons"].mass,
                "charge": self.objects["muons"].charge,
                "pdgId": self.objects["muons"].pdgId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        self.objects["muons"]["p4_orig"] = ak.zip(
            {
                "pt": self.objects["muons"].pt,
                "eta": self.objects["muons"].eta,
                "phi": self.objects["muons"].phi,
                "mass": self.objects["muons"].mass,
                "charge": self.objects["muons"].charge,
                "pdgId": self.objects["muons"].pdgId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        self.objects["electrons"]["p4"] = ak.zip(
            {
                "pt": self.objects["electrons"].pt,
                "eta": self.objects["electrons"].eta,
                "phi": self.objects["electrons"].phi,
                "mass": self.objects["electrons"].mass,
                "charge": self.objects["electrons"].charge,
                "pdgId": self.objects["electrons"].pdgId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        self.objects["electrons"]["p4_orig"] = ak.zip(
            {
                "pt": self.objects["electrons"].pt,
                "eta": self.objects["electrons"].eta,
                "phi": self.objects["electrons"].phi,
                "mass": self.objects["electrons"].mass,
                "charge": self.objects["electrons"].charge,
                "pdgId": self.objects["electrons"].pdgId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # concatenate muons and electrons
        self.objects["leptons"] = ak.concatenate(
            [self.objects["muons"], self.objects["electrons"]], axis=1
        )
        self.objects["leptons"] = self.objects["leptons"][
            ak.argsort(self.objects["leptons"].pt, axis=1)
        ]
        self.objects["leptons"]["idx"] = ak.local_index(self.objects["leptons"], axis=1)

        self.objects["leptons"] = ak.zip(
            {
                "pt": self.objects["leptons"].pt,
                "eta": self.objects["leptons"].eta,
                "phi": self.objects["leptons"].phi,
                "mass": self.objects["leptons"].mass,
                "charge": self.objects["leptons"].charge,
                "pdgId": self.objects["leptons"].pdgId,
                "p4": self.objects["leptons"].p4,
                "p4_orig": self.objects["leptons"].p4_orig,
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
