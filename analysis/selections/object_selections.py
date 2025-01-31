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
    select_zzto4l_zz_candidates,
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

        # select leptons with matched fsr photons
        leptons_with_match_fsrphotons = self.objects[leptons][self.objects[leptons].fsrPhotonIdx > -1]

        # impose some conditions on fsr photons 
        # and select further the leptons matched to the remaining fsr photons
        fsr_photons = self.events.FsrPhoton[leptons_with_match_fsrphotons.fsrPhotonIdx]
        fsr_photons = fsr_photons[
            (fsr_photons.pt > 2)
            & (np.abs(fsr_photons.eta) < 2.5)
            & (fsr_photons.relIso03 < 1.8)
            & (fsr_photons.dROverEt2 < 0.012)
            & (delta_r_lower(fsr_photons, leptons_with_match_fsrphotons, 0.5))
        ]
        has_fsr_match = (ak.num(fsr_photons) > 0) & (self.objects[leptons].fsrPhotonIdx > -1)
        leptons_with_match_fsrphotons = self.objects[leptons][has_fsr_match]
        leptons_without_match_fsrphotons = self.objects[leptons][~has_fsr_match]

        # add fsr photons to leptons
        fsr_photons["mass"] = 0
        fsr_photons["charge"] = 0
        leptons_plus_fsrphotons = ak.zip(
            {
                "pt": (leptons_with_match_fsrphotons + fsr_photons).pt,
                "eta": (leptons_with_match_fsrphotons + fsr_photons).eta,
                "phi": (leptons_with_match_fsrphotons + fsr_photons).phi,
                "charge": (leptons_with_match_fsrphotons + fsr_photons).charge,
                "mass": (leptons_with_match_fsrphotons + fsr_photons).mass,
                "pdgId": leptons_with_match_fsrphotons.pdgId,
                "idx": leptons_with_match_fsrphotons.idx
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

        # save original lepton 4-vectors
        orig_leptons = ak.zip(
            {
                "pt": self.objects[leptons].pt,
                "eta": self.objects[leptons].eta,
                "phi": self.objects[leptons].phi,
                "charge": self.objects[leptons].charge,
                "mass": self.objects[leptons].mass,
                "pdgId": self.objects[leptons].pdgId,
                "idx": self.objects[leptons].idx
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # concatenate leptons with and without matched fsr photons
        self.objects[leptons] = ak.concatenate([leptons_plus_fsrphotons, leptons_without_match_fsrphotons], axis=1)
        self.objects[leptons] = self.objects[leptons][ak.argsort(self.objects[leptons].pt, axis=1)]
        self.objects[leptons] = ak.zip(
            {
                "pt": self.objects[leptons].pt,
                "eta": self.objects[leptons].eta,
                "phi": self.objects[leptons].phi,
                "mass": self.objects[leptons].mass,
                "charge": self.objects[leptons].charge,
                "pdgId": self.objects[leptons].pdgId,
                "idx": self.objects[leptons].idx,
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
        # sort lepton pairs by proximity to Z mass
        zmass = 91.1876
        dist_from_z_all_pairs = np.abs(
            (self.objects["ll_pairs"].l1 + self.objects["ll_pairs"].l2).mass - zmass
        )
        sorted_ll_pairs = self.objects["ll_pairs"][
            ak.argsort(dist_from_z_all_pairs, axis=1)
        ]
        # ZZ candidates
        zz_pairs = select_zzto4l_zz_candidates(sorted_ll_pairs)
        # mass-sorted alternative pairing Z candidates
        alt_sorted_ll_pairs = self.objects["ll_pairs"][
            ak.argsort(
                -(self.objects["ll_pairs"].l1 + self.objects["ll_pairs"].l2).mass,
                axis=1,
            )
        ]
        alt_zz_pairs = select_zzto4l_zz_candidates(alt_sorted_ll_pairs)
        # 'smart cut': require NOT(|mZa - mZ| < |mZ1 − mZ| AND mZb < 12)
        # This cut discards 4µ and 4e candidates where the alternative pairing looks like an on-shell Z + low-mass l+l−
        smart_cut = ~(
            (
                np.abs(alt_zz_pairs.z1.p4.mass - zmass)
                < np.abs(zz_pairs.z1.p4.mass - zmass)
            )
            & (alt_zz_pairs.z2.p4.mass < 12)
        )
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
