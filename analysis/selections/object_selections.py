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
    def select_zzto4l_leptons(self):
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
