import inspect
import numpy as np
import awkward as ak

from coffea.nanoevents.methods.vector import LorentzVector
from analysis.working_points import working_points
from analysis.selections import delta_r_mask, select_dileptons, select_4leptons


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

    def select_dimuons(self):
        if "muons" not in self.objects:
            raise ValueError(f"'muons' object has not been defined!")
        self.objects["dimuons"] = select_dileptons(self.objects, "muons")

    def select_dielectrons(self):
        if "electrons" not in self.objects:
            raise ValueError(f"'electrons' object has not been defined!")
        self.objects["dielectrons"] = select_dileptons(self.objects, "electrons")

    def select_4muons(self):
        if "muons" not in self.objects:
            raise ValueError(f"'muons' object has not been defined!")
        self.objects["fourmuons"] = select_4leptons(self.objects, "muons")

    def select_higgs(self):
        # Select Z candidate with minimal |m(μμ) - m(Z)|
        zmass = 91.1876
        z1_diff = np.abs(self.objects["fourmuons"].z1.p4.mass - zmass)
        z2_diff = np.abs(self.objects["fourmuons"].z2.p4.mass - zmass)
        z1_candidate = np.where(
            z1_diff < z2_diff,
            self.objects["fourmuons"].z1,
            self.objects["fourmuons"].z2,
        )
        z2_candidate = np.where(
            z1_diff < z2_diff,
            self.objects["fourmuons"].z2,
            self.objects["fourmuons"].z1,
        )
        self.objects["higgs"] = ak.zip(
            {
                "z1": z1_candidate,
                "z2": z2_candidate,
                "p4": z1_candidate.p4 + z2_candidate.p4,
                "pt": (z1_candidate.p4 + z2_candidate.p4).pt,
            }
        )
