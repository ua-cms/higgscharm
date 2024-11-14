import inspect
import numpy as np
import awkward as ak
from analysis.selections import delta_r_mask
from analysis.working_points import working_points
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector



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

    def select_dileptons(self):
        leptons = ak.zip(
            {
                "pt": self.objects["leptons"].pt,
                "eta": self.objects["leptons"].eta,
                "phi": self.objects["leptons"].phi,
                "mass": self.objects["leptons"].mass,
                "charge": self.objects["leptons"].charge,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )
        # make sure they are sorted by transverse momentum
        leptons = leptons[ak.argsort(leptons.pt, axis=1)]
        # create pair combinations with all muons
        dileptons = ak.combinations(self.objects["leptons"], 2, fields=["l1", "l2"])
        # add dimuon 4-momentum field
        dileptons["z"] = dileptons.l1 + dileptons.l2
        dileptons["pt"] = dileptons.z.pt
        self.objects["dileptons"] = dileptons
