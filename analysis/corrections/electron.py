import json
import correctionlib
import numpy as np
import awkward as ak
import dask_awkward as dak
from typing import Type
from coffea.analysis_tools import Weights
from analysis.corrections.utils import get_pog_json


class ElectronWeights:
    """
    Electron weights class

    Parameters:
    -----------
        electrons:
            electrons collection
        weights:
            Weights container
        year:
            Year of the dataset {2022EE, 2022}
        variation:
            syst variation
        id_wp:
            ID working point {'wpiso80', 'wpiso90'}

    more info: https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_factors_and_correction_AN1
    """

    def __init__(
        self,
        electrons: ak.Array,
        weights: Type[Weights],
        year: str = "2022EE",
        variation: str = "nominal",
        id_wp: str = "medium",
    ) -> None:
        self.electrons = electrons
        self.weights = weights
        self.year = year
        self.variation = variation
        self.id_wp = id_wp
        # set id working points
        self.id_wps = {
            "wp80iso": electrons.mvaIso_WP80,
            "wp90iso": electrons.mvaIso_WP90,
        }
        self.year_map = {"2022EE": "2022Re-recoE+PromptFG", "2022": "2022Re-recoBCD"}

    def add_id_weights(self):
        """
        add electron ID weights to weights container
        """
        nominal_weights = self.get_id_weights(variation="sf")
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_id_weights(variation="sfup")
            down_weights = self.get_id_weights(variation="sfdown")
            # add scale factors to weights container
            self.weights.add(
                name=f"electron_id",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name=f"electron_id",
                weight=nominal_weights,
            )

    def add_reco_weights(self, reco_range):
        """
        add electron ID weights to weights container
        """
        nominal_weights = self.get_reco_weights(variation="sf", reco_range=reco_range)
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_reco_weights(variation="sfup", reco_range=reco_range)
            down_weights = self.get_reco_weights(
                variation="sfdown", reco_range=reco_range
            )
            # add scale factors to weights container
            self.weights.add(
                name=f"electron_reco_{reco_range}",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name=f"electron_reco_{reco_range}",
                weight=nominal_weights,
            )

    def add_hlt_weights(self):
        """
        add electron HLT weights to weights container
        """
        nominal_weights = self.get_hlt_weights(variation="sf")
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_hlt_weights(variation="sfup")
            down_weights = self.get_hlt_weights(variation="sfdown")
            # add scale factors to weights container
            self.weights.add(
                name="electron_hlt",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name="electron_hlt",
                weight=nominal_weights,
            )

    def get_id_weights(self, variation):
        """
        Compute electron ID weights

        Parameters:
        -----------
            variation:
                {sf, sfdown, sfup}
        """
        # get electron correction set
        cset = correctionlib.CorrectionSet.from_file(
            get_pog_json(json_name="electron_id", year=self.year)
        )

        # get electrons that pass the id wp, and within SF binning
        electron_pt_mask = self.electrons.pt > 10.0
        electron_id_mask = self.id_wps[self.id_wp]
        in_electron_mask = electron_pt_mask & electron_id_mask
        in_electrons = self.electrons.mask[in_electron_mask]

        # get electrons pT and abseta (replace None values with some 'in-limit' value)
        electron_pt = ak.fill_none(in_electrons.pt, 15.0)
        electron_eta = in_electrons.eta

        sf = dak.map_partitions(
            cset["Electron-ID-SF"].evaluate,
            self.year_map[self.year],
            variation,
            self.id_wp,
            electron_eta,
            electron_pt,
        )
        weights = ak.fill_none(
            ak.prod(ak.where(in_electron_mask, sf, ak.ones_like(sf)), axis=1), value=1
        )
        return weights

    def get_reco_weights(self, variation, reco_range):
        """
        Compute electron Reco weights

        Parameters:
        -----------
            variation:
                {sf, sfdown, sfup}
        """
        # get electron correction set
        cset = correctionlib.CorrectionSet.from_file(
            get_pog_json(json_name="electron_id", year=self.year)
        )
        # get electrons that pass the id wp, and within SF binning
        electron_pt_mask = {
            "RecoBelow20": (self.electrons.pt > 10.0) & (self.electrons.pt < 20.0),
            "Reco20to75": (self.electrons.pt > 20.0) & (self.electrons.pt < 75.0),
            "RecoAbove75": self.electrons.pt > 75,
        }
        in_electrons = self.electrons.mask[electron_pt_mask[reco_range]]

        # get electrons pT and abseta (replace None values with some 'in-limit' value)
        electron_pt_limits = {
            "RecoBelow20": 15,
            "Reco20to75": 30,
            "RecoAbove75": 80,
        }
        electron_pt = ak.fill_none(in_electrons.pt, electron_pt_limits[reco_range])
        electron_eta = in_electrons.eta

        sf = dak.map_partitions(
            cset["Electron-ID-SF"].evaluate,
            self.year_map[self.year],
            variation,
            reco_range,
            electron_eta,
            electron_pt,
        )
        weights = ak.fill_none(
            ak.prod(
                ak.where(electron_pt_mask[reco_range], sf, ak.ones_like(sf)), axis=1
            ),
            value=1,
        )
        return weights

    def get_hlt_weights(self, variation):
        """
        Compute electron HLT weights

        Parameters:
        -----------
            variation:
                {sf, sfdown, sfup}
        """
        # get electron correction set
        cset = correctionlib.CorrectionSet.from_file(
            get_pog_json(json_name="electron_hlt", year=self.year)
        )
        # get electrons that pass the id wp, and within SF binning
        electron_pt_mask = self.electrons.pt > 25.0
        in_electrons = self.electrons.mask[electron_pt_mask]

        # get electrons pT and abseta (replace None values with some 'in-limit' value)
        electron_pt = ak.fill_none(in_electrons.pt, 25)
        electron_eta = in_electrons.eta

        hlt_path_id_map = {
            "wp80iso": "HLT_SF_Ele30_MVAiso80ID",
            "wp90iso": "HLT_SF_Ele30_MVAiso90ID",
        }
        sf = dak.map_partitions(
            cset["Electron-HLT-SF"].evaluate,
            self.year_map[self.year],
            variation,
            hlt_path_id_map[self.id_wp],
            electron_eta,
            electron_pt,
        )
        weights = ak.fill_none(
            ak.prod(ak.where(electron_pt_mask, sf, ak.ones_like(sf)), axis=1), value=1
        )
        return weights