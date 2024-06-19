import json
import correctionlib
import numpy as np
import awkward as ak
import dask_awkward as dak
from typing import Type
from coffea.analysis_tools import Weights
from analysis.working_points import working_points
from analysis.corrections.utils import get_pog_json


ID_CORRECTIONS = {
    "2022": {
        "loose": "NUM_LooseID_DEN_TrackerMuons",
        "medium": "NUM_MediumID_DEN_TrackerMuons",
        "tight": "NUM_TightID_DEN_TrackerMuons",
    },
    "2022EE": {
        "loose": "NUM_LooseID_DEN_TrackerMuons",
        "medium": "NUM_MediumID_DEN_TrackerMuons",
        "tight": "NUM_TightID_DEN_TrackerMuons",
    },
}
ISO_CORRECTIONS = {
    "2022": {
        "loose": {
            "loose": "NUM_LoosePFIso_DEN_LooseID",
            "medium": "NUM_LoosePFIso_DEN_MediumID",
            "tight": "NUM_LoosePFIso_DEN_TightID",
        },
        "medium": {
            "loose": None,
            "medium": None,
            "tight": None,
        },
        "tight": {
            "loose": None,
            "medium": "NUM_TightPFIso_DEN_MediumID",
            "tight": "NUM_TightPFIso_DEN_TightID",
        },
    },
    "2022EE": {
        "loose": {
            "loose": "NUM_LoosePFIso_DEN_LooseID",
            "medium": "NUM_LoosePFIso_DEN_MediumID",
            "tight": "NUM_LoosePFIso_DEN_TightID",
        },
        "medium": {
            "loose": None,
            "medium": None,
            "tight": None,
        },
        "tight": {
            "loose": None,
            "medium": "NUM_TightPFIso_DEN_MediumID",
            "tight": "NUM_TightPFIso_DEN_TightID",
        },
    }
}


class MuonWeights:
    """
    Muon weights class

    Parameters:
    -----------
        muons:
            muons collection
        weights:
            Weights container
        year:
            Year of the dataset {2022, 2022EE}
        variation:
            syst variation
        id_wp:
            ID working point {loose, medium, tight}
        iso_wp:
            Iso working point {loose, medium, tight}

    more info at:
    https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/summaries/MUO_2022_Summer22EE_muon_Z.html
    """

    def __init__(
        self,
        muons: ak.Array,
        weights: Type[Weights],
        year: str = "2022EE",
        variation: str = "nominal",
        id_wp: str = "medium",
        iso_wp: str = "loose",
    ) -> None:
        self.muons = muons
        self.weights = weights
        self.year = year
        self.variation = variation
        self.id_wp = id_wp
        self.iso_wp = iso_wp
        
        self.muon_id_mask = working_points.muon_id(muons=muons, wp=id_wp)
        self.muon_iso_mask = working_points.muon_iso(muons=muons, wp=iso_wp) 

        # get muon correction set
        self.cset = correctionlib.CorrectionSet.from_file(
            get_pog_json(json_name="muon", year=year)
        )

    def add_id_weights(self):
        """
        add muon ID weights to weights container
        """
        nominal_weights = self.get_id_weights(variation="nominal")
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_id_weights(variation="systup")
            down_weights = self.get_id_weights(variation="systdown")
            # add scale factors to weights container
            self.weights.add(
                name=f"muon_id",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name=f"muon_id",
                weight=nominal_weights,
            )

    def add_iso_weights(self):
        """
        add muon iso weights to weights container
        """
        # get nominal scale factors
        nominal_weights = self.get_iso_weights(variation="nominal")
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_iso_weights(variation="systup")
            down_weights = self.get_iso_weights(variation="systdown")
            # add nominal, up and down weights to weights container
            self.weights.add(
                name=f"muon_iso",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            # add nominal weights to weights container
            self.weights.add(
                name=f"muon_iso",
                weight=nominal_weights,
            )

    def get_id_weights(self, variation):
        """
        Compute muon ID weights
        
        Parameters:
        -----------
            variation:
                {nominal, systup, systdown}
        """
        # get muons that pass the id wp, and within SF binning
        muon_pt_mask = self.muons.pt > 15.0
        muon_eta_mask = np.abs(self.muons.eta) < 2.399
        in_muon_mask = muon_pt_mask & muon_eta_mask & self.muon_id_mask
        in_muons = self.muons.mask[in_muon_mask]

        # get muons pT and abseta (replace None values with some 'in-limit' value)
        muon_pt = ak.fill_none(in_muons.pt, 15.0)
        muon_eta = np.abs(ak.fill_none(in_muons.eta, 0.0))

        sf = dak.map_partitions(
            self.cset[ID_CORRECTIONS[self.year][self.id_wp]].evaluate,
            muon_eta,
            muon_pt,
            variation,
        )
        weights = ak.fill_none(
            ak.prod(ak.where(in_muon_mask, sf, ak.ones_like(sf)), axis=1), value=1
        )
        return weights

    def get_iso_weights(self, variation):
        """
        Compute muon iso weights
        
        Parameters:
        -----------
            variation:
                {nominal, systup, systdown}
        """
        # get 'in-limits' muons
        muon_pt_mask = self.muons.pt > 15
        muon_eta_mask = np.abs(self.muons.eta) < 2.399
        in_muon_mask = muon_pt_mask & muon_eta_mask & self.muon_id_mask & self.muon_iso_mask
        in_muons = self.muons.mask[in_muon_mask]

        # get muons pT and abseta (replace None values with some 'in-limit' value)
        muon_pt = ak.fill_none(in_muons.pt, 15)
        muon_eta = np.abs(ak.fill_none(in_muons.eta, 0.0))

        sf = dak.map_partitions(
            self.cset[ISO_CORRECTIONS[self.year][self.iso_wp][self.id_wp]].evaluate,
            muon_eta,
            muon_pt,
            variation,
        )
        weights = ak.fill_none(
            ak.prod(ak.where(in_muon_mask, sf, ak.ones_like(sf)), axis=1), value=1
        )
        return weights