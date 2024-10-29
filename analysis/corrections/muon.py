import json
import correctionlib
import numpy as np
import awkward as ak
import dask_awkward as dak
from typing import Type
from coffea.analysis_tools import Weights
from analysis.working_points import working_points
from analysis.corrections.utils import get_pog_json
from analysis.selections.utils import trigger_match


ID_CORRECTIONS = {
    "2022preEE": {
        "loose": "NUM_LooseID_DEN_TrackerMuons",
        "medium": "NUM_MediumID_DEN_TrackerMuons",
        "tight": "NUM_TightID_DEN_TrackerMuons",
    },
    "2022postEE": {
        "loose": "NUM_LooseID_DEN_TrackerMuons",
        "medium": "NUM_MediumID_DEN_TrackerMuons",
        "tight": "NUM_TightID_DEN_TrackerMuons",
    },
}
ISO_CORRECTIONS = {
    "2022preEE": {
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
    "2022postEE": {
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
}


class MuonWeights:
    """
    Muon weights class

    Parameters:
    -----------
        events:
            events collection
        weights:
            Weights container
        year:
            Year of the dataset {2022preEE, 2022postEE}
        variation:
            syst variation
        id_wp:
            ID working point {loose, medium, tight}
        iso_wp:
            Iso working point {loose, medium, tight}

    more info at:
    https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/summaries/MUO_2022preEE_Summer22EE_muon_Z.html
    """

    def __init__(
        self,
        events: ak.Array,
        weights: Type[Weights],
        year: str,
        id_wp: str,
        iso_wp: str,
        variation: str = "nominal",
    ) -> None:
        self.events = events
        self.muons = events.Muon
        self.weights = weights
        self.year = year
        self.variation = variation
        self.id_wp = id_wp
        self.iso_wp = iso_wp

        self.muon_id_mask = working_points.muon_id(events=events, wp=id_wp)
        self.muon_iso_mask = working_points.muon_iso(events=events, wp=iso_wp)

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

    def add_trigger_weights(self, hlt_paths):
        """
        add muon iso weights to weights container
        """
        # get nominal scale factors
        nominal_weights = self.get_hlt_weights(variation="nominal", hlt_paths=hlt_paths)
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_hlt_weights(variation="systup", hlt_paths=hlt_paths)
            down_weights = self.get_hlt_weights(
                variation="systdown", hlt_paths=hlt_paths
            )
            # add nominal, up and down weights to weights container
            self.weights.add(
                name=f"muon_trigger",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            # add nominal weights to weights container
            self.weights.add(
                name=f"muon_trigger",
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
        in_muon_mask = (
            muon_pt_mask & muon_eta_mask & self.muon_id_mask & self.muon_iso_mask
        )
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

    def get_hlt_weights(self, variation, hlt_paths):
        """
        Compute muon HLT weights

        Parameters:
        -----------
            variation:
                {sf, systup, systdown}
            hlt_paths:
        """
        # get muons matched to trigger objects, pass ID and Iso wps, and within SF binning
        muon_pt_mask = self.muons.pt > 26.0
        muon_eta_mask = np.abs(self.muons.eta) < 2.4

        trig_match_mask = ak.zeros_like(self.events.PV.npvsGood, dtype="bool")
        for hlt_path in hlt_paths:
            if hlt_path in self.events.HLT.fields:
                trig_obj_mask = trigger_match(
                    leptons=self.events.Muon,
                    trigobjs=self.events.TrigObj,
                    hlt_path=hlt_path,
                )
                trig_match_mask = trig_match_mask | trig_obj_mask
        muon_trig_match_mask = dak.sum(trig_match_mask, axis=-1) > 0

        in_muons_mask = (
            muon_pt_mask
            & muon_eta_mask
            & self.muon_id_mask
            & self.muon_iso_mask
            & muon_trig_match_mask
        )
        in_muons = self.muons.mask[in_muons_mask]

        # get muons pT and abseta (replace None values with some 'in-limit' value)
        muon_pt = ak.fill_none(in_muons.pt, 26)
        muon_eta = ak.fill_none(np.abs(in_muons.eta), 0)

        sf = dak.map_partitions(
            self.cset["NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight"].evaluate,
            muon_eta,
            muon_pt,
            variation,
        )
        weights = ak.fill_none(
            ak.prod(ak.where(in_muons_mask, sf, ak.ones_like(sf)), axis=1), value=1
        )
        return weights
