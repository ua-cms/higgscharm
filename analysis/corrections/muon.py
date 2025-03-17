import json
import correctionlib
import numpy as np
import awkward as ak
from typing import Type
from coffea.analysis_tools import Weights
from analysis.working_points import working_points
from analysis.selections.trigger import trigger_match_mask
from analysis.corrections.utils import get_pog_json, unflat_sf
from analysis.selections.event_selections import get_trigger_mask


class MuonWeights:
    """
    Muon weights class

    Parameters:
    -----------
        events:
            pruned events
        weights:
            Weights container
        year:
            Year of the dataset {2022postEE, 2022preEE, 2023preBPix, 2023postBPix}
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
        variation: str = "nominal",
    ) -> None:
        self.events = events
        self.muons = events.selected_muons
        self.weights = weights
        self.year = year
        self.variation = variation

        self.flat_muons = ak.flatten(self.muons)
        self.muons_counts = ak.num(self.muons)

        # get muon correction set
        self.cset = correctionlib.CorrectionSet.from_file(
            get_pog_json(json_name="muon", year=year)
        )

    def add_id_weights(self, id_wp):
        """
        add muon ID weights to weights container
        """
        nominal_weights = self.get_id_weights(id_wp, variation="nominal")
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_id_weights(id_wp, variation="systup")
            down_weights = self.get_id_weights(id_wp, variation="systdown")
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

    def add_iso_weights(self, id_wp, iso_wp):
        """
        add muon iso weights to weights container
        """
        # get nominal scale factors
        nominal_weights = self.get_iso_weights(id_wp, iso_wp, variation="nominal")
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_iso_weights(id_wp, iso_wp, variation="systup")
            down_weights = self.get_iso_weights(id_wp, iso_wp, variation="systdown")
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

    def add_trigger_weights(self, id_wp, iso_wp, hlt_paths, dataset):
        """
        add muon iso weights to weights container
        """
        # get nominal scale factors
        nominal_weights = self.get_hlt_weights(
            id_wp=id_wp,
            iso_wp=iso_wp,
            variation="nominal",
            hlt_paths=hlt_paths,
            dataset=dataset,
        )
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_hlt_weights(
                id_wp=id_wp,
                iso_wp=iso_wp,
                variation="systup",
                hlt_paths=hlt_paths,
                dataset=dataset,
            )
            down_weights = self.get_hlt_weights(
                id_wp=id_wp,
                iso_wp=iso_wp,
                variation="systdown",
                hlt_paths=hlt_paths,
                dataset=dataset,
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

    def get_id_weights(self, id_wp, variation):
        """
        Compute muon ID weights

        Parameters:
        -----------
            variation:
                {nominal, systup, systdown}
        """
        id_corrections = {
            "loose": "NUM_LooseID_DEN_TrackerMuons",
            "medium": "NUM_MediumID_DEN_TrackerMuons",
            "tight": "NUM_TightID_DEN_TrackerMuons",
        }
        # get muons that pass the id wp, and within SF binning
        muon_pt_mask = self.flat_muons.pt > 15.0
        muon_eta_mask = np.abs(self.flat_muons.eta) < 2.399
        in_muon_mask = muon_pt_mask & muon_eta_mask
        in_muons = self.flat_muons.mask[in_muon_mask]

        # get muons pT and abseta (replace None values with some 'in-limit' value)
        muon_pt = ak.fill_none(in_muons.pt, 15.0)
        muon_eta = np.abs(ak.fill_none(in_muons.eta, 0.0))

        weights = unflat_sf(
            self.cset[id_corrections[id_wp]].evaluate(
                muon_eta,
                muon_pt,
                variation,
            ),
            in_muon_mask,
            self.muons_counts,
        )
        return weights

    def get_iso_weights(self, id_wp, iso_wp, variation):
        """
        Compute muon iso weights

        Parameters:
        -----------
            variation:
                {nominal, systup, systdown}
        """
        iso_corrections = {
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
        # get 'in-limits' muons
        muon_pt_mask = self.flat_muons.pt > 15
        muon_eta_mask = np.abs(self.flat_muons.eta) < 2.399
        in_muon_mask = muon_pt_mask & muon_eta_mask
        in_muons = self.flat_muons.mask[in_muon_mask]

        # get muons pT and abseta (replace None values with some 'in-limit' value)
        muon_pt = ak.fill_none(in_muons.pt, 15)
        muon_eta = np.abs(ak.fill_none(in_muons.eta, 0.0))

        weights = unflat_sf(
            self.cset[iso_corrections[iso_wp][id_wp]].evaluate(
                muon_eta,
                muon_pt,
                variation,
            ),
            in_muon_mask,
            self.muons_counts,
        )
        return weights

    def get_hlt_weights(self, id_wp, iso_wp, hlt_paths, variation, dataset):
        """
        Compute muon HLT weights

        Parameters:
        -----------
            variation:
                {sf, systup, systdown}
            hlt_paths:
        """
        muon_pt_mask = self.flat_muons.pt > 26.0
        muon_eta_mask = np.abs(self.flat_muons.eta) < 2.4
        # get trigger match mask
        trigger_match = trigger_match_mask(
            events=self.events, leptons=self.muons, hlt_paths=hlt_paths, year=self.year
        )
        trigger_mask = ak.flatten(trigger_match)
        # get muons passing ID and Iso wps, trigger, and within SF binning
        in_muons_mask = muon_pt_mask & muon_eta_mask & trigger_mask
        in_muons = self.flat_muons.mask[in_muons_mask]
        # get muons pT and abseta (replace None values with some 'in-limit' value)
        muon_pt = ak.fill_none(in_muons.pt, 26)
        muon_eta = ak.fill_none(np.abs(in_muons.eta), 0)
        hlt_path_id_map = {
            ("tight", "tight"): "NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight",
            ("medium", "medium"): "NUM_IsoMu24_DEN_CutBasedIdMedium_and_PFIsoMedium",
        }
        assert (
            id_wp,
            iso_wp,
        ) in hlt_path_id_map, (
            f"There's no HLT correction for (ID, ISO) wps pair {(id_wp, iso_wp)}"
        )
        weights = unflat_sf(
            self.cset[hlt_path_id_map[(id_wp, iso_wp)]].evaluate(
                muon_eta,
                muon_pt,
                variation,
            ),
            in_muons_mask,
            self.muons_counts,
        )
        return weights
