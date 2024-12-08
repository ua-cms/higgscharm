import correctionlib
import numpy as np
import awkward as ak
import correctionlib.schemav2 as cs
from typing import Type
from coffea.analysis_tools import Weights
from analysis.corrections.met import update_met
from analysis.selections.trigger import trigger_match
from analysis.corrections.utils import get_pog_json, unflat_sf
from analysis.selections.event_selections import get_trigger_mask


class ElectronWeights:
    """
    Electron ID, Reco and HLT weights class

    Parameters:
    -----------
        events:
            events collection
        weights:
            Weights container
        year:
            Year of the dataset {2022postEE, 2022preEE}
        variation:
            syst variation
        id_wp:
            ID working point {'wpiso80', 'wpiso90'}

    more info: https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_factors_and_correction_AN1
    """

    def __init__(
        self,
        events: ak.Array,
        weights: Type[Weights],
        year: str = "2022postEE",
        variation: str = "nominal",
        id_wp: str = "wp80iso",
    ) -> None:
        self.events = events
        self.electrons = events.Electron
        self.weights = weights
        self.year = year
        self.variation = variation
        self.id_wp = id_wp

        self.flat_electrons = ak.flatten(events.Electron)
        self.electrons_counts = ak.num(events.Electron)

        # set id working points
        self.id_wps = {
            "wp80iso": self.flat_electrons.mvaIso_WP80,
            "wp90iso": self.flat_electrons.mvaIso_WP90,
        }
        self.year_map = {
            "2022postEE": "2022Re-recoE+PromptFG",
            "2022preEE": "2022Re-recoBCD",
        }

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

    def add_hlt_weights(self, hlt_paths, dataset_key):
        """
        add electron HLT weights to weights container
        """
        nominal_weights = self.get_hlt_weights(
            variation="sf", hlt_paths=hlt_paths, dataset_key=dataset_key
        )
        if self.variation == "nominal":
            # get 'up' and 'down' weights
            up_weights = self.get_hlt_weights(
                variation="sfup", hlt_paths=hlt_paths, dataset_key=dataset_key
            )
            down_weights = self.get_hlt_weights(
                variation="sfdown", hlt_paths=hlt_paths, dataset_key=dataset_key
            )
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
        electron_pt_mask = self.flat_electrons.pt > 10.0
        electron_id_mask = self.id_wps[self.id_wp]
        in_electron_mask = electron_pt_mask & electron_id_mask
        in_electrons = self.flat_electrons.mask[in_electron_mask]

        # get electrons pT and abseta (replace None values with some 'in-limit' value)
        electron_pt = ak.fill_none(in_electrons.pt, 15.0)
        electron_eta = ak.fill_none(in_electrons.eta, 0)

        weights = unflat_sf(
            cset["Electron-ID-SF"].evaluate(
                self.year_map[self.year],
                variation,
                self.id_wp,
                electron_eta,
                electron_pt,
            ),
            in_electron_mask,
            self.electrons_counts,
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
            "RecoBelow20": (self.flat_electrons.pt > 10.0)
            & (self.flat_electrons.pt < 20.0),
            "Reco20to75": (self.flat_electrons.pt > 20.0)
            & (self.flat_electrons.pt < 75.0),
            "RecoAbove75": self.flat_electrons.pt > 75,
        }
        in_electrons_mask = electron_pt_mask[reco_range]
        in_electrons = self.flat_electrons.mask[in_electrons_mask]

        # get electrons pT and abseta (replace None values with some 'in-limit' value)
        electron_pt_limits = {
            "RecoBelow20": 15,
            "Reco20to75": 30,
            "RecoAbove75": 80,
        }
        electron_pt = ak.fill_none(in_electrons.pt, electron_pt_limits[reco_range])
        electron_eta = ak.fill_none(in_electrons.eta, 0)

        weights = unflat_sf(
            cset["Electron-ID-SF"].evaluate(
                self.year_map[self.year],
                variation,
                reco_range,
                electron_eta,
                electron_pt,
            ),
            in_electrons_mask,
            self.electrons_counts,
        )
        return weights

    def get_hlt_weights(self, variation, hlt_paths, dataset_key):
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
        # get trigger masks
        trigger_mask = get_trigger_mask(self.events, hlt_paths, dataset_key)
        trigger_mask = ak.flatten(ak.ones_like(self.electrons.pt) * trigger_mask) > 0
        """
        trigger_match_mask = np.zeros(len(self.events), dtype="bool")
        for hlt_path in hlt_paths:
            if hlt_path in self.events.HLT.fields:
                trig_obj_mask = trigger_match(
                    leptons=self.electrons,
                    trigobjs=self.events.TrigObj,
                    hlt_path=hlt_path,
                )
                trigger_match_mask = trigger_match_mask | trig_obj_mask
        trigger_match_mask = ak.flatten(trigger_match_mask)
        """
        # get electrons that pass the id wp, and within SF binning
        electron_pt_mask = self.flat_electrons.pt > 25.0
        in_electrons_mask = electron_pt_mask & trigger_mask  # & trigger_match_mask
        in_electrons = self.flat_electrons.mask[in_electrons_mask]

        # get electrons pT and abseta (replace None values with some 'in-limit' value)
        electron_pt = ak.fill_none(in_electrons.pt, 25)
        electron_eta = ak.fill_none(in_electrons.eta, 0)

        hlt_path_id_map = {
            "wp80iso": "HLT_SF_Ele30_MVAiso80ID",
            "wp90iso": "HLT_SF_Ele30_MVAiso90ID",
        }
        weights = unflat_sf(
            cset["Electron-HLT-SF"].evaluate(
                self.year_map[self.year],
                variation,
                hlt_path_id_map[self.id_wp],
                electron_eta,
                electron_pt,
            ),
            in_electrons_mask,
            self.electrons_counts,
        )
        return weights


class ElectronSS:
    """
    Electron Scale and Smearing (energy scale and resolution) corrector

    more info: https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_factors_and_correction_AN1

    coffea0.7 example: https://gitlab.cern.ch/cms-analysis/general/HiggsDNA/-/blob/master/higgs_dna/systematics/electron_systematics.py?ref_type=heads
    """

    def __init__(
        self,
        events: ak.Array,
        year: str,
        variation: str = "nominal",
    ) -> None:
        self.events = events
        self.year = year
        self.variation = variation
        self.flat_electrons = ak.flatten(events.Electron)
        self.electrons_counts = ak.num(events.Electron)
        # get correction set
        self.cset = correctionlib.CorrectionSet.from_file(
            get_pog_json(json_name="electron_scale", year=self.year)
        )

    def apply_scale(self):
        """
        from https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#Scale_factors_and_correction_AN1:

             'Note that we deal with pt instead of energy in the code below. When dealing with nanoAOD in a columnar format,
             this makes sense as pt is an actual field of the photon and electron collections. Scales and smearings are equal
             for pt and energy as they are directly linearly proportional. Changing the pt also automatically changes the energy
             when loading the nanoAOD in the framework below due to Lorentzvector behaviours'.

        """
        # define Electron pt_raw field (needed for MET recalculation)
        self.events["Electron", "pt_raw"] = (
            ak.ones_like(self.events.Electron.pt) * self.events.Electron.pt
        )
        # get correction input variables
        gain = self.flat_electrons.seedGain
        run = np.repeat(self.events.run, self.electrons_counts)
        etasc = self.flat_electrons.eta + self.flat_electrons.deltaEtaSC
        r9 = self.flat_electrons.r9
        pt = self.flat_electrons.pt
        # compute scale factor
        scale = self.cset["Scale"].evaluate(
            "total_correction",
            gain,
            run,
            etasc,
            r9,
            pt,
        )
        if self.variation == "nominal":
            # scale is multiplicative correction, unlike smearing, it is deterministic
            self.events["Electron", "pt"] = ak.unflatten(
                self.flat_electrons.pt * scale, self.electrons_counts
            )
            # propagate electron pT corrections to MET
            update_met(events=self.events, lepton="Electron")
        else:
            # uncertainties: TO DO (https://cms-talk.web.cern.ch/t/pnoton-energy-corrections-in-nanoaod-v11/34327/2)
            pass

    def apply_smearing(self, seed=42):
        # define Electron pt_raw field (needed for MET recalculation)
        self.events["Electron", "pt_raw"] = (
            ak.ones_like(self.events.Electron.pt) * self.events.Electron.pt
        )
        # get correction input variables
        etasc = self.flat_electrons.eta + self.flat_electrons.deltaEtaSC
        r9 = self.flat_electrons.r9

        # rho does not correspond to the pileup rho energy density,
        # instead it is the standard deviation of the Gaussian used to draw the smearing
        rho = self.cset["Smearing"].evaluate(
            "rho",
            etasc,
            r9,
        )
        # The smearing is done statistically, so we need some random numbers
        if self.variation == "nominal":
            rng = np.random.default_rng(seed=seed)
            smearing = rng.normal(loc=1.0, scale=rho)
            self.events["Electron", "pt"] = ak.unflatten(
                self.flat_electrons.pt * smearing, self.electrons_counts
            )
            # propagate electron pT corrections to MET
            update_met(events=self.events, lepton="Electron")
        else:
            # uncertainties: TO DO
            pass
