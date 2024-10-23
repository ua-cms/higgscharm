import correctionlib
import numpy as np
import awkward as ak
import dask_awkward as dak
import correctionlib.schemav2 as cs
from typing import Type
from coffea.analysis_tools import Weights
from analysis.corrections.utils import get_pog_json
from analysis.utils.trigger_matching import trigger_match


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
            Year of the dataset {2022EE, 2022}
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
        year: str = "2022EE",
        variation: str = "nominal",
        id_wp: str = "medium",
        hlt_paths: str = ["Ele30_WPTight_Gsf"],
    ) -> None:
        self.events = events
        self.electrons = events.Electron
        self.weights = weights
        self.year = year
        self.variation = variation
        self.id_wp = id_wp
        self.hlt_paths = hlt_paths
        # set id working points
        self.id_wps = {
            "wp80iso": self.electrons.mvaIso_WP80,
            "wp90iso": self.electrons.mvaIso_WP90,
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
        # get electrons matched to trigger objects
        trig_match_mask = ak.zeros_like(self.events.PV.npvsGood, dtype="bool")
        for hlt_path in self.hlt_paths:
            if hlt_path in self.events.HLT.fields:
                trig_obj_mask = trigger_match(
                    leptons=self.electrons,
                    trigobjs=self.events.TrigObj,
                    hlt_path=hlt_path,
                )
                trig_match_mask = trig_match_mask | trig_obj_mask
        # get electrons that pass the id wp, and within SF binning
        electron_pt_mask = self.electrons.pt > 25.0
        in_electrons = self.electrons.mask[
            electron_pt_mask & (dak.sum(trig_match_mask, axis=-1) > 0)
        ]

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
        scale = dak.map_partitions(
            self.cset["Scale"].evaluate,
            "total_correction",
            self.events.Electron.seedGain,
            self.events.run,
            self.events.Electron.eta + self.events.Electron.deltaEtaSC,
            self.events.Electron.r9,
            self.events.Electron.pt,
        )
        if self.variation == "nominal":
            # scale is multiplicative correction, unlike smearing, it is deterministic
            self.events["Electron", "pt"] = self.events.Electron.pt * scale

        # uncertainties: TO DO (https://cms-talk.web.cern.ch/t/pnoton-energy-corrections-in-nanoaod-v11/34327/2)

    def apply_smearing(self):
        # rho does not correspond to the pileup rho energy density,
        # instead it is the standard deviation of the Gaussian used to draw the smearing
        rho = dak.map_partitions(
            self.cset["Smearing"].evaluate,
            "rho",
            self.events.Electron.eta + self.events.Electron.deltaEtaSC,
            self.events.Electron.r9,
        )
        # The smearing is done statistically, so we need some random numbers
        # https://cms-nanoaod.github.io/correctionlib/schemav2.html#hashprng
        # https://cms-nanoaod.github.io/correctionlib/correctionlib_tutorial.html#Resolution-models
        rng = cs.Correction(
            name="resrng",
            description="Deterministic smearing value generator",
            version=1,
            inputs=[
                cs.Variable(name="rho", type="real", description="standard deviation"),
            ],
            output=cs.Variable(name="rng", type="real"),
            data=cs.HashPRNG(
                nodetype="hashprng",
                inputs=["rho"],
                distribution="normal",
            ),
        )
        smearing = rng.to_evaluator().evaluate(rho)
        if self.variation == "nominal":
            self.events["Electron", "pt"] = self.events.Electron.pt * smearing

        # uncertainties: TO DO