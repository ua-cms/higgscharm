import correctionlib
import awkward as ak
from coffea.analysis_tools import Weights

from analysis.corrections.utils import unflat_sf
from analysis.corrections.correctionlib_files import correction_files


class ElectronHZZWeights:
    """
    Apply HZZ electron identification and reconstruction scale factors.

    Parameters
    ----------
    events:
        Pruned events.
    weights:
        Coffea Weights container.
    year:
        Dataset year.
    shift:
        Systematic shift to process. If None, nominal weights and
        their up/down variations are added.
    """

    def __init__(
        self,
        events: ak.Array,
        weights: Weights,
        year: str,
        shift: str,
    ) -> None:
        self.events = events
        self.electrons = events.selected_electrons
        self.weights = weights
        self.year = year
        self.shift = shift

        self.flat_electrons = ak.flatten(self.electrons)
        self.electrons_counts = ak.num(self.electrons)

        self.cset = correctionlib.CorrectionSet.from_file(
            correction_files["electron_hzz"][year]
        )
        self.correction = self.cset[f"electron_sf"]

        # Electron kinematics used by the original LeptonSFHelper.
        self.electron_pt = ak.fill_none(self.flat_electrons.pt, 15.0)
        self.electron_eta = ak.fill_none(
            self.flat_electrons.eta + self.flat_electrons.deltaEtaSC,
            0.0,
        )
        self.electron_phi = ak.fill_none(self.flat_electrons.phi, 0.0)

        self.is_2022 = year.startswith("2022")
        self.is_2023 = year.startswith("2023")
        self.is_post_bpix = year == "2023postBPix"
        self.is_2024 = year == "2024"

    def _get_event_relative_uncertainty(self, relative_unc):
        """
        Sum the relative uncertainties linearly across electrons.

        This follows the full-correlation assumption in LeptonSFHelper.
        """
        relative_unc = ak.unflatten(
            relative_unc,
            self.electrons_counts,
        )

        return ak.fill_none(
            ak.sum(relative_unc, axis=1),
            value=0.0,
        )

    def _get_event_weights(self, electron_sf, in_electrons_mask):
        """Calculate the event-level product of electron SFs."""
        return unflat_sf(
            electron_sf,
            in_electrons_mask,
            self.electrons_counts,
        )

    def _get_shifted_weights(self, nominal_weights, relative_unc, syst):
        """Apply an event-level relative uncertainty."""
        if syst == "nominal":
            return nominal_weights

        if syst == "up":
            return nominal_weights * (1.0 + relative_unc)

        if syst == "down":
            return nominal_weights * (1.0 - relative_unc)

        raise ValueError(
            f"Unknown systematic variation '{syst}'. "
            "Expected 'nominal', 'up', or 'down'."
        )

    def _evaluate(self, correction_name, variation, eta, pt):
        """Evaluate one correction category."""
        return self.correction.evaluate(
            eta,
            pt,
            correction_name,
            variation,
        )

    def get_id_weights(self, syst):
        """
        Calculate event-level electron ID scale factors.

        The ID correction depends on the campaign and electron region:
        - Standard ID SF for all campaigns.
        - Gap ID SF for 2023 electrons with pT < 20 GeV.
        - BPix-hole ID SF for the corresponding 2023 postBPix region.

        The gap uncertainty is doubled for 2023 postBPix.
        """
        electron_pt = ak.where(
            self.electron_pt > 499.0,
            499.0,
            self.electron_pt,
        )

        electron_eta = self.electron_eta

        # Start with the standard ID correction.
        nominal_sf = self._evaluate(
            "id",
            "nominal",
            electron_eta,
            electron_pt,
        )
        absolute_unc = self._evaluate(
            "id",
            "error",
            electron_eta,
            electron_pt,
        )

        # Identify the gap region. The dedicated gap SF is only used
        # for electrons with pT < 20 GeV.
        is_gap = (
            (abs(electron_eta) > 1.444)
            & (abs(electron_eta) < 1.556)
            & (self.electron_pt < 20.0)
        )

        if self.is_2023 or self.is_2024:
            gap_sf = self._evaluate(
                "id_gap",
                "nominal",
                electron_eta,
                electron_pt,
            )
            gap_unc = self._evaluate(
                "id_gap",
                "error",
                electron_eta,
                electron_pt,
            )

            nominal_sf = ak.where(
                is_gap,
                gap_sf,
                nominal_sf,
            )
            absolute_unc = ak.where(
                is_gap,
                gap_unc,
                absolute_unc,
            )

            # The original helper doubles the gap uncertainty in 2023 postBPix.
            if self.is_post_bpix:
                absolute_unc = ak.where(
                    is_gap,
                    2.0 * absolute_unc,
                    absolute_unc,
                )

        # Identify the BPix-hole region. The original implementation
        # uses this correction only when the gap correction is not applied.
        if self.is_post_bpix:
            is_hole = (
                (self.electron_eta > -1.5)
                & (self.electron_eta < 0.0)
                & (self.electron_phi > -1.2)
                & (self.electron_phi < -0.8)
                & ~is_gap
            )

            hole_sf = self._evaluate(
                "id_hole",
                "nominal",
                electron_eta,
                electron_pt,
            )
            hole_unc = self._evaluate(
                "id_hole",
                "error",
                electron_eta,
                electron_pt,
            )

            nominal_sf = ak.where(
                is_hole,
                hole_sf,
                nominal_sf,
            )
            absolute_unc = ak.where(
                is_hole,
                hole_unc,
                absolute_unc,
            )

        # Calculate the relative uncertainty for each electron.
        relative_unc = ak.where(
            nominal_sf != 0,
            absolute_unc / nominal_sf,
            0.0,
        )

        # Calculate the event-level nominal SF and relative uncertainty.
        in_electrons_mask = ak.ones_like(
            self.electron_pt,
            dtype=bool,
        )

        event_nominal_sf = self._get_event_weights(
            nominal_sf,
            in_electrons_mask,
        )
        event_relative_unc = self._get_event_relative_uncertainty(
            relative_unc,
        )

        return self._get_shifted_weights(
            event_nominal_sf,
            event_relative_unc,
            syst,
        )

    def get_reco_weights(self, syst, reco_range):
        """
        Calculate event-level electron reconstruction scale factors.

        Parameters
        ----------
        syst:
            Systematic variation: 'nominal', 'up', or 'down'.
        reco_range:
            Reconstruction pT range: 'low', 'mid', or 'high'.
        """
        reco_ranges = {
            "low": {
                "category": "reco_low",
                "mask": self.electron_pt < 20.0,
                "lookup_pt": 15.0,
            },
            "mid": {
                "category": "reco_mid",
                "mask": ((self.electron_pt >= 20.0) & (self.electron_pt < 75.0)),
                "lookup_pt": None,
            },
            "high": {
                "category": "reco_high",
                "mask": self.electron_pt >= 75.0,
                "lookup_pt": 499.0,
            },
        }

        if reco_range not in reco_ranges:
            raise ValueError(
                f"Unknown reconstruction range '{reco_range}'. "
                "Expected 'low', 'mid', or 'high'."
            )

        reco_info = reco_ranges[reco_range]
        correction_name = reco_info["category"]
        in_electrons_mask = reco_info["mask"]

        # Use the lookup pT prescribed by the original helper.
        if reco_range == "low":
            lookup_pt = ak.ones_like(self.electron_pt) * 15.0

        elif reco_range == "mid":
            lookup_pt = ak.where(
                self.electron_pt > 75.0,
                75.0,
                self.electron_pt,
            )

        else:
            lookup_pt = ak.where(
                self.electron_pt > 499.0,
                499.0,
                self.electron_pt,
            )

        # The 2022 low-pT reconstruction SF is symmetrised in eta.
        lookup_eta = (
            abs(self.electron_eta)
            if self.is_2022 and reco_range == "low"
            else self.electron_eta
        )

        nominal_sf = self._evaluate(
            correction_name,
            "nominal",
            lookup_eta,
            lookup_pt,
        )
        absolute_unc = self._evaluate(
            correction_name,
            "error",
            lookup_eta,
            lookup_pt,
        )

        # Electrons outside the requested pT range do not contribute
        # to this reconstruction correction.
        nominal_sf = ak.where(
            in_electrons_mask,
            nominal_sf,
            1.0,
        )
        absolute_unc = ak.where(
            in_electrons_mask,
            absolute_unc,
            0.0,
        )

        # Calculate the relative uncertainty for each electron.
        relative_unc = ak.where(
            nominal_sf != 0,
            absolute_unc / nominal_sf,
            0.0,
        )

        # Calculate the event-level nominal SF and relative uncertainty.
        event_nominal_sf = self._get_event_weights(
            nominal_sf,
            ak.ones_like(in_electrons_mask, dtype=bool),
        )
        event_relative_unc = self._get_event_relative_uncertainty(
            relative_unc,
        )

        return self._get_shifted_weights(
            event_nominal_sf,
            event_relative_unc,
            syst,
        )

    def add_id_weights(self):
        """Add electron ID scale factors to the Coffea Weights container."""

        nominal_weights = self.get_id_weights("nominal")

        if self.shift is None:
            up_weights = self.get_id_weights("up")
            down_weights = self.get_id_weights("down")

            self.weights.add(
                name=f"CMS_eff_e_id_{self.year[:4]}",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name=f"CMS_eff_e_id_{self.year[:4]}",
                weight=nominal_weights,
            )

    def add_reco_weights(self, reco_range):
        """Add electron reconstruction scale factors to the Weights container."""

        nominal_weights = self.get_reco_weights(
            "nominal",
            reco_range,
        )

        if self.shift is None:
            up_weights = self.get_reco_weights(
                "up",
                reco_range,
            )
            down_weights = self.get_reco_weights(
                "down",
                reco_range,
            )

            self.weights.add(
                name=f"CMS_eff_e_reco_{reco_range}_{self.year[:4]}",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name=f"CMS_eff_e_reco_{reco_range}_{self.year[:4]}",
                weight=nominal_weights,
            )
