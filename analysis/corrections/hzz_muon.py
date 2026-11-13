import correctionlib
import awkward as ak
from coffea.analysis_tools import Weights
from analysis.filesets.utils import get_nano_version
from analysis.corrections.utils import unflat_sf
from analysis.corrections.correctionlib_files import correction_files


class MuonHZZWeights:
    """
    Apply HZZ muon identification scale factors and uncertainties.

    Parameters
    ----------
    events:
        Pruned events.
    weights:
        Coffea Weights container.
    year:
        Dataset year.
    shift:
        Systematic shift to process.
    """

    def __init__(
        self,
        events: ak.Array,
        weights: Weights,
        year: str,
        shift: str,
    ) -> None:
        self.events = events
        self.muons = events.selected_muons
        self.weights = weights
        self.year = year
        self.shift = shift
        self.nano_version = get_nano_version(year)

        self.flat_muons = ak.flatten(self.muons)
        self.muons_counts = ak.num(self.muons)

        self.cset = correctionlib.CorrectionSet.from_file(
            correction_files["muon_hzz"][year]
        )

        self.correction = self.cset["HZZ_muon_id_sf"]

    def add_id_weights(self):

        nominal_weights = self.get_id_weights()

        if self.shift is None:
            relative_unc = self.get_id_relative_uncertainty()
            up_weights = nominal_weights * (1.0 + relative_unc)
            down_weights = nominal_weights * (1.0 - relative_unc)

            self.weights.add(
                name=f"CMS_eff_m_id_{self.year[:4]}",
                weight=nominal_weights,
                weightUp=up_weights,
                weightDown=down_weights,
            )
        else:
            self.weights.add(
                name=f"CMS_eff_m_id_{self.year[:4]}",
                weight=nominal_weights,
            )

    def get_muon_mask_and_kinematics(self):
        """Select muons within the correction acceptance."""

        muon_pt = ak.fill_none(self.flat_muons.pt, 15.0)
        muon_eta = ak.fill_none(self.flat_muons.eta, 0.0)

        in_muon_mask = (muon_pt > 3.0) & (muon_pt < 200.0) & (abs(muon_eta) < 2.399)

        # Use valid dummy values for muons outside the correction acceptance.
        muon_pt = ak.where(in_muon_mask, muon_pt, 15.0)
        muon_eta = ak.where(in_muon_mask, abs(muon_eta), 0.0)

        return in_muon_mask, muon_eta, muon_pt

    def get_id_weights(self):
        """Calculate the event-level nominal muon SF."""
        in_muon_mask, muon_eta, muon_pt = self.get_muon_mask_and_kinematics()

        muon_sfs = self.correction.evaluate(
            "nominal",
            muon_eta,
            muon_pt,
        )
        return unflat_sf(
            muon_sfs,
            in_muon_mask,
            self.muons_counts,
        )

    def get_id_relative_uncertainty(self):
        """
        Calculate the event-level relative uncertainty.

        The relative uncertainties of individual muons are added linearly,
        assuming full correlation between different muons.
        """
        in_muon_mask, muon_eta, muon_pt = self.get_muon_mask_and_kinematics()
        nominal_sfs = self.correction.evaluate(
            "nominal",
            muon_eta,
            muon_pt,
        )
        absolute_unc = self.correction.evaluate(
            "error",
            muon_eta,
            muon_pt,
        )
        # Calculate the relative uncertainty for each muon.
        relative_unc = ak.where(
            nominal_sfs != 0,
            absolute_unc / nominal_sfs,
            0.0,
        )
        # Exclude muons outside the correction acceptance.
        relative_unc = ak.where(
            in_muon_mask,
            relative_unc,
            0.0,
        )
        # Sum the relative uncertainties event-wise.
        relative_unc = ak.unflatten(
            relative_unc,
            self.muons_counts,
        )
        return ak.fill_none(
            ak.sum(relative_unc, axis=1),
            value=0.0,
        )
