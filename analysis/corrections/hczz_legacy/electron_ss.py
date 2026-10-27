import correctionlib
import numpy as np
import awkward as ak
from pathlib import Path
from analysis.corrections.met import update_met
from analysis.corrections.utils import correction_files


def filter_boundaries(pt_corr, pt, nested=True):
    if not nested:
        pt_corr = np.asarray(pt_corr)
        pt = np.asarray(pt)

    # Check for pt values outside the range
    outside_bounds = (pt < 20) | (pt > 250)

    if nested:
        n_pt_outside = ak.sum(ak.any(outside_bounds, axis=-1))
    else:
        n_pt_outside = np.sum(outside_bounds)

    if n_pt_outside > 0:
        print(
            f"There are {n_pt_outside} events with muon pt outside of [26,200] GeV. "
            "Setting those entries to their initial value."
        )
        pt_corr = np.where(pt > 250, pt, pt_corr)
        pt_corr = np.where(pt < 20, pt, pt_corr)

    # Check for NaN entries in pt_corr
    nan_entries = np.isnan(pt_corr)

    if nested:
        n_nan = ak.sum(ak.any(nan_entries, axis=-1))
    else:
        n_nan = np.sum(nan_entries)

    if n_nan > 0:
        print(
            f"There are {n_nan} nan entries in the corrected pt. "
            "This might be due to the number of tracker layers hitting boundaries. "
            "Setting those entries to their initial value."
        )
        pt_corr = np.where(np.isnan(pt_corr), pt, pt_corr)

    return pt_corr


def apply_electron_ss_corrections(
    events: ak.Array,
    year: str,
    variation: str = "nominal",
):
    """
    Apply electron scale and smearing corrections for Run3

    from docs https://egammapog.docs.cern.ch/Run3/SaS/

    The purpose of scale and smearing (more formal: energy scale and resolution corrections) is to correct and calibrate electron and photon energies in data and MC. This step is performed after the MC-based semi-parametric EGamma energy regression, which is applied to both MC and data (aiming to correct for inherent imperfections that are not in principle related to data/MC differences like crystal-by-crystal differences, intermodule gaps, ...). The remaining differences between the electron and photon energy scales and the resolution in the data and simulation need to be corrected for. To address this, a multistep procedure is implemented to calibrate the residual energy scale in data. Additionally, an extra smearing is applied to the electron or photon energy in simulation to ensure that the energy resolution matches that observed in data
    """
    cset = correctionlib.CorrectionSet.from_file(correction_files["electron_ss"][year])

    events["Electron", "pt_raw"] = ak.ones_like(events.Electron.pt) * events.Electron.pt
    electrons = ak.flatten(events.Electron)
    counts = ak.num(events.Electron)
    seedgain = electrons.seedGain
    run = np.repeat(events.run, counts)
    sceta = electrons.eta + electrons.deltaEtaSC
    r9 = electrons.r9
    pt = electrons.pt_raw

    if variation == "nominal":
        if hasattr(events, "genWeight"):
            smear = cset["SmearAndSyst"].evaluate("smear", pt, r9, sceta)
            rng = np.random.default_rng(seed=42)
            random_numbers = rng.normal(loc=0.0, scale=1.0, size=len(pt))
            correction_factor = 1 + smear * random_numbers
        else:
            correction_factor = cset.compound["Scale"].evaluate(
                "scale", run, sceta, r9, pt, seedgain
            )

        ele_pt = ak.unflatten(electrons.pt_raw, counts)
        ele_pt_corr = ak.unflatten(electrons.pt_raw * correction_factor, counts)
        corrected_pt = filter_boundaries(ele_pt_corr, ele_pt)

        events["Electron", "pt"] = corrected_pt
        update_met(
            events=events,
            other_obj="Electron",
            met_obj="MET" if year.startswith("201") else "PuppiMET",
        )

        # TO DO: ADD UNCERTAINTIES https://gitlab.cern.ch/cms-analysis-corrections/EGM/examples/-/blob/latest/egmScaleAndSmearingExample.py
