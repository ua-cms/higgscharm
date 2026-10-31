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

    # 2026-10-30: no pt window any more. The [20, 250] GeV window was copied
    # from the muon code (its message even said "muon"); the EGM EtDependent
    # jsons are binned in pt from 0 to 9999 GeV (flow clamp), and the EGM
    # example applies no window, so electrons below 20 GeV (most of the 4e /
    # 2e2mu subleading electrons) were left uncorrected.

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
            # kept so apply_electron_ss_shifts reuses the same random number
            # (EGM recipe: the smearing variations use the nominal one)
            events["Electron", "ss_rndm"] = ak.unflatten(random_numbers, counts)
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



def apply_electron_ss_shifts(events, year, met_field_key):
    """Return [(collections_dict, shift_name), ...] for the electron energy
    scale/resolution systematics CMS_scale_e_<year>, CMS_res_e_<year> (added
    2026-10-30), with the same contract as muon_ss.py::apply_muon_ss_shifts:
    nominal entry first, then Up/Down for scale and resolution; collections_dict
    carries {"Electron": ..., met_field_key: ...}.

    Follows the EGM example (cms-analysis-corrections/EGM/examples,
    egmScaleAndSmearingExample.py) for the EtDependent jsons:
      resolution: smear +- esmear (down clipped at 0), with the SAME random
                  number as the nominal smearing, applied to the raw pt
      scale:      (1 +- escale) applied to the nominally smeared pt
    All inputs are the original MC variables (raw pt, r9, ScEta).

    MUST be called after apply_electron_ss_corrections(variation="nominal") in
    the same pass (needs Electron.pt_raw and Electron.ss_rndm). Data: nominal
    entry only.
    """
    nominal_entry = (
        {"Electron": events.Electron, met_field_key: events[met_field_key]},
        None,
    )
    if not hasattr(events, "genWeight"):
        return [nominal_entry]

    cset = correctionlib.CorrectionSet.from_file(correction_files["electron_ss"][year])
    sas = cset["SmearAndSyst"]
    electrons = ak.flatten(events.Electron)
    counts = ak.num(events.Electron)
    pt_raw = np.asarray(electrons.pt_raw)
    pt_nom = np.asarray(electrons.pt)
    rndm = np.asarray(electrons.ss_rndm)
    sceta = np.asarray(electrons.eta + electrons.deltaEtaSC)
    r9 = np.asarray(electrons.r9)

    smear = sas.evaluate("smear", pt_raw, r9, sceta)
    unc_smear = sas.evaluate("esmear", pt_raw, r9, sceta)
    unc_scale = sas.evaluate("escale", pt_raw, r9, sceta)
    pt_var = {
        "CMS_res_e_{}Up": pt_raw * (1 + (smear + unc_smear) * rndm),
        "CMS_res_e_{}Down": pt_raw * (1 + np.maximum(smear - unc_smear, 0.0) * rndm),
        "CMS_scale_e_{}Up": pt_nom * (1 + unc_scale),
        "CMS_scale_e_{}Down": pt_nom * (1 - unc_scale),
    }
    nested_nom = events.Electron.pt
    nested_raw = events.Electron.pt_raw
    variants = []
    for name in [
        "CMS_scale_e_{}Up",
        "CMS_scale_e_{}Down",
        "CMS_res_e_{}Up",
        "CMS_res_e_{}Down",
    ]:
        # same NaN guard as the nominal correction (no pt window); a fresh
        # array per variant (no aliasing between Up and Down)
        pt_v = filter_boundaries(ak.unflatten(pt_var[name], counts), nested_raw)
        ele = ak.with_field(events.Electron, pt_v, "pt")
        met = _met_with_ele_delta(events, met_field_key, nested_nom, pt_v, events.Electron.phi)
        variants.append(
            ({"Electron": ele, met_field_key: met}, name.format(year[:4]))
        )
    return [nominal_entry] + variants


def _met_with_ele_delta(events, met_field_key, pt_from, pt_to, phi):
    """events' CURRENT met_field_key moved from electron pt `pt_from` to `pt_to`,
    without mutating events -- same met.py::update_met reuse as
    muon_ss.py::_met_with_muon_delta"""
    tmp = ak.Array(
        {
            "Electron": ak.zip({"pt_raw": pt_from, "pt": pt_to, "phi": phi}),
            met_field_key: events[met_field_key],
        }
    )
    update_met(tmp, other_obj="Electron", met_obj=met_field_key)
    return tmp[met_field_key]
