import correctionlib
import numpy as np
import awkward as ak
from pathlib import Path
from analysis.corrections.met import update_met


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
    json_path = (
        Path.cwd() / "analysis" / "data" / f"{year}_electronSS_EtDependent.json.gz"
    )
    cset = correctionlib.CorrectionSet.from_file(str(json_path))

    events["Electron", "pt_raw"] = ak.ones_like(events.Electron.pt) * events.Electron.pt
    electrons = ak.flatten(events.Electron)
    counts = ak.num(events.Electron)
    gain = electrons.seedGain
    run = np.repeat(events.run, counts)
    eta = electrons.eta + electrons.deltaEtaSC
    abseta = np.abs(eta)
    r9 = electrons.r9
    pt = electrons.pt_raw

    # get correction factor: scale for Data and smear for MC
    if hasattr(events, "genWeight"):
        smear_evaluator = cset[f"EGMSmearAndSyst_ElePTsplit_{year}"]
        rho = smear_evaluator.evaluate("smear", pt, r9, abseta)
        rng = np.random.default_rng(seed=42)
        correction_factor = rng.normal(loc=1.0, scale=rho)
    else:
        scale_evaluator = cset.compound[f"EGMScale_Compound_Ele_{year}"]
        correction_factor = scale_evaluator.evaluate(
            "scale", run, eta, r9, abseta, pt, gain
        )

    if variation == "nominal":
        # get corrected electron pt
        ele_pt = ak.unflatten(electrons.pt_raw, counts)
        ele_pt_corr = ak.unflatten(electrons.pt_raw * correction_factor, counts)
        corrected_pt = filter_boundaries(ele_pt_corr, ele_pt)

        # update muon pT
        events["Electron", "pt"] = corrected_pt
        # Propagate to MET
        update_met(events=events, other_obj="Muon", met_obj="PuppiMET")
