import correctionlib
import numpy as np
import awkward as ak


def apply_met_phi_corrections(
    events: ak.Array,
    is_mc: bool,
    year: str,
):
    """
    Apply MET phi modulation corrections

    Parameters:
    -----------
        events:
            Events array
        is_mc:
            True if dataset is MC
        year:
            Year of the dataset  {'2022preEE', '2022postEE'}

    Returns:
    --------
        corrected MET pt and phi
    """
    cset = correctionlib.CorrectionSet.from_file(
        "analysis/data/met_xy_corrections.json"
    )
    events["PuppiMET", "pt_raw"] = ak.ones_like(events.PuppiMET.pt) * events.PuppiMET.pt
    events["PuppiMET", "phi_raw"] = (
        ak.ones_like(events.PuppiMET.phi) * events.PuppiMET.phi
    )

    # make sure to not cross the maximum allowed value for uncorrected met
    met_pt = events.PuppiMET.pt_raw
    met_pt = np.clip(met_pt, 0.0, 6499.0)
    met_phi = events.PuppiMET.phi_raw
    met_phi = np.clip(met_phi, -3.15, 3.15)

    # use correct run ranges when working with data, otherwise use uniform run numbers in an arbitrary large window
    run_ranges = {
        "2022preEE": [355094, 359017],
        "2022postEE": [359045, 362760],
    }
    data_kind = "mc" if is_mc else "data"
    if data_kind == "mc":
        run = np.random.randint(
            run_ranges[year][0], run_ranges[year][1], size=len(met_pt)
        )
    else:
        run = events.run
    try:
        events["PuppiMET", "pt"] = cset[f"pt_metphicorr_puppimet_{data_kind}"].evaluate(
            met_pt.to_numpy(), met_phi.to_numpy(), events.PV.npvsGood.to_numpy(), run
        )
        events["PuppiMET", "phi"] = cset[
            f"phi_metphicorr_puppimet_{data_kind}"
        ].evaluate(
            met_pt.to_numpy(), met_phi.to_numpy(), events.PV.npvsGood.to_numpy(), run
        )
    except:
        pass


def update_met(events: ak.Array, lepton: str = "Muon") -> None:
    """
    helper function to compute new MET after lepton pT correction.
    It uses the 'pt_raw' and 'pt' fields from 'leptons' to update MET 'pt' and 'phi' fields

    Parameters:
        - events:
            Events array
        - lepton:
            Lepton name {'Muon', 'Electron', Tau'}

    https://github.com/columnflow/columnflow/blob/16d35bb2f25f62f9110a8f1089e8dc5c62b29825/columnflow/calibration/util.py#L42
    https://github.com/Katsch21/hh2bbtautau/blob/e268752454a0ce0089ff08cc6c373a353be77679/hbt/calibration/tau.py#L117
    """
    assert lepton in ["Muon", "Electron", "Tau"], "Lepton not provided"

    # get needed lepton and MET fields
    lepton_pt_raw = events[lepton, "pt_raw"]
    lepton_pt = events[lepton, "pt"]
    lepton_phi = events[lepton, "phi"]
    met_pt = events.MET.pt
    met_phi = events.MET.phi

    # build px and py sums before and after: we sum the time at x and the time at y of each event
    old_px = ak.sum(lepton_pt_raw * np.cos(lepton_phi), axis=1)
    old_py = ak.sum(lepton_pt_raw * np.sin(lepton_phi), axis=1)
    new_px = ak.sum(lepton_pt * np.cos(lepton_phi), axis=1)
    new_py = ak.sum(lepton_pt * np.sin(lepton_phi), axis=1)

    # get x and y changes
    delta_x = new_px - old_px
    delta_y = new_py - old_py

    # propagate changes to MET (x, y) components
    met_px = met_pt * np.cos(met_phi) + delta_x
    met_py = met_pt * np.sin(met_phi) + delta_y

    # propagate changes to MET (pT, phi) components
    met_pt = np.sqrt((met_px**2.0 + met_py**2.0))
    met_phi = np.arctan2(met_py, met_px)

    # update MET fields
    events["MET", "pt"] = met_pt
    events["MET", "phi"] = met_phi
