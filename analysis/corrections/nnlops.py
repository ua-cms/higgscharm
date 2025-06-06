# taken from: https://gitlab.cern.ch/cms-analysis/general/HiggsDNA/-/blob/master/higgs_dna/systematics/event_weight_systematics.py?ref_type=heads#L631
import json
import numpy as np
import importlib.resources
from pathlib import Path
from scipy.interpolate import interp1d


def add_nnlops_weight(events, weights_container, generator="powheg"):
    """
    NNLOPS reweighting for ggH events to be applied to NLO Madgraph (and Powheg).
    Swap generator argument to 'powheg' if to be applied to powheg events
    Reweight event based on truth Higgs pt and number of jets, extracted from HTXS object
    Constructs njet-dependent linear splines based on input data, functions of Higgs pt
    Reweighting is applied always if correction is specified in runner JSON.
    Warning is thrown if ggh or glugluh is not in the name.
    """
    data_path = f"{Path.cwd()}.analysis.data"
    with importlib.resources.open_text("analysis.data", "NNLOPS_reweight.json") as file:
        nnlops_reweight = json.load(file)  
        
    # Load reweight factors for specific generator
    nnlops_reweight = nnlops_reweight[generator]

    # Build linear splines for different njet bins
    spline_0jet = interp1d(
        nnlops_reweight["0jet"]["pt"], nnlops_reweight["0jet"]["weight"]
    )
    spline_1jet = interp1d(
        nnlops_reweight["1jet"]["pt"], nnlops_reweight["1jet"]["weight"]
    )
    spline_2jet = interp1d(
        nnlops_reweight["2jet"]["pt"], nnlops_reweight["2jet"]["weight"]
    )
    spline_ge3jet = interp1d(
        nnlops_reweight["3jet"]["pt"], nnlops_reweight["3jet"]["weight"]
    )

    # Load truth Higgs pt and njets (pt>30) from events
    higgs_pt = events.HTXS.Higgs_pt
    njets30 = events.HTXS.njets30

    # Extract scale factors from splines and mask for different jet bins
    # Define maximum pt values as interpolated splines only go up so far
    sf = (
        (njets30 == 0) * spline_0jet(np.minimum(np.array(higgs_pt), 125.0))
        + (njets30 == 1) * spline_1jet(np.minimum(np.array(higgs_pt), 625.0))
        + (njets30 == 2) * spline_2jet(np.minimum(np.array(higgs_pt), 800.0))
        + (njets30 >= 3) * spline_ge3jet(np.minimum(np.array(higgs_pt), 925.0))
    )
    weights_container.add("ggH_nnlops", sf)