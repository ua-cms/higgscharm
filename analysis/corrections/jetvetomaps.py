import correctionlib
import numpy as np
import awkward as ak
import dask_awkward as dak
from analysis.corrections.utils import get_pog_json


def jetvetomaps_mask(jets: ak.Array, year: str, mapname: str = "jetvetomap"):
    """
    These are the jet veto maps showing regions with an excess of jets (hot zones) and lack of jets
    (cold zones). Using the phi-symmetry of the CMS detector, these areas with detector and or
    calibration issues can be pinpointed.

    Non-zero value indicates that the region is vetoed

    taken from: https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/summaries/JME_2022_Summer22EE_jetvetomaps.html
    """
    vetomap_names = {
        "2022preEE": "Summer22_23Sep2023_RunCD_V1",
        "2022postEE": "Summer22EE_23Sep2023_RunEFG_V1",
    }
    cset = correctionlib.CorrectionSet.from_file(get_pog_json("jetvetomaps", year))

    jet_eta_mask = np.abs(jets.eta) < 5.19
    jet_phi_mask = np.abs(jets.phi) < 3.14

    in_jet_mask = jet_eta_mask & jet_phi_mask
    in_jets = jets.mask[in_jet_mask]

    jets_eta = ak.fill_none(in_jets.eta, 0.0)
    jets_phi = ak.fill_none(in_jets.phi, 0.0)

    vetomaps = dak.map_partitions(
        cset[vetomap_names[year]].evaluate, mapname, jets_eta, jets_phi
    )
    return vetomaps == 0