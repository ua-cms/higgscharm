import re
import awkward as ak


# summary of pog scale factors: https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/
# CorrectionLib files are available from
POG_CORRECTION_PATH = "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration"
POG_JSONS = {
    "pileup": ["LUM", "puWeights.json.gz"],
    "muon": ["MUO", "muon_Z.json.gz"],
    "electron_id": ["EGM", "electron.json.gz"],
    "electron_hlt": ["EGM", "electronHlt.json.gz"],
    "electron_scale": ["EGM", "electronSS.json.gz"],
    "jetvetomaps": ["JME", "jetvetomaps.json.gz"],
    "jec": ["JME", "jet_jerc.json.gz"],
    "ctag": ["BTV", "ctagging.json.gz"],
}
POG_YEARS = {
    "2022preEE": "2022_Summer22",
    "2022postEE": "2022_Summer22EE",
}


def get_pog_json(json_name: str, year: str) -> str:
    """
    returns pog json file path

    Parameters:
    -----------
        json_name:
            pog json name
        year:
            dataset year {2022preEE, 2022postEE}
    """
    if json_name in POG_JSONS:
        pog_json = POG_JSONS[json_name]
    else:
        print(f"No json for {json_name}")
    return f"{POG_CORRECTION_PATH}/POG/{pog_json[0]}/{POG_YEARS[year]}/{pog_json[1]}"


def unflat_sf(sf: ak.Array, in_limit_mask: ak.Array, n: ak.Array):
    """
    get scale factors for in-limit objects (otherwise assign 1).
    Unflat array to original shape and multiply scale factors event-wise

    Parameters:
    -----------
        sf:
            Array with 1D scale factors
        in_limit_mask:
            Array mask for events with objects within correction limits
        n:
            Array with number of objects per event
    """
    sf = ak.where(in_limit_mask, sf, ak.ones_like(sf))
    return ak.fill_none(ak.prod(ak.unflatten(sf, n), axis=1), value=1)


def get_era(input_str):
    # Check if the input string starts with "Muon" or "EGamma"
    if input_str.startswith("Muon") or input_str.startswith("MuonEG") or input_str.startswith("EGamma"):
        # Use regex to find the letter following "Muon" or "EGamma"
        match = re.search(r"MuonEG([A-Za-z])|Muon([A-Za-z])|EGamma([A-Za-z])", input_str)
        if match:
            # Return the first matched group (the letter following "Muon", "MuonEG" or "EGamma")
            return match.group(1) or match.group(2)
    # If the input doesn't start with "Muon" or "EGamma", return "MC"
    return "MC"
