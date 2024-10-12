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
    "ctag": ["BTV", "ctagging.json.gz"]
}
POG_YEARS = {
    "2022": "2022_Summer22",
    "2022EE": "2022_Summer22EE",
}

def get_pog_json(json_name: str, year: str) -> str:
    """
    returns pog json file path

    Parameters:
    -----------
        json_name:
            json name {pileup, muon, jetvetomaps}
        year:
            dataset year {2022, 2022EE}
    """
    if json_name in POG_JSONS:
        pog_json = POG_JSONS[json_name]
    else:
        print(f"No json for {json_name}")
    return f"{POG_CORRECTION_PATH}/POG/{pog_json[0]}/{POG_YEARS[year]}/{pog_json[1]}"

