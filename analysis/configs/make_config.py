# shamelessly copied from https://github.com/green-cabbage/copperheadV2/blob/main/make_parameters.py
import os
import yaml
import argparse

config = {}
config["zplusjet"] = {
    "2022EE": {
        "muon_id_wp": "medium",
        "muon_iso_wp": "loose",
        "tagger": "pnet",
        "tagger_wp": "tight",
    }
}
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmVRun3Analysis#DATA_AN2
config["lumimask"] = {
    "2022EE": "analysis/data/Cert_Collisions2022_355100_362760_Golden.txt",
}

# https://twiki.cern.ch/twiki/bin/view/CMS/MuonHLT2022#Recommended_trigger_paths_for_20
config["hlt_paths"] = {
    "2022EE": ["IsoMu24", "Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"],
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        default="2022EE",
        action="store",
        help="year",
    )
    args = parser.parse_args()
    config_to_save = {}
    for key, val in config.items():
        print(f"type(val) : {type(val)}")
        print(f"make parameters key: {key}")
        if "jec" in key:
            sub_jec_pars = {}
            for sub_key, sub_val in val.items():
                sub_jec_pars[sub_key] = sub_val[args.year]
            print(f"make parameters sub_jec_pars: {sub_jec_pars}")
            config_to_save[key] = sub_jec_pars
        else:
            config_to_save[key] = val[args.year]
    print(f"make_parameters config_to_save: \n {config_to_save}")
    print(f"make_parameters type(config_to_save): \n {type(config_to_save)}")

    with open(f"config_{args.year}.yaml", "w") as outfile:
        yaml.dump(config_to_save, outfile, default_flow_style=False)