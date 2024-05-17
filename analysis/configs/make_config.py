# shamelessly copied from https://github.com/green-cabbage/copperheadV2/blob/main/make_parameters.py
import os
import yaml
import argparse

def for_all_years(value):
    out = {k: value for k in ["2022EE"]}
    return out

def get_variations(sources):
    result = []
    for v in sources:
        result.append(v + "_up")
        result.append(v + "_down")
    return result


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

# https://cms-jerc.web.cern.ch/Recommendations/
jec_config = {}
jec_config["runs"] = {
    "2022EE": ["E"],
}
jec_config["jec_levels_mc"] = for_all_years(["L1FastJet", "L2Relative", "L3Absolute"])
jec_config["jec_levels_data"] = for_all_years(
    ["L1FastJet", "L2Relative", "L3Absolute", "L2L3Residual"]
)
jec_config["jec_tags"] = {
    "2022EE": "Summer22EE_22Sep2023_V2_MC",
}
jec_config["jer_tags"] = {
    "2022EE": "Summer22EE_22Sep2023_JRV1",
}
jec_config["jec_data_tags"] = {
    "2022EE": {
        "Summer22EE_22Sep2023_RunE_V2": ["E"],
    },
}
"""
jec_unc_to_consider = {
    "2022EE": [
        "Absolute",
        "Absolute2018",
        "BBEC1",
        "BBEC12018",
        "EC2",
        "EC22018",
        "HF",
        "HF2018",
        "RelativeBal",
        "RelativeSample2018",
        "FlavorQCD",
    ],
}
jec_config["jec_variations"] = {
    year: get_variations(jec_unc_to_consider[year]) for year in ["2022EE"]
}

jer_variations = ["jer1", "jer2", "jer3", "jer4", "jer5", "jer6"]
jec_config["jer_variations"] = {
    year: get_variations(jer_variations) for year in ["2022EE"]
}
"""
config["jec_config"] = jec_config

"""
# https://indico.cern.ch/event/1304360/contributions/5518916/attachments/2692786/4673101/230731_BTV.pdf
config["ctagger_wps"] = {
    "2022EE": {
        "deepjet": {
            "loose": {
                "cvsb": 0.206,
                "cvsl": 0.042,
            },
            "medium": {
                "cvsb": 0.298,
                "cvsl": 0.108,
            },
            "tight": {
                "cvsb": 0.241,
                "cvsl": 0.305,
            },
        },
        "pnet": {
            "loose": {
                "cvsb": 0.182,
                "cvsl": 0.054,
            },
            "medium": {
                "cvsb": 0.304,
                "cvsl": 0.160,
            },
            "tight": {
                "cvsb": 0.258,
                "cvsl": 0.491,
            },
        },
        "part": {
            "loose": {
                "cvsb": 0.067,
                "cvsl": 0.0390,
            },
            "medium": {
                "cvsb": 0.128,
                "cvsl": 0.117,
            },
            "tight": {
                "cvsb": 0.095,
                "cvsl": 0.358,
            },
        },
    }
}
"""
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