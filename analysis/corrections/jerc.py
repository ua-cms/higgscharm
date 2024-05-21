# tools to apply JEC/JER and compute their uncertainties (https://cms-jerc.web.cern.ch/Recommendations/)
# copied from https://github.com/green-cabbage/copperheadV2/blob/main/corrections/jet.py
import numpy as np
import awkward as ak
from pathlib import Path
from coffea.lookup_tools import extractor
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory


JEC_PARAMS = {
    "runs": {
        "2022EE": ["E", "F", "G"],
    },
    "jec_levels_mc": {
        "2022EE": ["L1FastJet", "L2Relative", "L3Absolute"],
    },
    "jec_levels_data": {
        "2022EE": ["L1FastJet", "L2Relative", "L3Absolute", "L2L3Residual"],
    },
    # I modified the original names since coffea jetmet_tools requires file names 
    # of "5 words in length" ('Summer22EE22Sep2023_V2_MC_L1FastJet_AK4PFPuppi.jec')
    "jec_tags": {
        "2022EE": "Summer22EE22Sep2023_V2_MC",
    },
    "jer_tags": {
        "2022EE": "Summer22EE22Sep2023_JRV1_MC",
    },
    "jec_data_tags": {
        "2022EE": {
            "Summer22EE22Sep2023_RunE_V2_DATA": ["E", "F", "G"],
        },
    },
    "jec_variations": {
        "2022EE": [
            "AbsoluteMPFBias",
            "AbsoluteScale",
            "AbsoluteStat",
            "FlavorQCD",
            "Fragmentation",
            "PileUpDataMC",
            "PileUpPtBB",
            "PileUpPtEC1",
            "PileUpPtEC2",
            "PileUpPtHF",
            "PileUpPtRef",
            "RelativeFSR",
            "RelativeJEREC1",
            "RelativeJEREC2",
            "RelativeJERHF",
            "RelativePtBB",
            "RelativePtEC1",
            "RelativePtEC2",
            "RelativePtHF",
            "RelativeBal",
            "RelativeSample",
            "RelativeStatEC",
            "RelativeStatFSR",
            "RelativeStatHF",
            "SinglePionECAL",
            "SinglePionHCAL",
            "TimePtEta",
            "Total",
        ],
    },
}


def jec_names_and_sources(year):
    names = {}
    suffix = {
        "jec_names": [
            f"_{level}_AK4PFPuppi" for level in JEC_PARAMS["jec_levels_mc"][year]
        ],
        "jec_names_data": [
            f"_{level}_AK4PFPuppi" for level in JEC_PARAMS["jec_levels_data"][year]
        ],
        "junc_names": ["_Uncertainty_AK4PFPuppi"],
        "junc_names_data": ["_Uncertainty_AK4PFPuppi"],
        "junc_sources": ["_UncertaintySources_AK4PFPuppi"],
        "junc_sources_data": ["_UncertaintySources_AK4PFPuppi"],
        "jer_names": ["_PtResolution_AK4PFPuppi"],
        "jersf_names": ["_SF_AK4PFPuppi"],
    }
    for key, suff in suffix.items():
        if "data" in key:
            names[key] = {}
            for run in JEC_PARAMS["runs"][year]:
                for tag, iruns in JEC_PARAMS["jec_data_tags"][year].items():
                    if run in iruns:
                        names[key].update({run: [f"{tag}{s}" for s in suff]})
        else:
            tag = (
                JEC_PARAMS["jer_tags"][year]
                if "jer" in key
                else JEC_PARAMS["jec_tags"][year]
            )
            names[key] = [f"{tag}{s}" for s in suff]
    return names


def jec_weight_sets(year):
    data_path = str(Path(Path.home(), "higgscharm/analysis/data"))
    names = jec_names_and_sources(year)
    weight_sets = {}
    extensions = {
        "jec_names": "jec",
        "jer_names": "jr",
        "jersf_names": "jersf",
        "junc_names": "junc",
        "junc_sources": "junc",
    }
    weight_sets["jec_weight_sets_mc"] = []
    weight_sets["jec_weight_sets_data"] = []
    
    for opt, ext in extensions.items():
        # MC
        weight_sets["jec_weight_sets_mc"].extend(
            [f"* * {data_path}/jec/{name}.{ext}.txt" for name in names[opt]]
        )
        # Data
        if "jer" in opt:
            continue
        data = []
        for run, items in names[f"{opt}_data"].items():
            data.extend(items)
        data = list(set(data))
        weight_sets["jec_weight_sets_data"].extend(
            [f"* * {data_path}/jec/{name}.{ext}.txt" for name in data]
        )
    return weight_sets, names


def get_jec_factory(
    events,
    era="E",
    year="2022EE",
    apply_jec=False,
    apply_jer=True,
    apply_junc=False,
):
    weight_sets, names = jec_weight_sets(year)
    # prepare evaluators for JEC, JER and their systematics
    jec_factories = {}
    jec_factories_data = {}
    jetext = extractor()
    jetext.add_weight_sets(weight_sets["jec_weight_sets_mc"])
    jetext.add_weight_sets(weight_sets["jec_weight_sets_data"])
    jetext.finalize()
    jet_evaluator = jetext.make_evaluator()

    stacks_def = {
        "jec_stack": ["jec_names"],
        "jer_stack": ["jer_names", "jersf_names"],
        "junc_stack": ["junc_names"],
    }
    stacks = {}
    for key, vals in stacks_def.items():
        stacks[key] = []
        for v in vals:
            stacks[key].extend(names[v])
    jec_input_options = {}
    jet_variations = ["jec", "jer", "junc"]
    for variation in jet_variations:
        jec_input_options[variation] = {}
        for name in stacks[f"{variation}_stack"]:
            jec_input_options[variation][name] = jet_evaluator[name]
    for src in names["junc_sources"]:
        for key in jet_evaluator.keys():
            if src in key:
                jec_input_options["junc"][key] = jet_evaluator[key]
    jec_options = {}
    if apply_jec:
        jec_options.update(jec_input_options["jec"])
    if apply_jer:
        jec_options.update(jec_input_options["jer"])
    if apply_junc:
        jec_options.update(jec_input_options["junc"])
        
    # add requiered variables to Jet collection
    jets = events.Jet
    if apply_jec:
        # set raw pT and Mass, otherwise original pT and Mass will be used as 'raw' values
        events["Jet", "pt_raw"] = (
            (1 - jets.rawFactor) * jets.pt if apply_jec else jets.pt
        )
        events["Jet", "mass_raw"] = (
            (1 - jets.rawFactor) * jets.mass if apply_jec else jets.mass
        )
    if apply_jer:
        # set ptGenJet (required for hybrid JER smearing method)
        events["Jet", "pt_gen"] = ak.values_astype(
            ak.fill_none(jets.matched_gen.pt, 0), np.float32
        )
    events["Jet", "rho"] = ak.ones_like(jets.pt) * events.Rho.fixedGridRhoFastjetAll

    # set jerc name map (I don't use JECStack.blank_name_map since it includes 'ptRaw' and 'massRaw' by default)
    jec_name_map = {
        "JetPt": "pt",
        "JetMass": "mass",
        "JetEta": "eta",
        "JetA": "area",
        "ptGenJet": "pt_gen",
        "Rho": "rho",
        "METpt": None,
        "METphi": None,
        "JetPhi": "phi",
        "UnClusteredEnergyDeltaX": None,
        "UnClusteredEnergyDeltaY": None,
    }
    if apply_jec:
        jec_name_map.update(
            {
                "ptRaw": "pt_raw",
                "massRaw": "mass_raw",
            }
        )
    if era == "MC":
        # create MC factory with jec, jer and junc stack
        stack = JECStack(jec_options)
        jec_factory = CorrectedJetsFactory(jec_name_map, stack)
    else:
        # create a separate factory for the data era
        jec_inputs_data = {}
        for opt in ["jec", "junc"]:
            jec_inputs_data.update(
                {name: jet_evaluator[name] for name in names[f"{opt}_names_data"][era]}
            )
        for src in names["junc_sources_data"][era]:
            for key in jet_evaluator.keys():
                if src in key:
                    jec_inputs_data[key] = jet_evaluator[key]
        jec_stack_data = JECStack(jec_inputs_data)
        jec_factory = CorrectedJetsFactory(jec_name_map, jec_stack_data)
        
    return jec_factory