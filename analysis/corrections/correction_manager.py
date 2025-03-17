import awkward as ak
from coffea.analysis_tools import Weights
from analysis.corrections.muon import MuonWeights
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.nnlops import add_nnlops_weight
from analysis.corrections.lhepdf import add_lhepdf_weight
from analysis.corrections.jerc import apply_jerc_corrections
from analysis.corrections.lhescale import add_scalevar_weight
from analysis.corrections.met import apply_met_phi_corrections
from analysis.corrections.partonshower import add_partonshower_weight
from analysis.corrections.electron import ElectronWeights, ElectronSS


def object_corrector_manager(events, year, dataset, processor_config):
    """apply object level corrections"""
    objcorr_config = processor_config.corrections_config["objects"]
    if "jets" in objcorr_config:
        # apply JEC/JER corrections
        apply_jec = True
        apply_jer = False
        apply_junc = False
        if hasattr(events, "genWeight"):
            apply_jer = True
        apply_jerc_corrections(
            events,
            year=year,
            dataset=dataset,
            apply_jec=apply_jec,
            apply_jer=apply_jer,
            apply_junc=apply_junc,
        )
    if "electrons" in objcorr_config:
        # apply electron scale and smearing corrections
        electron_ss = ElectronSS(
            events=events,
            year=year,
            variation="nominal",
        )
        if hasattr(events, "genWeight"):
            # energies in MC are smeared
            electron_ss.apply_smearing()
        else:
            # energies in data are scaled
            electron_ss.apply_scale()
    if "met" in objcorr_config:
        # apply MET-phi modulation corrections
        if year.startswith("2022"):
            apply_met_phi_corrections(
                events=events, is_mc=hasattr(events, "genWeight"), year=year
            )


def weight_manager(pruned_ev, year, dataset, processor_config, variation="nominal"):
    """apply event level corrections (weights)"""
    # get weights config info
    weights_config = processor_config.corrections_config["event_weights"]
    # initialize weights container
    weights_container = Weights(len(pruned_ev), storeIndividual=True)
    if hasattr(pruned_ev, "genWeight"):
        if weights_config["genWeight"]:
            weights_container.add("genweight", pruned_ev.genWeight)

        if weights_config["pileupWeight"]:
            add_pileup_weight(
                events=pruned_ev,
                year=year,
                variation="nominal",
                weights_container=weights_container,
            )
        if weights_config["partonshowerWeight"]:
            if "PSWeight" in pruned_ev.fields:
                add_partonshower_weight(
                    events=pruned_ev,
                    weights_container=weights_container,
                )
        if weights_config["lhepdfWeight"]:
            if "LHEPdfWeight" in pruned_ev.fields:
                add_lhepdf_weight(
                    events=pruned_ev,
                    weights_container=weights_container,
                )
        if weights_config["lhescaleWeight"]:
            if "LHEScaleWeight" in pruned_ev.fields:
                add_scalevar_weight(
                    events=pruned_ev, weights_container=weights_container
                )

        if weights_config["nnlopsWeight"]:
            if dataset.startswith("GluGluH"):
                add_nnlops_weight(
                    events=pruned_ev,
                    weights_container=weights_container,
                )
        if "muon" in weights_config:
            if "selected_muons" in pruned_ev.fields:
                muon_weights = MuonWeights(
                    events=pruned_ev,
                    year=year,
                    variation=variation,
                    weights=weights_container,
                )
                if "id" in weights_config["muon"]:
                    if weights_config["muon"]["id"]:
                        muon_weights.add_id_weights(id_wp=weights_config["muon"]["id"])
                if "iso" in weights_config["muon"]:
                    if weights_config["muon"]["iso"]:
                        muon_weights.add_iso_weights(
                            id_wp=weights_config["muon"]["id"],
                            iso_wp=weights_config["muon"]["iso"],
                        )
                if "trigger" in weights_config["muon"]:
                    if weights_config["muon"]["trigger"]:
                        muon_weights.add_trigger_weights(
                            dataset=dataset,
                            id_wp=weights_config["muon"]["id"],
                            iso_wp=weights_config["muon"]["iso"],
                            hlt_paths=processor_config.event_selection["hlt_paths"],
                        )
        if "electron" in weights_config:
            if "selected_electrons" in pruned_ev.fields:
                electron_weights = ElectronWeights(
                    events=pruned_ev,
                    year=year,
                    weights=weights_container,
                    variation=variation,
                )
                if "id" in weights_config["electron"]:
                    if weights_config["electron"]["id"]:
                        electron_weights.add_id_weights(
                            id_wp=weights_config["electron"]["id"]
                        )
                if "reco" in weights_config["electron"]:
                    if weights_config["electron"]["reco"]:
                        electron_weights.add_reco_weights("RecoBelow20")
                        electron_weights.add_reco_weights("Reco20to75")
                        electron_weights.add_reco_weights("RecoAbove75")
                if "trigger" in weights_config["electron"]:
                    if weights_config["electron"]["trigger"]:
                        electron_weights.add_hlt_weights(
                            dataset=dataset,
                            id_wp=weights_config["electron"]["id"],
                            hlt_paths=processor_config.event_selection["hlt_paths"],
                        )
    else:
        weights_container.add("weight", ak.ones_like(pruned_ev.PV.npvsGood))
    return weights_container
