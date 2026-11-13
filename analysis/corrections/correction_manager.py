import warnings
import numpy as np
from coffea.analysis_tools import Weights
from analysis.filesets.utils import get_nano_version
from analysis.corrections.muon import MuonWeights
from analysis.corrections.hzz_muon import MuonHZZWeights
from analysis.corrections.ctag import CTagCorrector
from analysis.corrections.jetvetomaps import jet_veto
from analysis.corrections.pileup import add_pileup_weight
from analysis.corrections.nnlops import add_nnlops_weight
from analysis.corrections.lhepdf import add_lhepdf_weight
from analysis.corrections.top_pt import add_top_pt_weight
from analysis.corrections.higgs_hf import add_higgs_hf_weight
from analysis.corrections.electron import ElectronWeights
from analysis.corrections.hzz_electron import ElectronHZZWeights
from analysis.corrections.jec import apply_jerc_corrections
from analysis.corrections.kfactors import add_kfactor_weights
from analysis.corrections.lhescale import add_scalevar_weight
from analysis.corrections.hb_syst_transfer import is_old_hplusbottom, add_hb_syst_transfer_weights
from analysis.corrections.met import apply_met_phi_corrections
from analysis.corrections.muon_ss import apply_muon_ss_corrections
from analysis.corrections.partonshower import add_partonshower_weight
from analysis.corrections.correctionlib_files import correction_files
from analysis.corrections.ctag2d import CTag2DCorrector, BOUNDARIES_BY_ERA
from analysis.corrections.electron_ss import apply_electron_ss_corrections
from analysis.corrections.hczz_legacy import jerc as hczz_jerc
from analysis.corrections.hczz_legacy import muon_ss as hczz_muon_ss
from analysis.corrections.hczz_legacy import electron_ss as hczz_electron_ss
from analysis.corrections.hczz_legacy import jetvetomaps as hczz_jetvetomaps
from analysis.corrections.tau_energy import apply_tau_energy_scale_corrections


def object_corrector_manager(events, year, run, dataset, workflow_config):
    """apply object level corrections.

    Returns (vetoed_events, shifts): shifts is a list of (collections, shift_name),
    nominal first (shift_name None). Two workflow schemas are supported:
      corrections: object: [jet, jetveto, muon, electron]   (coffea-2026)
      corrections: objects: [jec | jec_shifts, jet_vetomaps, muon_ss, electon_ss, met_phi]
                                                             (HcZZ, analysis/corrections/hczz_legacy)
    """
    corrections_config = workflow_config.corrections_config
    if "objects" in corrections_config:
        return _object_corrector_manager_hczz(
            events, year, dataset, corrections_config["objects"]
        )
    objcorr_config = corrections_config["object"]

    shifts = [({}, None)]
    vetoed_events = events
    if objcorr_config:

        if "jet" in objcorr_config:
            # apply jet energy scale and smearing corrections
            shifts = apply_jerc_corrections(
                events=events,
                year=year,
                shifts=shifts,
                corrections_config=corrections_config,
            )
        else:
            met_field_key = "MET" if run == "2" else "PuppiMET"
            shifts = [({"Jet": events.Jet, "MET": events[met_field_key]}, None)]

        if "muon" in objcorr_config:
            # apply muon scale and smearing corrections
            shifts = apply_muon_ss_corrections(
                events=events,
                year=year,
                shifts=shifts,
                corrections_config=corrections_config,
            )
        else:
            for i in range(len(shifts)):
                shifts[i][0]["Muon"] = events.Muon

        if "electron" in objcorr_config:
            # electron scale and smearing corrections are applied only to run3 datasets
            # they are already applied in nanov9 datasets
            if run == "3":
                shifts = apply_electron_ss_corrections(
                    events=events,
                    year=year,
                    shifts=shifts,
                    corrections_config=corrections_config,
                )
        else:
            for i in range(len(shifts)):
                shifts[i][0]["Electron"] = events.Electron

        if "tau" in objcorr_config:
            shifts = apply_tau_energy_scale_corrections(
                events=events,
                year=year,
                shifts=shifts,
                corrections_config=corrections_config,
            )
        else:
            for i in range(len(shifts)):
                shifts[i][0]["Tau"] = events.Tau

        if "met" in objcorr_config:
            # apply met phi modulation corrections
            if year in correction_files["met"]:
                shifts = apply_met_phi_corrections(
                    events=events,
                    year=year,
                    shifts=shifts,
                    corrections_config=corrections_config,
                )

        if "jetveto" in objcorr_config:
            event_veto = jet_veto(events, year)
            vetoed_events = events[~event_veto]
            for collections, _ in shifts:
                for key in collections:
                    collections[key] = collections[key][~event_veto]
        else:
            vetoed_events = events

    return vetoed_events, shifts


def _object_corrector_manager_hczz(events, year, dataset, objcorr_config):
    """HcZZ object corrections (hczz_legacy modules), as in the 2022/2023 production.

    Jet veto map first (event veto on the NanoAOD jets). With "jec_shifts": muon/electron
    scale and smearing, then JEC/JER nominal + CMS_scale_j/CMS_res_j shifts, plus
    CMS_scale_m/CMS_res_m and CMS_scale_e/CMS_res_e for MC; every shift entry carries
    Jet, MET, Muon and Electron. Otherwise
    (nominal only): JEC/JER -> muon SS -> electron SS -> MET-phi (2022).
    """
    met_field_key = "PuppiMET" if int(get_nano_version(year)) >= 12 else "MET"
    objcorr_config = objcorr_config or []
    if "jet_vetomaps" in objcorr_config:
        events = hczz_jetvetomaps.apply_jetvetomaps(events, year)

    if "jec_shifts" in objcorr_config:
        # nominal muon/electron SS first; apply_jerc_shifts reads the current MET, and
        # independent MET deltas (JEC Type-1, muon SS) commute
        if "muon_ss" in objcorr_config:
            hczz_muon_ss.apply_muon_ss_corrections(events=events, year=year)
        if "electon_ss" in objcorr_config and year.startswith("202"):
            hczz_electron_ss.apply_electron_ss_corrections(
                events=events, year=year, variation="nominal"
            )
        # jes_regrouped (2026-11-03): the 11 JME Regrouped JES sources per era + JER per era instead of Total JES + JER per year
        jec_variants = hczz_jerc.apply_jerc_shifts(
            events, year, dataset, jes_regrouped="jes_regrouped" in objcorr_config
        )
        nominal_jet = jec_variants[0][0]["Jet"]
        nominal_met = jec_variants[0][0]["MET"]
        events["Jet"] = nominal_jet
        events[met_field_key] = nominal_met
        if "muon_ss" in objcorr_config and hasattr(events, "genWeight"):
            muon_variants = hczz_muon_ss.apply_muon_ss_shifts(events, year, met_field_key)
        else:
            muon_variants = [
                ({"Muon": events.Muon, met_field_key: events[met_field_key]}, None)
            ]
        nominal_muon = muon_variants[0][0]["Muon"]
        # electron energy scale/resolution shifts (CMS_scale_e/CMS_res_e,
        # added 2026-10-30); Run3 MC only, as the nominal electon_ss
        if (
            "electon_ss" in objcorr_config
            and year.startswith("202")
            and hasattr(events, "genWeight")
        ):
            electron_variants = hczz_electron_ss.apply_electron_ss_shifts(
                events, year, met_field_key
            )
        else:
            electron_variants = [({"Electron": events.Electron}, None)]
        nominal_electron = electron_variants[0][0]["Electron"]
        shifts = [
            (
                {
                    "Jet": nominal_jet,
                    met_field_key: nominal_met,
                    "Muon": nominal_muon,
                    "Electron": nominal_electron,
                },
                None,
            )
        ]
        for collections, shift_name in jec_variants[1:]:
            shifts.append(
                (
                    {
                        "Jet": collections["Jet"],
                        met_field_key: collections["MET"],
                        "Muon": nominal_muon,
                        "Electron": nominal_electron,
                    },
                    shift_name,
                )
            )
        for collections, shift_name in muon_variants[1:]:
            shifts.append(
                (
                    {
                        "Jet": nominal_jet,
                        met_field_key: collections[met_field_key],
                        "Muon": collections["Muon"],
                        "Electron": nominal_electron,
                    },
                    shift_name,
                )
            )
        for collections, shift_name in electron_variants[1:]:
            shifts.append(
                (
                    {
                        "Jet": nominal_jet,
                        met_field_key: collections[met_field_key],
                        "Muon": nominal_muon,
                        "Electron": collections["Electron"],
                    },
                    shift_name,
                )
            )
        return events, shifts

    if "jec" in objcorr_config:
        hczz_jerc.apply_jerc_corrections(events, year, dataset)
    if "muon_ss" in objcorr_config:
        hczz_muon_ss.apply_muon_ss_corrections(events=events, year=year)
    if "electon_ss" in objcorr_config:
        # already applied in nanov9
        if year.startswith("202"):
            hczz_electron_ss.apply_electron_ss_corrections(
                events=events, year=year, variation="nominal"
            )
    if "met_phi" in objcorr_config:
        if year.startswith("2022"):
            apply_met_phi_corrections(
                events=events, is_mc=hasattr(events, "genWeight"), year=year
            )
    return events, [({}, None)]


def weight_manager(pruned_ev, year, dataset, workflow_config, category, shift):
    """apply event level corrections (weights)"""
    # get weights config info
    weights_config = workflow_config.corrections_config["event_weights"]
    # initialize weights container
    weights_container = Weights(len(pruned_ev), storeIndividual=True)
    # get nano version
    nano_version = get_nano_version(year)
    # add weights
    if hasattr(pruned_ev, "genWeight"):
        if weights_config["genWeight"]:
            weights_container.add("genweight", pruned_ev.genWeight)

        if "pileupWeight" in weights_config:
            if weights_config["pileupWeight"]:
                add_pileup_weight(
                    events=pruned_ev,
                    year=year,
                    shift=shift,
                    weights_container=weights_container,
                )
        if "partonshowerWeight" in weights_config:
            if weights_config["partonshowerWeight"]:
                if "PSWeight" in pruned_ev.fields:
                    add_partonshower_weight(
                        events=pruned_ev,
                        weights_container=weights_container,
                        shift=shift,
                    )
        # 2026-11-09: old-gridpack private H+b (broken LHE weights) -> scale/PDF/alphaS transferred from the
        # official H+b -> gamma gamma 5FS FxFx sample (hb_syst_transfer.py) instead of its own LHE weights
        old_hb = is_old_hplusbottom(dataset) and (weights_config.get("lhepdfWeight") or weights_config.get("lhescaleWeight"))
        if old_hb:
            add_hb_syst_transfer_weights(pruned_ev, weights_container)
        if "lhepdfWeight" in weights_config and not old_hb:
            if weights_config["lhepdfWeight"]:
                if "LHEPdfWeight" in pruned_ev.fields:
                    add_lhepdf_weight(
                        events=pruned_ev,
                        weights_container=weights_container,
                        shift=shift,
                    )
        if "lhescaleWeight" in weights_config and not old_hb:
            if weights_config["lhescaleWeight"]:
                if "LHEScaleWeight" in pruned_ev.fields:
                    add_scalevar_weight(
                        events=pruned_ev,
                        weights_container=weights_container,
                        shift=shift,
                    )
        if "nnlopsWeight" in weights_config:
            if weights_config["nnlopsWeight"]:
                if dataset.startswith("GluGluH"):
                    add_nnlops_weight(
                        events=pruned_ev,
                        weights_container=weights_container,
                    )
        # 2026-11-03: m(ZZ)-dependent ZZ k-factors (corrections/zz_kfactor.py; gg->ZZ NNLO incl. variations,
        # qq->ZZ NNLO/NLO QCD); the card builder must then drop its flat gg->ZZ 2.27 (HCZZ_ZZ_KFACTOR_WEIGHTS=1)
        if weights_config.get("zz_kfactors"):
            from analysis.corrections.zz_kfactor import add_zz_kfactor_weights
            add_zz_kfactor_weights(pruned_ev, weights_container, dataset, variation="nominal" if shift is None else shift)
        # 2026-11-03: HZZ lepton SFs on the 4 leptons of the best candidate (corrections/hzz_lepton.py).
        # Use with muon: id: false and electron: id/reco: false (it replaces both, see module docstring).
        if weights_config.get("hzz_leptons"):
            from analysis.corrections.hzz_lepton import HZZLeptonWeights
            HZZLeptonWeights(
                events=pruned_ev, weights=weights_container, year=year, variation="nominal" if shift is None else shift
            ).add_all()
        if "muon" in weights_config:
            if weights_config["muon"]:
                if "selected_muons" in pruned_ev.fields:
                    muon_weights = MuonWeights(
                        events=pruned_ev,
                        year=year,
                        shift=shift,
                        weights=weights_container,
                    )
                    if "id" in weights_config["muon"]:
                        if weights_config["muon"]["id"]:
                            muon_weights.add_id_weights(
                                id_wp=weights_config["muon"]["id"]
                            )
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
                                hlt_paths=workflow_config.event_selection["hlt_paths"],
                            )
        if "electron" in weights_config:
            if weights_config["electron"]:
                if "selected_electrons" in pruned_ev.fields:
                    electron_weights = ElectronWeights(
                        events=pruned_ev,
                        year=year,
                        shift=shift,
                        weights=weights_container,
                    )
                    if "id" in weights_config["electron"]:
                        if weights_config["electron"]["id"]:
                            electron_weights.add_id_weights(
                                id_wp=weights_config["electron"]["id"]
                            )
                    if "reco" in weights_config["electron"]:
                        if weights_config["electron"]["reco"]:
                            if nano_version == "9":
                                electron_weights.add_reco_weights("RecoAbove20")
                                electron_weights.add_reco_weights("RecoBelow20")
                            elif nano_version == "12":
                                electron_weights.add_reco_weights("RecoBelow20")
                                electron_weights.add_reco_weights("Reco20to75")
                                electron_weights.add_reco_weights("RecoAbove75")
                            elif nano_version == "15":
                                # 2026-10-23: RecoBelow20 for 2024 (electrons have pT > 7 in the
                                # 4l selection); the 2025 file has no RecoBelow20 bin
                                if year == "2024":
                                    electron_weights.add_reco_weights("RecoBelow20")
                                electron_weights.add_reco_weights("Reco20to75")
                                electron_weights.add_reco_weights("RecoAbove75")
                    if "trigger" in weights_config["electron"]:
                        if weights_config["electron"]["trigger"]:
                            # no available HLT Sfs for 2024 yet
                            if nano_version != "15":
                                electron_weights.add_hlt_weights(
                                    id_wp=weights_config["electron"]["id"],
                                )

        if "ctagging" in weights_config:
            if weights_config["ctagging"]:
                working_points = ["loose", "medium", "tight"]
                if weights_config["ctagging"]["wp"] not in working_points:
                    raise ValueError(
                        f"There are no available c-tag SFs for the working point. Please specify {working_points}"
                    )
                ctag_corrector = CTagCorrector(
                    events=pruned_ev,
                    weights=weights_container,
                    worging_point=weights_config["ctagging"]["wp"],
                    category=category,
                    year=year,
                    shift=shift,
                )
                ctag_corrector.add_ctag_weights(flavor="b")
                ctag_corrector.add_ctag_weights(flavor="c")
                ctag_corrector.add_ctag_weights(flavor="light")

        if "topPtWeight" in weights_config:
            if weights_config["topPtWeight"]:
                add_top_pt_weight(
                    events=pruned_ev,
                    year=year,
                    weights_container=weights_container,
                    dataset=dataset,
                )

        if "muon_hzz" in weights_config:
            if weights_config["muon_hzz"]:
                if "selected_muons" in pruned_ev.fields:
                    if year in correction_files["muon_hzz"]:
                        muon_hzz_weights = MuonHZZWeights(
                            events=pruned_ev,
                            year=year,
                            shift=shift,
                            weights=weights_container,
                        )
                        muon_hzz_weights.add_id_weights()

        if "electron_hzz" in weights_config:
            if weights_config["electron_hzz"]:
                if "selected_electrons" in pruned_ev.fields:
                    if year in correction_files["electron_hzz"]:
                        electron_hzz_weights = ElectronHZZWeights(
                            events=pruned_ev,
                            year=year,
                            shift=shift,
                            weights=weights_container,
                        )
                        electron_hzz_weights.add_id_weights()
                        electron_hzz_weights.add_reco_weights("low")
                        electron_hzz_weights.add_reco_weights("mid")
                        electron_hzz_weights.add_reco_weights("high")

        if "kFactorWeight" in weights_config:
            if weights_config["kFactorWeight"]:
                # NNLO QCD k-factors for ggZZ (vs m4l) and NNLO QCD x NLO EW for
                # qqZZ (vs m4l, |cos theta*|), from gen-level hard-process leptons.
                # Only applied to datasets matching the ggzz/qqzz name prefixes
                # (see kfactors.py); a no-op for everything else.
                add_kfactor_weights(
                    events=pruned_ev,
                    weights_container=weights_container,
                    year=year,
                    dataset=dataset,
                    config=weights_config["kFactorWeight"],
                )

        if "ctagging_2d" in weights_config:
            if weights_config["ctagging_2d"] and (
                year not in BOUNDARIES_BY_ERA
                or year not in correction_files["ctagging_2d"]
            ):
                # Safety net for eras without a boundary table or SF file. Since 2026-10-23
                # 2024 and 2025 (2024 file as placeholder) are wired, so this no longer
                # triggers for any analysis era.
                warnings.warn(
                    f"ctagging_2d requested but not available for year '{year}' -- "
                    f"NO c-tag SF applied (weight = 1, no CMS_ctag2d nuisance).",
                    RuntimeWarning,
                    stacklevel=2,
                )
            elif weights_config["ctagging_2d"]:
                ctag2d_corrector = CTag2DCorrector(
                    events=pruned_ev,
                    weights=weights_container,
                    year=year,
                    shift=shift,
                )
                ctag2d_corrector.add_weights()

        if "higgsHFWeight" in weights_config:
            if weights_config["higgsHFWeight"]:
                # ggH/VBF only (see higgs_hf.py); per-event, keyed on gen-jet heavy
                # flavour, replaces a flat lnN on the pooled Other_Higgs group
                add_higgs_hf_weight(
                    events=pruned_ev,
                    weights_container=weights_container,
                    dataset=dataset,
                    shift=shift,
                )
    else:
        weights_container.add("weight", np.ones(len(pruned_ev)))
    return weights_container
