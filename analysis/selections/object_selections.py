import numpy as np
import awkward as ak
from coffea.nanoevents.methods import candidate
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.working_points import working_points
from analysis.selections import (
    delta_r_higher,
    delta_r_lower,
    select_dileptons,
    transverse_mass,
    get_closest_lepton,
    assign_lepton_fsr_idx,
    fourlepcand,
    make_cand,
    select_best_zzcandidate,
    select_candidate_mass,
    select_candidate_cjet_dphi,
)


class ObjectSelector:

    def __init__(self, object_selection_config, year):
        self.object_selection_config = object_selection_config
        self.year = year

    def select_objects(self, events):
        self.objects = {}
        self.events = events

        for obj_name, obj_config in self.object_selection_config.items():
            # check if object is defined from events or user defined function
            if "events" in obj_config["field"]:
                self.objects[obj_name] = eval(obj_config["field"])
            else:
                selection_function = getattr(self, obj_config["field"])
                selection_function(obj_name)
            if "add_cut" in obj_config:
                for field_to_add in obj_config["add_cut"]:
                    selection_mask = self.get_selection_mask(
                        events=events,
                        obj_name=obj_name,
                        cuts=obj_config["add_cut"][field_to_add],
                    )
                    self.objects[obj_name][field_to_add] = selection_mask
            if "add_field" in obj_config:
                for field_name, field_to_add in obj_config["add_field"].items():
                    self.objects[obj_name][field_name] = eval(field_to_add)
            if "cuts" in obj_config:
                selection_mask = self.get_selection_mask(
                    events=events, obj_name=obj_name, cuts=obj_config["cuts"]
                )
                self.objects[obj_name] = self.objects[obj_name][selection_mask]
        return self.objects

    def get_selection_mask(self, events, obj_name, cuts):
        # bring objects and year to local scope
        objects = self.objects
        year = self.year
        # initialize selection mask
        selection_mask = ak.ones_like(self.objects[obj_name].pt, dtype=bool)
        # iterate over all cuts
        for str_mask in cuts:
            mask = eval(str_mask)
            selection_mask = np.logical_and(selection_mask, mask)
        return selection_mask

    def select_genparts(self, obj_name):
        if not hasattr(self.events, "GenPart"):
            return

        gen = self.events.GenPart
        print("GenPart pt sample:", ak.to_list(gen.pt[0][:5]))
        print("GenPart eta sample:", ak.to_list(gen.eta[0][:5]))

        self.objects[obj_name] = ak.zip(
            {
                "pt": gen.pt,
                "eta": gen.eta,
                "phi": gen.phi,
                "mass": gen.mass,
                "pdgId": gen.pdgId,
                "statusFlags": gen.statusFlags,
                "genPartIdxMother": gen.genPartIdxMother,
                "isHardProcess": (gen.statusFlags & (1 << 7)) != 0,
                "isLastCopy": (gen.statusFlags & (1 << 13)) != 0,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

    # --------------------------------------------------------------------------------
    # ZToLL
    # --------------------------------------------------------------------------------
    def select_dimuons(self, obj_name):
        if "muons" not in self.objects:
            raise ValueError(f"'muons' object has not been defined!")
        self.objects[obj_name] = select_dileptons(self.objects, "muons")

    def select_dielectrons(self, obj_name):
        if "electrons" not in self.objects:
            raise ValueError(f"'electrons' object has not been defined!")
        self.objects[obj_name] = select_dileptons(self.objects, "electrons")

    # --------------------------------------------------------------------------------
    # ZZTo4L (Z+L, ZLL)
    # --------------------------------------------------------------------------------
    def select_zzto4l_leptons(self, obj_name):
        muons = self.objects["muons"]
        muons["lostHits"] = ak.zeros_like(muons.pt)
        muons["mvaHZZIso"] = ak.ones_like(muons.pt) * -999

        electrons = self.objects["electrons"]
        electrons["isPFcand"] = ak.ones_like(electrons.pt, dtype=bool)
        electrons["highPtId"] = ak.zeros_like(electrons.pt, dtype=int)

        # 1. Establish stable initial sorting layout
        leptons_pre = ak.concatenate([muons, electrons], axis=1)
        leptons_pre = leptons_pre[ak.argsort(leptons_pre.pt, axis=1, ascending=False)]
        leptons_pre["idx"] = ak.local_index(leptons_pre, axis=1)
        leptons_pre = ak.zip(
            {
                "pt": leptons_pre.pt,
                "eta": leptons_pre.eta,
                "phi": leptons_pre.phi,
                "mass": leptons_pre.mass,
                "charge": leptons_pre.charge,
                "pdgId": leptons_pre.pdgId,
                "idx": leptons_pre.idx,
                "is_loose": leptons_pre.is_loose,
                "is_relaxed": leptons_pre.is_relaxed,
                "is_tight": leptons_pre.is_tight,
                "lostHits": leptons_pre.lostHits,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

        # 2. Setup FSR photons
        fsr_photons = self.objects["fsr_photons"]
        fsr_photons["mass"] = ak.zeros_like(fsr_photons.pt)
        fsr_photons["charge"] = ak.zeros_like(fsr_photons.pt)
        fsr_photons["idx"] = ak.local_index(fsr_photons, axis=1)

        # 3. Match against the pre-isolation sorted lepton layout
        closest_lepton, dr_et2 = get_closest_lepton(fsr_photons, leptons_pre)
        fsr_photons["helper_idx"] = ak.local_index(closest_lepton, axis=1)
        fsr_photons["lepton_idx"] = ak.fill_none(closest_lepton.idx, -1)

        # Clean duplicate photon records
        fsr_cartesian = ak.cartesian(
            {"fsr1": fsr_photons, "fsr2": fsr_photons}, nested=True, axis=1
        )
        is_duplicate = (
            ak.sum(
                fsr_cartesian.fsr1.lepton_idx == fsr_cartesian.fsr2.lepton_idx, axis=-1
            )
            > 1
        )
        min_dr_et2 = ak.argmin(dr_et2.mask[is_duplicate], axis=-1, keepdims=True)
        is_min = ak.fill_none(
            ak.flatten(min_dr_et2[:, None] == fsr_photons.helper_idx, axis=-1), False
        )
        fsr_photons["lepton_idx"] = ak.where(
            ~is_duplicate | (is_duplicate & is_min), fsr_photons.lepton_idx, -1
        )

        # Extract temporary fsr mapping indices
        leptons_pre = assign_lepton_fsr_idx(fsr_photons, leptons_pre)
        computed_fsr_idx = leptons_pre.fsr_idx

        # 4. FSR isolation subtraction for Muons
        selected_fsr_photons = fsr_photons[fsr_photons.lepton_idx > -1]
        muons_matchedfsr_cartesian = ak.cartesian(
            {"muon": muons, "fsr": selected_fsr_photons}, nested=True, axis=1
        )
        muons_matchedfsr_cartesian_dr = muons_matchedfsr_cartesian.muon.delta_r(
            muons_matchedfsr_cartesian.fsr
        )
        muons_matchedfsr_cartesian_dr_valid = (muons_matchedfsr_cartesian_dr > 0.01) & (
            muons_matchedfsr_cartesian_dr < 0.3
        )
        valid_matchedfsr_pt = ak.sum(
            # muons_matchedfsr_cartesian.fsr.mask[muons_matchedfsr_cartesian_dr_valid].pt,
            ak.mask(
                muons_matchedfsr_cartesian.fsr, muons_matchedfsr_cartesian_dr_valid
            ).pt,
            axis=-1,
        )
        muon_corrected_iso = muons.pfRelIso03_all - (valid_matchedfsr_pt / muons.pt)
        muons["pfRelIso03_all"] = ak.where(
            muon_corrected_iso > 0, muon_corrected_iso, 0.0
        )
        muons["is_tight"] = muons.is_tight & (muons.pfRelIso03_all < 0.35)
        electrons["pfRelIso03_all"] = ak.zeros_like(electrons.pt)

        # 5. Re-merge collections (Order matches original sort paths perfectly)
        leptons = ak.concatenate([muons, electrons], axis=1)
        leptons = leptons[ak.argsort(leptons.pt, axis=1, ascending=False)]

        # Inject the calculated fsr mapping back safely onto the final array
        leptons["fsr_idx"] = computed_fsr_idx

        # Extract matching photons cleanly via mapped tracking.
        # NOTE: awkward 1.x cannot slice with an option-typed (masked) integer
        # array, so we index with a clamped plain-int array and apply the mask
        # to the *result* instead. pad_none guards events with no FSR photon
        # (where the clamped index 0 would otherwise be out of bounds).
        valid_fsr = ak.fill_none(leptons.fsr_idx >= 0, False)
        safe_fsr_idx = ak.values_astype(
            ak.fill_none(ak.where(valid_fsr, leptons.fsr_idx, 0), 0), np.int64
        )
        fsr_photons_padded = ak.pad_none(fsr_photons, 1, axis=1)
        matched_photons = ak.mask(fsr_photons_padded[safe_fsr_idx], valid_fsr)

        leptons = ak.zip(
            {
                "pt": leptons.pt,
                "eta": leptons.eta,
                "phi": leptons.phi,
                "mass": leptons.mass,
                "charge": leptons.charge,
                "pdgId": leptons.pdgId,
                "pfRelIso03_all": leptons.pfRelIso03_all,
                "idx": ak.local_index(leptons, axis=1),
                "fsr_idx": leptons.fsr_idx,
                "is_loose": leptons.is_loose,
                "is_relaxed": leptons.is_relaxed,
                "is_tight": leptons.is_tight,
                "lostHits": leptons.lostHits,
                "sip3d": leptons.sip3d,
                "mvaHZZIso": leptons.mvaHZZIso,
                "isPFcand": leptons.isPFcand,
                "highPtId": leptons.highPtId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

        # 6. Four-vector reconstruction (add matched FSR photon when present).
        # Build p4 as a single, uniformly typed four-vector. Use the plain
        # LorentzVector behavior (NOT candidate): p4 is only kinematics, and
        # the candidate behavior's add() requires a 'charge' field, which a
        # bare four-vector doesn't carry. Charge/pdgId stay on the lepton
        # record itself, which is what every downstream cut actually uses.
        has_fsr = ~ak.is_none(matched_photons, axis=1)
        combined = leptons + matched_photons  # None where there is no matched photon
        leptons["p4"] = ak.zip(
            {
                "pt": ak.where(has_fsr, ak.fill_none(combined.pt, 0.0), leptons.pt),
                "eta": ak.where(has_fsr, ak.fill_none(combined.eta, 0.0), leptons.eta),
                "phi": ak.where(has_fsr, ak.fill_none(combined.phi, 0.0), leptons.phi),
                "mass": ak.where(
                    has_fsr, ak.fill_none(combined.mass, 0.0), leptons.mass
                ),
            },
            with_name="PtEtaPhiMLorentzVector",
            behavior=candidate.behavior,
        )
        self.objects[obj_name] = leptons

    def select_zcandidates(self, obj_name):
        """selects Z candidates for SR and all CRS"""
        zcand = ak.combinations(self.objects["leptons"], 2, fields=["l1", "l2"])
        zcand = zcand[zcand.l1.is_relaxed & zcand.l2.is_relaxed]
        zcand["p4"] = zcand.l1.p4 + zcand.l2.p4
        zcand["pt"] = zcand.p4.pt
        zcand["idx"] = ak.local_index(zcand, axis=1)
        self.objects[obj_name] = zcand

    def select_best_zcandidate(self, obj_name):
        """selects best Z candidate as a flattened non-jagged option-type"""
        zmass = 91.1876
        best_zcand_idx = ak.argmin(
            np.abs(self.objects["zcandidates"].p4.mass - zmass), axis=1, keepdims=True
        )
        best_zcand = self.objects["zcandidates"][best_zcand_idx]
        self.objects[obj_name] = ak.firsts(best_zcand)

    def select_other_relaxed_leptons(self, obj_name):
        """
        selects additional relaxed leptons in the Z+L CR matching the exact physics
        vetos and event criteria of the historic baseline producer framework.
        """
        best_z = self.objects["best_zcandidates"]
        relaxed_leptons = self.objects["leptons"][self.objects["leptons"].is_relaxed]

        # 1. Clean track matching: Drop probe if it's part of the Z pair
        relaxed_leptons_bestzl1_idx = ak.fill_none(
            relaxed_leptons.idx != best_z.l1.idx, True
        )
        relaxed_leptons_bestzl2_idx = ak.fill_none(
            relaxed_leptons.idx != best_z.l2.idx, True
        )
        relaxed_leptons = relaxed_leptons[
            relaxed_leptons_bestzl1_idx & relaxed_leptons_bestzl2_idx
        ]

        # 2. Ghost removal (ΔR > 0.02)
        dr1 = ak.fill_none(relaxed_leptons.delta_r(best_z.l1), 999)
        dr2 = ak.fill_none(relaxed_leptons.delta_r(best_z.l2), 999)
        relaxed_leptons = relaxed_leptons[(dr1 > 0.02) & (dr2 > 0.02)]

        # 3. QCD suppression veto format: Fail only if OS AND mass <= 4 GeV
        m_l1 = ak.fill_none((relaxed_leptons.p4 + best_z.l1.p4).mass, 0)
        m_l2 = ak.fill_none((relaxed_leptons.p4 + best_z.l2.p4).mass, 0)

        opp_charge_l1 = ak.fill_none(relaxed_leptons.charge != best_z.l1.charge, False)
        opp_charge_l2 = ak.fill_none(relaxed_leptons.charge != best_z.l2.charge, False)

        fails_qcd_l1 = opp_charge_l1 & (m_l1 <= 4)
        fails_qcd_l2 = opp_charge_l2 & (m_l2 <= 4)

        # Reverse to form an exact veto check block
        qcd_mask = ~(fails_qcd_l1 | fails_qcd_l2)
        # If there is no best Z in the event, don't apply the QCD veto (keep all).
        # Use a broadcasted OR rather than ak.where: mixing a scalar branch with
        # a jagged branch under a per-event condition builds a union-typed array
        # in awkward 1.x, which can't be used as a slice. The per-event bool
        # broadcasts cleanly across each event's lepton list here.
        qcd_mask = qcd_mask | ak.is_none(best_z)
        relaxed_leptons = relaxed_leptons[qcd_mask]

        # 4. Save clean selection status flags
        relaxed_leptons["pass_selection"] = relaxed_leptons.is_tight
        self.objects[obj_name] = relaxed_leptons

    def select_trilepton(self, obj_name):
        self.objects[obj_name] = (
            self.objects["best_zcandidates"].l1
            + self.objects["best_zcandidates"].l2
            + ak.firsts(self.objects["other_relaxed_leptons"])
        )

    def select_zzcandidates(self, obj_name):
        """selects ZZ candidates for SR and CRs"""
        self.objects[obj_name] = make_cand(
            self.objects["zcandidates"], kind="zz", sort_by_mass=True
        )

    def select_zllcandidates_os(self, obj_name):
        """selects Zll candidates for CRs"""
        self.objects[obj_name] = make_cand(
            self.objects["zcandidates"], kind="zll", sort_by_mass=False, os_method=True
        )

    def select_zllcandidates_ss(self, obj_name):
        """selects Zll candidates for CRs"""
        self.objects[obj_name] = make_cand(
            self.objects["zcandidates"], kind="zll", sort_by_mass=False, os_method=False
        )

    def select_best_zzcandidate(self, obj_name):
        """selects best ZZ candidates for SR"""
        self.objects[obj_name] = select_best_zzcandidate(self.objects["zzcandidates"])

    def select_best_1fcr_zllcandidate(self, obj_name):
        """selects best Zll candidates for 3P1F CR (OS region)"""
        # FIX: was self.objects["zllcandidates"] — that key no longer exists
        # now that the OS and SS control regions are built as two separate
        # objects (zllcandidates_os / zllcandidates_ss) so each can carry its
        # own m4l mass-window cut. 1FCR/2FCR are both OS-flavor regions, so
        # they read from zllcandidates_os.
        self.objects[obj_name] = select_best_zzcandidate(
            self.objects["zllcandidates_os"], "is_1fcr"
        )

    def select_best_2fcr_zllcandidate(self, obj_name):
        """selects best Zll candidates for 2P2F CR (OS region)"""
        # FIX: same as above — reads from zllcandidates_os, not the old
        # unified "zllcandidates" key.
        self.objects[obj_name] = select_best_zzcandidate(
            self.objects["zllcandidates_os"], "is_2fcr"
        )

    def select_best_sscr_zllcandidate(self, obj_name):
        """selects best Zll candidates for SS CR"""
        # FIX: was self.objects["zllcandidates"] — the SS region is built as
        # its own zllcandidates_ss object (separate mass cut from the OS
        # regions), so is_sscr must be evaluated against that collection.
        self.objects[obj_name] = select_best_zzcandidate(
            self.objects["zllcandidates_ss"], "is_sscr"
        )

    def select_best_2fcr_ss_zllcandidate(self, obj_name):
        """selects best Zll candidates for SS CR (legacy is_2fcr_ss definition)"""
        # FIX: was self.objects["zllcandidates"]. Kept for configs that still
        # reference the older exactly-0-tight is_2fcr_ss definition rather
        # than is_sscr; also now reads from zllcandidates_ss.
        self.objects[obj_name] = select_best_zzcandidate(
            self.objects["zllcandidates_ss"], "is_2fcr_ss"
        )

    def select_mass_4e_1fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_3p1f"], "4e"
        )

    def select_mass_2e2mu_1fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_3p1f"], "2e2mu"
        )

    def select_mass_2mu2e_1fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_3p1f"], "2mu2e"
        )

    def select_mass_4mu_1fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_3p1f"], "4mu"
        )

    def select_mass_4e_2fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_2p2f"], "4e"
        )

    def select_mass_2e2mu_2fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_2p2f"], "2e2mu"
        )

    def select_mass_2mu2e_2fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_2p2f"], "2mu2e"
        )

    def select_mass_4mu_2fcr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_2p2f"], "4mu"
        )

    def select_mass_4e_sscr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_sscr"], "4e"
        )

    def select_mass_2e2mu_sscr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_sscr"], "2e2mu"
        )

    def select_mass_2mu2e_sscr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_sscr"], "2mu2e"
        )

    def select_mass_4mu_sscr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zllcandidate_sscr"], "4mu"
        )

    def select_mass_4mu_sr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zzcandidate"], "4mu"
        )

    def select_mass_4e_sr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zzcandidate"], "4e"
        )

    def select_mass_2e2mu_sr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zzcandidate"], "2e2mu"
        )

    def select_mass_2mu2e_sr(self, obj_name):
        self.objects[obj_name] = select_candidate_mass(
            self.objects["best_zzcandidate"], "2mu2e"
        )

    def select_dphi_4e(self, obj_name):
        self.objects[obj_name] = select_candidate_cjet_dphi(
            self.objects["best_zzcandidate"], "4e", self.objects["cjets"]
        )

    def select_dphi_4mu(self, obj_name):
        self.objects[obj_name] = select_candidate_cjet_dphi(
            self.objects["best_zzcandidate"], "4mu", self.objects["cjets"]
        )

    def select_dphi_2mu2e(self, obj_name):
        self.objects[obj_name] = select_candidate_cjet_dphi(
            self.objects["best_zzcandidate"], "2mu2e", self.objects["cjets"]
        )

    def select_dphi_2e2mu(self, obj_name):
        self.objects[obj_name] = select_candidate_cjet_dphi(
            self.objects["best_zzcandidate"], "2e2mu", self.objects["cjets"]
        )

    def select_dphi_inclusive(self, obj_name):
        self.objects[obj_name] = select_candidate_cjet_dphi(
            self.objects["best_zzcandidate"], "inclusive", self.objects["cjets"]
        )

    # --------------------------------------------------------------------------------
    # HWW
    # --------------------------------------------------------------------------------
    def select_hww_leptons(self, obj_name):
        # set 'leptons' by concatenating electrons and muons
        leptons = ak.concatenate(
            [self.objects["muons"], self.objects["electrons"]], axis=1
        )
        leptons = leptons[ak.argsort(leptons.pt, axis=1)]
        self.objects[obj_name] = ak.zip(
            {
                "pt": leptons.pt,
                "eta": leptons.eta,
                "phi": leptons.phi,
                "mass": leptons.mass,
                "charge": leptons.charge,
                "pdgId": leptons.pdgId,
            },
            with_name="PtEtaPhiMCandidate",
            behavior=candidate.behavior,
        )

    def select_hww_zcandidates(self, obj_name):
        zcandidates = ak.combinations(self.objects["leptons"], 2, fields=["l1", "l2"])
        zcandidates = ak.with_field(
            zcandidates,
            zcandidates.l1.pt + zcandidates.l2.pt,
            "pt",
        )
        self.objects["zcandidates"] = zcandidates

    def select_hww_mll(self, obj_name):
        self.objects["mll"] = transverse_mass(
            self.objects["zcandidates"].l1 + self.objects["zcandidates"].l2,
            self.objects["met"],
        )

    def select_hww_ml1(self, obj_name):
        self.objects["ml1"] = transverse_mass(
            self.objects["zcandidates"].l1, self.objects["met"]
        )

    def select_hww_ml2(self, obj_name):
        self.objects["ml2"] = transverse_mass(
            self.objects["zcandidates"].l2, self.objects["met"]
        )

    def select_candidate_cjet(self, obj_name):
        self.objects["candidate_cjet"] = self.objects["cjets"][
            ak.argmax(self.objects["cjets"].btagDeepFlavCvL, axis=1)
            == ak.local_index(self.objects["cjets"], axis=1)
        ]
