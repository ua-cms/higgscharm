"""
2026-11-03: HZZ lepton scale factors on the four leptons of the selected ZZ (or Z+ll CR) candidate.

Replaces the muon POG loose-ID SF (corrections/muon.py) and the per-electron EGM reco loop
(corrections/electron.py) for the HZZ workflows. Those act on events.selected_muons/electrons, i.e. on
EVERY lepton in the event (the object selection only adds is_loose/is_tight flags), so events with
extra leptons picked up extra SF factors; and no SF existed for the HZZ selection itself.

Per lepton of the best candidate (z1.l1, z1.l2, z2.l1, z2.l2):
  muons     : HZZ muon SF (CJLST Run-3 "FINAL"/"ERROR", ID + SIP)                    -> CMS_eff_m_id_<year>
  electrons : HZZ electron selection SF (CJLST Run-3, BDT ID + SIP; gap / BPix-hole
              regions as LeptonSFHelper.cc)                                          -> CMS_eff_e_id_<year>
              EGM reco SF (electron.json, below20 / 20to75 / above75, SC eta, phi
              for 2023)                                                              -> CMS_eff_e_reco_<range>_<year>
The event weight is the product over the leptons; Up/Down move every lepton of a flavour together
(fully correlated, as CJLST). Maps: analysis/data/lepton_hzz/hzz_lepton_sf_<era>.json
(scripts/make_hzz_lepton_sf_json.py). SC eta = eta + deltaEtaSC (selections/object_selections.py).
"""
from pathlib import Path

import awkward as ak
import correctionlib
import numpy as np

from analysis.corrections.utils import correction_files

HZZ_SF_DIR = Path(__file__).resolve().parent.parent / "data" / "lepton_hzz"

EGM_YEAR = {
    "2022postEE": "2022Re-recoE+PromptFG",
    "2022preEE": "2022Re-recoBCD",
    "2023preBPix": "2023PromptC",
    "2023postBPix": "2023PromptD",
    "2024": "2024Prompt",  # 2026-11-09; EGM 2024 json has no phi input
    "2025": "2024Prompt",  # 2026-11-09: 2025/2026 use the 2024 maps (HZZ + EGM reco), as CJLST LeptonSFHelper.cc
    "2026": "2024Prompt",  # (year >= 2025: "SFs not yet available, using 2024 ones"; the 2025 EGM json has no RecoBelow20)
}
# era whose HZZ SF json / EGM reco file is used
SF_ERA = {"2025": "2024", "2026": "2024"}
# EGM reco pT ranges; electrons with 7 < pT < 10 GeV take the lowest bin (as CJLST, whose low-pT
# reco map has one pT bin), instead of no SF
RECO_RANGES = {
    "RecoBelow20": (0.0, 20.0, 10.0, 19.999),
    "Reco20to75": (20.0, 75.0, 20.0, 74.999),
    "RecoAbove75": (75.0, np.inf, 75.0, 1.0e4),
}
RECO_NAMES = {"RecoBelow20": "below20", "Reco20to75": "20to75", "RecoAbove75": "above75"}
CAND_FIELDS = (
    "selected_best_zzcandidate",
    "selected_best_1fcr_zllcandidate_os", "selected_best_2fcr_zllcandidate_os",
    "selected_best_sscr_zllcandidate_ss",
    "selected_best_1fcr_zllcandidate", "selected_best_2fcr_zllcandidate", "selected_best_sscr_zllcandidate",
)


def candidate_leptons(events):
    """(n_events, 4) leptons of the best candidate; raises if the event has none."""
    field = next((f for f in CAND_FIELDS if f in events.fields), None)
    if field is None:
        raise KeyError(f"hzz_lepton: no best-candidate field among {CAND_FIELDS}")
    cand = ak.firsts(events[field])
    if ak.any(ak.is_none(cand)):
        raise ValueError(f"hzz_lepton: events without a {field}")
    return ak.concatenate(
        [cand.z1.l1[:, None], cand.z1.l2[:, None], cand.z2.l1[:, None], cand.z2.l2[:, None]], axis=1
    ), field


class HZZLeptonWeights:
    def __init__(self, events, weights, year, variation="nominal"):
        if year not in EGM_YEAR:
            raise ValueError(f"hzz_lepton: no HZZ lepton SFs for {year}")
        self.weights = weights
        self.year = year
        self.variation = variation
        self.leptons, self.cand_field = candidate_leptons(events)
        sf_era = SF_ERA.get(year, year)
        cset = correctionlib.CorrectionSet.from_file(str(HZZ_SF_DIR / f"hzz_lepton_sf_{sf_era}.json"))
        self.mu_sf = cset["HZZ-Muon-SF"]
        self.ele_sf = cset["HZZ-Electron-ID-SF"]
        self.egm = correctionlib.CorrectionSet.from_file(correction_files["electron_reco"][sf_era])["Electron-ID-SF"]

        flat = ak.flatten(self.leptons)
        self.n = len(self.leptons)
        self.apid = np.abs(ak.to_numpy(flat.pdgId))
        self.pt = ak.to_numpy(flat.pt).astype(np.float64)
        self.eta = ak.to_numpy(flat.eta).astype(np.float64)
        self.phi = ak.to_numpy(flat.phi).astype(np.float64)
        self.sceta = self.eta + ak.to_numpy(flat.deltaEtaSC).astype(np.float64)
        if not np.all(np.isin(self.apid, (11, 13))):
            raise ValueError("hzz_lepton: candidate lepton that is neither e nor mu")

    def _event_product(self, per_lepton):
        return np.prod(per_lepton.reshape(self.n, 4), axis=1)

    def _add(self, name, nom, up, down):
        if self.variation == "nominal":
            self.weights.add(name=name, weight=nom, weightUp=up, weightDown=down)
        else:
            self.weights.add(name=name, weight=nom)

    def _muon(self, syst):
        sf = np.ones_like(self.pt)
        m = self.apid == 13
        if m.any():
            sf[m] = self.mu_sf.evaluate(syst, self.eta[m], np.minimum(self.pt[m], 199.0))
        return self._event_product(sf)

    def _ele_region(self):
        region = np.full(self.pt.shape, "main", dtype=object)
        abs_sceta = np.abs(self.sceta)
        # gap map for 2023 and 2024 only (LeptonSFHelper.cc applyGapSF: theYear == 2023 || theYear == 2024, so not
        # for 2025/2026 even though they use the 2024 maps); 2024 has no BPix hole
        if self.year.startswith("2023") or self.year == "2024":
            gap = (abs_sceta > 1.444) & (abs_sceta < 1.556) & (self.pt < 20.0)
            region[gap] = "gap"
        if self.year == "2023postBPix":
            hole = (self.sceta > -1.5) & (self.sceta < 0.0) & (self.phi > -1.2) & (self.phi < -0.8)
            region[hole & (region == "main")] = "hole"
        return region

    def _electron_id(self, syst):
        sf = np.ones_like(self.pt)
        e = self.apid == 11
        if e.any():
            region = self._ele_region()[e]
            pt = np.minimum(self.pt[e], 499.0)
            vals = np.empty(e.sum())
            for reg in np.unique(region):
                r = region == reg
                vals[r] = self.ele_sf.evaluate(str(reg), syst, self.sceta[e][r], pt[r])
            sf[e] = vals
        return self._event_product(sf)

    def _electron_reco(self, reco_range, syst):
        lo, hi, clip_lo, clip_hi = RECO_RANGES[reco_range]
        sf = np.ones_like(self.pt)
        sel = (self.apid == 11) & (self.pt >= lo) & (self.pt < hi)
        if sel.any():
            args = [EGM_YEAR[self.year], syst, reco_range, self.sceta[sel], np.clip(self.pt[sel], clip_lo, clip_hi)]
            if self.year.startswith("2023"):
                args.append(self.phi[sel])
            sf[sel] = self.egm.evaluate(*args)
        return self._event_product(sf)

    def add_all(self):
        y = self.year[:4]
        self._add(f"CMS_eff_m_id_{y}", self._muon("nominal"), self._muon("up"), self._muon("down"))
        self._add(f"CMS_eff_e_id_{y}", self._electron_id("nominal"), self._electron_id("up"), self._electron_id("down"))
        for reco_range, short in RECO_NAMES.items():
            self._add(
                f"CMS_eff_e_reco_{short}_{y}",
                self._electron_reco(reco_range, "sf"),
                self._electron_reco(reco_range, "sfup"),
                self._electron_reco(reco_range, "sfdown"),
            )
