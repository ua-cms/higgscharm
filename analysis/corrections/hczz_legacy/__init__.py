"""
HcZZ object corrections as used for the 2022/2023 H+c(ZZ->4l) production (branch
HcZZ-zx-estimation, 2026-10): JEC/JER nominal and Total-JES/JER shifts
(CMS_scale_j_<year>, CMS_res_j_<year>), muon scale/smearing incl. CMS_scale_m/CMS_res_m
shifts, electron scale/smearing, jet veto maps. Selected by workflows that use the
`corrections: objects:` schema (see correction_manager.object_corrector_manager);
upstream workflows (`corrections: object:`) use analysis/corrections/{jec,muon_ss,...}.py.
Kept so the HcZZ results can be reproduced exactly on coffea 2026; the switch to the
coffea-2026 corrections is done workflow by workflow.
"""
