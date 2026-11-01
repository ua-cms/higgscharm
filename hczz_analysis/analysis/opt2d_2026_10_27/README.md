# 2026-10-27: joint (m4l, MVA score) binning for the 2D HcZZ card (user: 2D binned first, then optimised, unbinned later)

Model: no-m4l 90-160 (nom4l9016), fixed scorer (`_scored_nom4l_jetfix`), same builder chain/options as the 1D/2D
cards of ../nom4l_card_2026_10_27/pipeline.sh. Reference limits: 1D S10 521.0, hand 2D 9x4 467.0.

1. `dump_events_2d.py` (condor 16525580): card-K builder dump with the score replaced by (i_m4l + s)/220 on a 0.25 GeV
   m4l grid -> `nom4l9016/events_2d.npz` (26.6M events; decoded yields: Z+X 35.86, qqZZ 38.4, ggH 35.6, signal 0.037).
2. `optimise_2d.py`: fine grid 55 (1 GeV m4l) x 40 (S+B score quantiles); FoM = sum Asimov Z^2 with bkg MC stat; per
   cell b >= 1, MC rel <= 0.2, Z+X >= 0 (Z+X factorised + running-mean smoothed for the optimisation).
   rect = rectangular grid (coordinate ascent, exact DP per axis); cat = score categories with own m4l edges (nested DP,
   bin budget). --half: optimise on half, evaluate on the other half.
   sumZ2 x1e-5 (full | half opt / eval): 1D-10: 2.68 | 2.81/2.63; hand 9x4: infeasible (eval 3.47, 19 cells fail the
   constraints); rect 3x3: 3.24 | 3.32/3.00; rect 3x4: 3.36 | 3.26/2.85; cat T10: 3.37 | 3.43/2.97; cat T15: 3.44 |
   3.47/3.06; cat T20 (20 bins): 3.46 | 3.47/3.08. Half-split loss ~10% = spread between layouts -> decide on Combine.
3. Combine (condor 16525625, `~/condor_hczz_2026_10_27_opt2d/layout.sh`, builder `create_datacards_unroll_2d.py`
   with the new score-major `--unroll-score-cats`): rect3x3, rect3x4, rect5x3, cat10, cat15, cat20 ->
   `nom4l9016/limits.txt`.

## Combine results round 1 (constraints b >= 1, MC rel <= 0.2) — expected r
hand 9x4 (36 bins) 467.0 | cat20 469.0 | cat15 470.5 | cat10 476.0 | rect3x4 480.0 | rect5x3 481.0 | rect3x3 483.0 | 1D S10 521.0
-> none beats the hand layout. The hand layout violates those constraints in 19/36 cells yet is best: autoMCStats
handles MC stat in the fit, the constraints blocked the 2 GeV m4l bins around the peak.
## Round 2: relaxed constraints (opt_b0.3_r0.35*, opt_b0.1_r0.5*; km 16), half-split eval sumZ2 x1e-5:
b0.3/0.35: rect3x7 3.46, cat30 3.54, cat40 3.57; b0.1/0.5: rect5x8 3.68, cat30 3.57, cat40 3.68 (hand eval 3.47)
Combine: condor 16525682 (layouts2.txt) -> nom4l9016/limits.txt.
Round 2 Combine (expected r): b0.1/0.5: cat40 444.5, rect5x8 447.5, cat30 448.0; b0.3/0.35: cat40 449.5, cat30 450.0,
rect3x7 456.0. BEST: b0p1r0p5cat40 = 444.5 (hand 2D 467.0 -4.8%; 1D 521 -14.7%; production MVA 1D 426 +4.3%).
Layout: nom4l9016/layout_b0p1r0p5cat40.json; card nom4l9016/card_b0p1r0p5cat40.
Caveat: edges tuned on the same MC as the card; the half-split (independent half) still ranks it above the hand
layout (3.68 vs 3.47 x1e-5), so the gain is not only MC fluctuation.
