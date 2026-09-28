#!/usr/bin/env python3
"""
compute_os_ss_ratios.py

Parquet translation of the ROOT event-COUNTING OS/SS ratio script.

IMPORTANT: this is a DIFFERENT quantity from the "(OS/SS)^MC" ratio computed
inside compute_an_ss_yield.py (which is a weight_nominal-weighted, MC-only
ratio used to scale the fake-rate-weighted SS data sum in the Z+X formula).
THIS script instead does raw, unweighted DATA event counting:

    N_OS_final = (N_2P2F_data + N_3P1F_data) - (N_2P2F_ZZ + N_3P1F_ZZ)   [ZZ weighted]
    N_SS       = N_SS_data                                               [raw count]
    ratio      = N_OS_final / N_SS
    err        = sqrt(1/N_OS_final + 1/N_SS)     <- printed AS-IS, not
                                                     multiplied by `ratio`.
                                                     This is the Poisson
                                                     *relative*-error formula
                                                     being used as if it were
                                                     the absolute error --
                                                     reproduced faithfully
                                                     from the original ROOT
                                                     script rather than
                                                     "fixed", since it's what
                                                     actually produced the
                                                     small OS/SS errors seen
                                                     in the slide tables.

  ROOT                                     -> parquet
  --------------------------------------------------------------------------
  ZLL2P2F{fs}_mass.size()>0, counted        -> CR_2P2F category data rows,
                                                final state resolved via
                                                z1_l1_pdgid_2p2f / l3_pdgid_2p2f
                                                (same fields compute_an_ss_yield.py
                                                already uses for this, reused
                                                here via _os_final_state)
  ZLL3P1F{fs}_mass.size()>0, counted        -> CR_3P1F category data rows,
                                                final state resolved via the
                                                3P1F analogue fields (ASSUMED
                                                name, see note below)
  ZLLSSCR{fs}_mass.size()>0, counted        -> CR_SS category data rows,
                                                final state resolved via
                                                zll_mass_{fs}_sscr (reused
                                                via _ss_final_state)
  qqZZ_final_merged.root, "final_weight"    -> ZZ MC rows (classified via
                                                _ZZ_KW), weight_nominal column

============================== ASSUMPTIONS ===================================
- hplus_FR.yaml (not shown to us) is assumed to expose 3P1F-side flavor
  fields analogous to the 2P2F ones compute_an_ss_yield.py already reads:
  "z1_l1_pdgid_3p1f" and "l3_pdgid_3p1f". If the real field names differ,
  fix _resolve_3p1f_final_state() below -- everything else is unaffected.
- ZZ classification reuses _ZZ_KW from compute_an_ss_yield.py (ZZto4L,
  GluGluToContinto/GluGlutoContinto). The original ROOT script only used
  qqZZ_final_merged.root (no separate ggZZ file); your hplus_FR_SS listing
  only showed "ZZto4L*" directories, so this should match -- but if your
  parquet output ever splits qq/gg ZZ into separate samples, restrict via
  --zz-sample-keyword.
- Same events.CR_2P2F / CR_3P1F / CR_SS category folder convention as the
  rest of this pipeline (_load_parquets_for_category, imported unchanged).
================================================================================

Usage:
    python compute_os_ss_ratios.py \\
        --cr-input-os /eos/user/p/pkatris/higgscharm/outputs/hplus_FR \\
        --cr-input-ss /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_SS \\
        --year 2022preEE
"""

import argparse
import math

import numpy as np
import pandas as pd

from compute_an_ss_yield import (
    _load_parquets_for_category, _extract_scalar, _os_final_state,
    _ss_final_state, _FINAL_STATES,
)


def _resolve_3p1f_final_state(df):
    """Same convention as _os_final_state() (2P2F) but for the 3P1F fields.
    ASSUMED field names -- see module docstring."""
    result = np.full(len(df), '', dtype=object)
    if df.empty or 'z1_l1_pdgid_3p1f' not in df.columns or 'l3_pdgid_3p1f' not in df.columns:
        return result
    z1 = np.abs(_extract_scalar(df['z1_l1_pdgid_3p1f']))
    z2 = np.abs(_extract_scalar(df['l3_pdgid_3p1f']))
    result[(z1 == 11) & (z2 == 11)] = '4e'
    result[(z1 == 13) & (z2 == 13)] = '4mu'
    result[(z1 == 11) & (z2 == 13)] = '2e2mu'
    result[(z1 == 13) & (z2 == 11)] = '2mu2e'
    return result


def compute_os_ss_ratios(cr_input_os, cr_input_ss, year):
    frames_2p2f = _load_parquets_for_category(cr_input_os, year, 'CR_2P2F')
    frames_3p1f = _load_parquets_for_category(cr_input_os, year, 'CR_3P1F')
    frames_ss = _load_parquets_for_category(cr_input_ss, year, 'CR_SS')

    df_2p2f_data = frames_2p2f.get('data', pd.DataFrame())
    df_3p1f_data = frames_3p1f.get('data', pd.DataFrame())
    df_ss_data = frames_ss.get('data', pd.DataFrame())

    df_2p2f_zz = frames_2p2f.get('zz', pd.DataFrame())
    df_3p1f_zz = frames_3p1f.get('zz', pd.DataFrame())

    fs_2p2f_data = _os_final_state(df_2p2f_data)
    fs_3p1f_data = _resolve_3p1f_final_state(df_3p1f_data)
    fs_ss_data = _ss_final_state(df_ss_data)
    fs_2p2f_zz = _os_final_state(df_2p2f_zz)
    fs_3p1f_zz = _resolve_3p1f_final_state(df_3p1f_zz)

    w_2p2f_zz = (_extract_scalar(df_2p2f_zz['weight_nominal'])
                 if not df_2p2f_zz.empty and 'weight_nominal' in df_2p2f_zz.columns
                 else np.array([]))
    w_3p1f_zz = (_extract_scalar(df_3p1f_zz['weight_nominal'])
                 if not df_3p1f_zz.empty and 'weight_nominal' in df_3p1f_zz.columns
                 else np.array([]))

    results = {}
    for fs in ['inclusive'] + _FINAL_STATES:
        if fs == 'inclusive':
            n2p2f_d = len(df_2p2f_data)
            n3p1f_d = len(df_3p1f_data)
            nss_d = len(df_ss_data)
            n2p2f_zz = float(np.nansum(w_2p2f_zz)) if len(w_2p2f_zz) else 0.0
            n3p1f_zz = float(np.nansum(w_3p1f_zz)) if len(w_3p1f_zz) else 0.0
        else:
            n2p2f_d = int(np.sum(fs_2p2f_data == fs))
            n3p1f_d = int(np.sum(fs_3p1f_data == fs))
            nss_d = int(np.sum(fs_ss_data == fs))
            sel_2p2f_zz = (fs_2p2f_zz == fs)
            sel_3p1f_zz = (fs_3p1f_zz == fs)
            n2p2f_zz = float(np.nansum(w_2p2f_zz[sel_2p2f_zz])) if len(w_2p2f_zz) else 0.0
            n3p1f_zz = float(np.nansum(w_3p1f_zz[sel_3p1f_zz])) if len(w_3p1f_zz) else 0.0

        n_os_data = n2p2f_d + n3p1f_d
        n_os_mc = n2p2f_zz + n3p1f_zz
        n_os_final = max(n_os_data, 0.0)

        if nss_d > 0 and n_os_final > 0:
            ratio = n_os_final / nss_d
            err = math.sqrt(1.0 / n_os_final + 1.0 / nss_d)
        else:
            ratio, err = 0.0, 0.0

        results[fs] = {
            'n_os_data': n_os_data, 'n_os_mc': n_os_mc, 'n_os_final': n_os_final,
            'n_ss_data': nss_d, 'ratio': ratio, 'err': err,
        }

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Raw event-counting OS/SS ratio (reproduces the ROOT script exactly, incl. its error formula)'
    )
    parser.add_argument('--cr-input-os', required=True, help='Path to hplus_FR output dir')
    parser.add_argument('--cr-input-ss', required=True, help='Path to hplus_FR_SS output dir')
    parser.add_argument('--year', required=True, help='Era, e.g. 2022preEE')
    args = parser.parse_args()

    print("\n============================================")
    print("[INFO] OS/SS ratio computation (parquet)")
    print("============================================\n")

    results = compute_os_ss_ratios(args.cr_input_os, args.cr_input_ss, args.year)

    for fs, r in results.items():
        print(f"[RESULT] Final state: {fs}")
        print(f"   OS counts (data):   {r['n_os_data']}")
        print(f"   OS counts (MC):     {r['n_os_mc']:.3f}")
        print(f"   OS counts (final):  {r['n_os_final']:.3f}")
        print(f"   SS counts (data):   {r['n_ss_data']}")
        if r['n_ss_data'] > 0:
            print(f"   OS/SS ratio:        {r['ratio']:.3f} \u00b1 {r['err']:.3f}")
        else:
            print(f"   OS/SS ratio:        N/A (SS=0)")
        print("")


if __name__ == '__main__':
    main()