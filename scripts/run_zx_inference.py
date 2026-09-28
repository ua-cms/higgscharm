#!/usr/bin/env python3
"""
run_mva_inference.py

Run MVA inference on the Z+X background parquet files produced by
compute_zx_parquets.py. Scores each event with the trained MVA, producing
mva_score_* columns for combine histograms.

This is a direct continuation of compute_zx_parquets.py — it expects exactly
the files that script writes:
    <input>/<era>/zx_3p1f_<era>.parquet
    <input>/<era>/zx_2p2f_<era>.parquet

Usage:
    python run_mva_inference.py \\
        --input /eos/user/p/pkatris/Analysis/zx_background \\
        --config /eos/home-p/pkatris/b-hive_ttcc/config/hc_zzto4l_mw_training_4class_nomass.yml \\
        --model /eos/home-p/pkatris/b-hive_ttcc/output/TrainingTask/hc_zzto4l_mw_training_4class_nomass/.../best_model.pt \\
        --output /eos/user/p/pkatris/Analysis/zx_background_mva
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

HIGGSCHARM_DEFAULT = '/afs/cern.ch/user/p/pkatris/higgscharm'

ERAS = ['2022postEE', '2022preEE', '2023preBPix', '2023postBPix']

# Column name mapping: Z+X parquet name -> MVA expected name.
# These match the branch names in the merged hplus_FR_full config exactly
# (m4l_3p1f, pT_4l_3p1f, z1_pt_3p1f, deltaR_ZZ_3p1f, z1_l1_pt_3p1f, ... all
# present there), so no renaming gaps are expected against that config.
COL_MAP_3P1F = {
    'm4l_3p1f':          'm4l',
    'pT_4l_3p1f':        'pT_4l',
    'eta_4l_3p1f':       'eta_4l',
    'phi_4l_3p1f':       'phi_4l',
    'z1_mass_3p1f':      'z1_mass',
    'z2_mass_3p1f':      'z2_mass',
    'z1_pt_3p1f':        'z1_pt',
    'z1_eta_3p1f':       'z1_eta',
    'z1_phi_3p1f':       'z1_phi',
    'z2_pt_3p1f':        'z2_pt',
    'z2_eta_3p1f':       'z2_eta',
    'z2_phi_3p1f':       'z2_phi',
    'deltaR_ZZ_3p1f':    'deltaR_ZZ',
    'z1_l1_pt_3p1f':     'z1_l1_pt',
    'z1_l1_eta_3p1f':    'z1_l1_eta',
    'z1_l1_phi_3p1f':    'z1_l1_phi',
    'z1_l2_pt_3p1f':     'z1_l2_pt',
    'z1_l2_eta_3p1f':    'z1_l2_eta',
    'z1_l2_phi_3p1f':    'z1_l2_phi',
    'l3_pt_3p1f':        'z2_l1_pt',
    'l3_eta_3p1f':       'z2_l1_eta',
    'l4_pt_3p1f':        'z2_l2_pt',
    'l4_eta_3p1f':       'z2_l2_eta',
    'cjets_h_dphi_3p1f': 'cjets_h_dphi_inclusive',
}

COL_MAP_2P2F = {
    'm4l_2p2f':          'm4l',
    'pT_4l_2p2f':        'pT_4l',
    'eta_4l_2p2f':       'eta_4l',
    'phi_4l_2p2f':       'phi_4l',
    'z1_mass_2p2f':      'z1_mass',
    'z2_mass_2p2f':      'z2_mass',
    'z1_pt_2p2f':        'z1_pt',
    'z1_eta_2p2f':       'z1_eta',
    'z1_phi_2p2f':       'z1_phi',
    'z2_pt_2p2f':        'z2_pt',
    'z2_eta_2p2f':       'z2_eta',
    'z2_phi_2p2f':       'z2_phi',
    'deltaR_ZZ_2p2f':    'deltaR_ZZ',
    'z1_l1_pt_2p2f':     'z1_l1_pt',
    'z1_l1_eta_2p2f':    'z1_l1_eta',
    'z1_l1_phi_2p2f':    'z1_l1_phi',
    'z1_l2_pt_2p2f':     'z1_l2_pt',
    'z1_l2_eta_2p2f':    'z1_l2_eta',
    'z1_l2_phi_2p2f':    'z1_l2_phi',
    'l3_pt_2p2f':        'z2_l1_pt',
    'l3_eta_2p2f':       'z2_l1_eta',
    'l4_pt_2p2f':        'z2_l2_pt',
    'l4_eta_2p2f':       'z2_l2_eta',
    'cjets_h_dphi_2p2f': 'cjets_h_dphi_inclusive',
}

# Columns that are the same name in both CR and MVA inputs. Includes
# 'zx_sign' (+1 for 3P1F, -1 for 2P2F) written by compute_zx_parquets.py,
# used downstream to build the signed combine template.
PASSTHROUGH = [
    'jet_multiplicity', 'n_cjet', 'jet_ht', 'cjet_HT', 'nSV',
    'jet_pt', 'jet_eta', 'jet_btagPNetB', 'jet_btagPNetCvL', 'jet_btagPNetCvB',
    'cjets_pt', 'cjets_eta', 'cjets_btagPNetB', 'cjets_btagPNetCvL', 'cjets_btagPNetCvB',
    'weight_nominal', 'zx_weight', 'zx_weight_up', 'zx_sign',
]


def prepare_df_for_inference(df, col_map):
    """Rename CR columns to MVA-expected names and pass through common columns."""
    out = {}
    for src, dst in col_map.items():
        if src in df.columns:
            out[dst] = df[src]
        else:
            logging.debug(f'  Missing column {src} -> {dst}, will default to 0 in MVA')
    for col in PASSTHROUGH:
        if col in df.columns:
            out[col] = df[col]
    return pd.DataFrame(out)


def run_inference_on_era(era, input_dir, output_dir, processor):
    """Run MVA inference on 3P1F and 2P2F parquets (from compute_zx_parquets.py) for one era."""
    os.makedirs(os.path.join(output_dir, era), exist_ok=True)

    for cr_type, col_map in [('3p1f', COL_MAP_3P1F), ('2p2f', COL_MAP_2P2F)]:
        in_file = os.path.join(input_dir, era, f'zx_{cr_type}_{era}.parquet')
        out_file = os.path.join(output_dir, era, f'zx_{cr_type}_{era}_mva.parquet')

        if not os.path.exists(in_file):
            logging.warning(f'Not found: {in_file}, skipping')
            continue

        logging.info(f'\n--- {era} {cr_type.upper()} ---')
        df_orig = pd.read_parquet(in_file)
        logging.info(f'  Loaded {len(df_orig)} events')

        df_mva = prepare_df_for_inference(df_orig, col_map)

        processor._load_model()
        features = processor.prepare_features(df_mva)
        logging.info(f'  Feature matrix shape: {features.shape}')

        # No mass window filter for Z+X — apply mass window separately
        # (compute_zx_parquets.py's --mass-window controls that upstream)
        result = processor.predict(features)

        for i, cls in enumerate(processor.class_names):
            df_orig[f'mva_score_{cls}'] = result['scores'][:, i]
        df_orig['mva_signal_score'] = result['signal_score']
        df_orig['mva_class_prediction'] = result['class_prediction']

        df_orig.to_parquet(out_file, index=False)
        logging.info(f'  Saved: {out_file}')
        logging.info(f'  Mean signal score: {result["signal_score"].mean():.4f}')
        logging.info(f'  Class distribution: {np.bincount(result["class_prediction"])}')


def main():
    parser = argparse.ArgumentParser(description='Run MVA inference on Z+X parquets from compute_zx_parquets.py')
    parser.add_argument('--input', required=True,
                        help='Directory with zx_3p1f/zx_2p2f parquets (from compute_zx_parquets.py)')
    parser.add_argument('--config', required=True, help='b-hive config YAML')
    parser.add_argument('--model', required=True, help='Path to best_model.pt')
    parser.add_argument('--output', required=True, help='Output directory for MVA-scored parquets')
    parser.add_argument('--higgscharm-path', default=HIGGSCHARM_DEFAULT,
                        help='Path to the higgscharm repo (for MVAPostProcessor import)')
    parser.add_argument('--eras', nargs='+', default=ERAS, help='Eras to process (default: all 4)')
    parser.add_argument('--no-mass-window', action='store_true',
                        help='Skip mass window filter (score all events)')
    args = parser.parse_args()

    if args.higgscharm_path not in sys.path:
        sys.path.insert(0, args.higgscharm_path)
    from analysis.postprocess.mva_inference import MVAPostProcessor

    logging.info(f'Config:  {args.config}')
    logging.info(f'Model:   {args.model}')
    logging.info(f'Input:   {args.input}')
    logging.info(f'Output:  {args.output}')

    processor = MVAPostProcessor(
        bhive_config_path=args.config,
        bhive_model_path=args.model,
        apply_mass_window=not args.no_mass_window,
    )

    for era in args.eras:
        logging.info(f'\n========== {era} ==========')
        run_inference_on_era(era, args.input, args.output, processor)

    logging.info('\nDone!')


if __name__ == '__main__':
    main()