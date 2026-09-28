#!/usr/bin/env python3
"""
Print fake rates as formatted tables for all eras (electron and muon).

Reads fake_rates.pkl files from each era and prints:
  1. Per-era 2D table (pT × |η|) for electrons and muons
  2. Summary table (barrel/endcap averages) across eras

Usage:
    python print_fake_rate_table.py \
        --input /eos/user/s/snandaku/higgscharm/outputs/fake_rates
"""

import os
import pickle
import argparse
import numpy as np

ERAS = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']
ERA_LABELS = {
    '2022preEE':    '2022 preEE',
    '2022postEE':   '2022 postEE',
    '2023preBPix':  '2023 preBPix',
    '2023postBPix': '2023 postBPix',
}

# Barrel: |η| bins 0-2  (|η| < 1.5), Endcap: bins 3-4  (|η| ≥ 1.5)
BARREL_BINS = [0, 1, 2]
ENDCAP_BINS = [3, 4]


def load_fr(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def print_2d_table(fr, flavor, era_label):
    data = fr[flavor]
    pt_edges  = data['pt_edges']
    eta_edges = data['eta_edges']
    rates     = data['fake_rate']
    errors    = data.get('fake_rate_err', np.zeros_like(rates))

    n_pt  = len(pt_edges) - 1
    n_eta = len(eta_edges) - 1

    # Header
    eta_header = '  '.join([f'|η|∈[{eta_edges[j]:.1f},{eta_edges[j+1]:.1f}]' for j in range(n_eta)])
    print(f'\n  {flavor.upper()} fake rates — {era_label}')
    print(f'  {"pT bin [GeV]":<22}  {eta_header}')
    print('  ' + '-'*(22 + 20*n_eta))
    for i in range(n_pt):
        pt_label = f'[{pt_edges[i]:.0f}, {pt_edges[i+1]:.0f})'
        row = '  '.join([
            f'{rates[i,j]*100:5.1f}% ± {errors[i,j]*100:4.1f}%'
            for j in range(n_eta)
        ])
        print(f'  {pt_label:<22}  {row}')


def print_summary_table(all_fr, flavor):
    """Print barrel/endcap average fake rate vs pT for all eras."""
    print(f'\n  {"─"*70}')
    print(f'  {flavor.upper()} — Barrel (|η|<1.5) and Endcap (1.5≤|η|<2.5) averages vs pT')
    print(f'  {"─"*70}')

    # Use first era to get bin edges (same across eras)
    first = list(all_fr.values())[0][flavor]
    pt_edges = first['pt_edges']
    n_pt = len(pt_edges) - 1

    era_list = list(all_fr.keys())

    # Header
    header_parts = []
    for era in era_list:
        lbl = ERA_LABELS.get(era, era)
        header_parts.append(f'{lbl} (B)  {lbl} (E)')
    print(f'  {"pT [GeV]":<20}  ' + '  '.join([f'{ERA_LABELS.get(e,e):>32}' for e in era_list]))
    print(f'  {"":20}  ' + '  '.join(['Barrel     Endcap   ' for _ in era_list]))
    print('  ' + '-'*80)

    for i in range(n_pt):
        pt_label = f'[{pt_edges[i]:.0f},{pt_edges[i+1]:.0f})'
        parts = []
        for era in era_list:
            rates = all_fr[era][flavor]['fake_rate']
            barrel_avg = np.mean(rates[i, BARREL_BINS])
            endcap_avg = np.mean(rates[i, ENDCAP_BINS])
            parts.append(f'{barrel_avg*100:6.1f}%   {endcap_avg*100:6.1f}%')
        print(f'  {pt_label:<20}  ' + '    '.join(parts))
    print(f'  {"─"*70}')


def print_yield_table_from_parquets(zx_base):
    """Print Z+X yield table (net, 3P1F, 2P2F) per era if parquets exist."""
    import pandas as pd

    print('\n\n' + '='*72)
    print('  Z+X Yield Summary (OS method, m4l 100-150 GeV, no c-jet cut on CR)')
    print('='*72)
    print(f'  {"Era":<16}  {"N_3P1F":>8}  {"N_2P2F":>8}  {"Sum 3P1F f/(1-f)":>18}  {"Sum 2P2F":>10}  {"Net Z+X":>10}')
    print('  ' + '-'*72)

    totals = {'n3': 0, 'n2': 0, 'y3': 0.0, 'y2': 0.0, 'net': 0.0}
    for era in ERAS:
        f3 = os.path.join(zx_base, era, f'zx_3p1f_{era}.parquet')
        f2 = os.path.join(zx_base, era, f'zx_2p2f_{era}.parquet')
        if not os.path.exists(f3):
            print(f'  {era:<16}  [not found]')
            continue
        d3 = pd.read_parquet(f3)
        d2 = pd.read_parquet(f2) if os.path.exists(f2) else pd.DataFrame()
        n3 = len(d3)
        n2 = len(d2)
        y3 = float(d3['zx_weight'].sum()) if 'zx_weight' in d3.columns else 0.0
        y2 = float(d2['zx_weight'].abs().sum()) if 'zx_weight' in d2.columns else 0.0
        net = y3 - y2
        totals['n3'] += n3
        totals['n2'] += n2
        totals['y3'] += y3
        totals['y2'] += y2
        totals['net'] += net
        print(f'  {era:<16}  {n3:>8,}  {n2:>8,}  {y3:>18.2f}  {y2:>10.2f}  {net:>10.2f}')
    print('  ' + '-'*72)
    print(f'  {"Total":<16}  {totals["n3"]:>8,}  {totals["n2"]:>8,}  '
          f'{totals["y3"]:>18.2f}  {totals["y2"]:>10.2f}  {totals["net"]:>10.2f}')
    print('='*72)

    # Final state breakdown
    print('\n  Final state breakdown (3P1F + 2P2F combined)')
    final_states = ['4mu', '4e', '2e2mu', '2mu2e']
    print(f'\n  {"Era":<16}  ' + '  '.join([f'{fs:>12}' for fs in final_states]) + f'  {"Total":>10}')
    print('  ' + '-'*72)
    grand = {fs: 0.0 for fs in final_states}
    for era in ERAS:
        f3 = os.path.join(zx_base, era, f'zx_3p1f_{era}.parquet')
        f2 = os.path.join(zx_base, era, f'zx_2p2f_{era}.parquet')
        if not os.path.exists(f3):
            continue
        d3 = pd.read_parquet(f3)
        d2 = pd.read_parquet(f2) if os.path.exists(f2) else pd.DataFrame()

        if 'final_state' not in d3.columns:
            print(f'  {era:<16}  [final_state column missing — run estimate_zx_background.py]')
            continue

        fs_net = {}
        for fs in final_states:
            y3 = float(d3.loc[d3['final_state'] == fs, 'zx_weight'].sum()) if 'zx_weight' in d3.columns else 0.0
            y2 = float(d2.loc[d2['final_state'] == fs, 'zx_weight'].sum()) if ('zx_weight' in d2.columns and 'final_state' in d2.columns and len(d2) > 0) else 0.0
            fs_net[fs] = y3 + y2   # 2P2F zx_weight is already negative
            grand[fs] += fs_net[fs]

        total = sum(fs_net.values())
        row = '  '.join([f'{fs_net[fs]:>12.2f}' for fs in final_states])
        print(f'  {era:<16}  {row}  {total:>10.2f}')

    grand_total = sum(grand.values())
    grand_row = '  '.join([f'{grand[fs]:>12.2f}' for fs in final_states])
    print('  ' + '-'*72)
    print(f'  {"Total":<16}  {grand_row}  {grand_total:>10.2f}')
    print('='*72 + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True,
                        help='Directory containing per-era fake_rates.pkl files')
    parser.add_argument('--zx-output', default=None,
                        help='Z+X parquet output dir (for yield table)')
    parser.add_argument('--detail', action='store_true',
                        help='Print full 2D tables per era per flavor')
    args = parser.parse_args()

    all_fr = {}
    for era in ERAS:
        path = os.path.join(args.input, era, 'fake_rates.pkl')
        if not os.path.exists(path):
            print(f'WARNING: fake rates not found for {era}: {path}')
            continue
        all_fr[era] = load_fr(path)

    print('\n' + '='*72)
    print('  FAKE RATES — All Eras, All Flavors')
    print('='*72)

    # Detail tables
    if args.detail:
        for era, fr in all_fr.items():
            print(f'\n{"="*60}')
            print(f'  Era: {ERA_LABELS.get(era, era)}')
            print(f'{"="*60}')
            for flavor in ['electron', 'muon']:
                print_2d_table(fr, flavor, ERA_LABELS.get(era, era))

    # Summary tables
    for flavor in ['electron', 'muon']:
        print_summary_table(all_fr, flavor)

    # Yield table
    if args.zx_output and os.path.exists(args.zx_output):
        print_yield_table_from_parquets(args.zx_output)
    else:
        print('\n  (Pass --zx-output to also print Z+X yield table)')


if __name__ == '__main__':
    main()
