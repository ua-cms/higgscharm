#!/usr/bin/env python3
"""
Print Z+X background summary table in iBOF page-12 style.

Shows:
  1. Fake rates (electron and muon) per era — barrel / endcap averaged vs pT
  2. Z+X yield per era × final state (4mu, 4e, 2e2mu, 2mu2e) with stat errors

Usage:
    python print_zx_summary_table.py \
        --fake-rates /eos/user/s/snandaku/higgscharm/outputs/fake_rates \
        --zx-input   /eos/user/s/snandaku/Analysis/zx_background \
        [--latex]
"""

import os, argparse, pickle
import numpy as np
import pandas as pd

ERAS = ['2022preEE', '2022postEE', '2023preBPix', '2023postBPix']
ERA_LABELS = {
    '2022preEE':    '2022 preEE',
    '2022postEE':   '2022 postEE',
    '2023preBPix':  '2023 preBPix',
    '2023postBPix': '2023 postBPix',
}
ERA_LUMI = {
    '2022preEE':    7.98,
    '2022postEE':   26.67,
    '2023preBPix':  17.79,
    '2023postBPix': 9.53,
}
FINAL_STATES = ['4mu', '4e', '2e2mu', '2mu2e']
FS_LABELS = {
    '4mu':   r'4\mu' ,
    '4e':    '4e',
    '2e2mu': r'2e2\mu',
    '2mu2e': r'2\mu2e',
}

BARREL_BINS = [0, 1, 2]   # |eta| < 1.5
ENDCAP_BINS = [3, 4]       # 1.5 <= |eta| < 2.5


# ─────────────────────────────────────────────────────────────────────────────
# Fake rate loading
# ─────────────────────────────────────────────────────────────────────────────

def load_fr(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


def barrel_endcap_avg(fr, flavor):
    """Return (pt_labels, barrel_rates, endcap_rates) averaged over eta bins."""
    data = fr[flavor]
    pt_edges = data['pt_edges']
    rates    = data['fake_rate']
    errors   = data.get('fake_rate_err', np.zeros_like(rates))

    pt_labels, barrel, barrel_err, endcap, endcap_err = [], [], [], [], []
    for i in range(len(pt_edges) - 1):
        lo, hi = pt_edges[i], pt_edges[i+1]
        pt_labels.append(f'[{lo:.0f},{hi:.0f})')

        b_rates = rates[i, BARREL_BINS]
        e_rates = rates[i, ENDCAP_BINS]
        b_errs  = errors[i, BARREL_BINS]
        e_errs  = errors[i, ENDCAP_BINS]

        # simple mean over valid (err < 1) bins; fall back to all if none valid
        def weighted_mean(r, e):
            good = e < 1.0
            return float(r[good].mean()) if good.any() else float(r.mean())

        barrel.append(weighted_mean(b_rates, b_errs))
        barrel_err.append(float(np.mean(b_errs[b_errs < 1.0])) if (b_errs < 1.0).any() else 0.0)
        endcap.append(weighted_mean(e_rates, e_errs))
        endcap_err.append(float(np.mean(e_errs[e_errs < 1.0])) if (e_errs < 1.0).any() else 0.0)

    return pt_labels, np.array(barrel), np.array(barrel_err), np.array(endcap), np.array(endcap_err)


# ─────────────────────────────────────────────────────────────────────────────
# Yield loading
# ─────────────────────────────────────────────────────────────────────────────

def load_yields(zx_base):
    """Return dict era -> {fs: (yield, err), 'total': (net, err)}."""
    results = {}
    for era in ERAS:
        f3 = os.path.join(zx_base, era, f'zx_3p1f_{era}.parquet')
        f2 = os.path.join(zx_base, era, f'zx_2p2f_{era}.parquet')
        if not os.path.exists(f3):
            continue
        d3 = pd.read_parquet(f3)
        d2 = pd.read_parquet(f2) if os.path.exists(f2) else pd.DataFrame()

        fs_yield = {}
        fs_err   = {}
        for fs in FINAL_STATES:
            y3 = e3 = y2 = e2 = 0.0
            if 'final_state' in d3.columns and 'zx_weight' in d3.columns:
                m3 = d3['final_state'] == fs
                y3 = float(d3.loc[m3, 'zx_weight'].sum())
                e3 = float(np.sqrt((d3.loc[m3, 'zx_weight_sq'] if 'zx_weight_sq' in d3.columns
                                    else d3.loc[m3, 'zx_weight'] ** 2).sum()))
            if 'final_state' in d2.columns and 'zx_weight' in d2.columns:
                m2 = d2['final_state'] == fs
                y2 = float(d2.loc[m2, 'zx_weight'].sum())   # already negative
                e2 = float(np.sqrt((d2.loc[m2, 'zx_weight_sq'] if 'zx_weight_sq' in d2.columns
                                    else d2.loc[m2, 'zx_weight'] ** 2).sum()))
            net = y3 + y2
            err = float(np.sqrt(e3 ** 2 + e2 ** 2))
            fs_yield[fs] = net
            fs_err[fs]   = err

        net_total = sum(fs_yield.values())
        err_total = float(np.sqrt(sum(e ** 2 for e in fs_err.values())))
        results[era] = {'fs_yield': fs_yield, 'fs_err': fs_err,
                        'net': net_total, 'err': err_total}
    return results


# ─────────────────────────────────────────────────────────────────────────────
# ASCII printing
# ─────────────────────────────────────────────────────────────────────────────

def print_fake_rate_table(all_fr, flavor):
    """Print barrel/endcap average fake rates vs pT for all eras (ASCII)."""
    header = f'  {flavor.upper()} fake rates — barrel (|η|<1.5) and endcap (1.5<|η|<2.5)'
    print('\n' + '─' * 80)
    print(header)
    print('─' * 80)

    pt_labels = None
    barrel_cols, endcap_cols = {}, {}
    for era in ERAS:
        if era not in all_fr:
            continue
        pts, b, _, e, _ = barrel_endcap_avg(all_fr[era], flavor)
        pt_labels = pts
        barrel_cols[era] = b
        endcap_cols[era] = e

    if pt_labels is None:
        print('  [no data]')
        return

    # Header row
    col_w = 16
    hdr = f'  {"pT [GeV]":<12}'
    for era in ERAS:
        lbl = ERA_LABELS[era]
        hdr += f'  {lbl:^{col_w*2+1}}'
    print(hdr)

    sub_hdr = f'  {"":12}'
    for era in ERAS:
        sub_hdr += f'  {"Barrel":>{col_w}}{"Endcap":>{col_w+1}}'
    print(sub_hdr)
    print('  ' + '-' * (12 + (col_w * 2 + 3) * len(ERAS)))

    for i, pt in enumerate(pt_labels):
        row = f'  {pt:<12}'
        for era in ERAS:
            b = barrel_cols.get(era, [0] * len(pt_labels))[i] * 100
            e = endcap_cols.get(era, [0] * len(pt_labels))[i] * 100
            row += f'  {b:>{col_w-1}.1f}%{e:>{col_w}.1f}%'
        print(row)
    print('─' * 80)


def print_yield_table(results, latex=False):
    """Print Z+X yield table per era × final state (iBOF page-12 style)."""
    sep = '─' * 95

    print('\n' + sep)
    print('  Z+X Background Yield (OS fake-rate method, m4l 100–150 GeV, ZZ-corrected)')
    print(sep)

    col_w = 16
    fs_display = {'4mu': '4μ', '4e': '4e', '2e2mu': '2e2μ', '2mu2e': '2μ2e'}

    # Header
    hdr = f'  {"Era":<16}'
    for fs in FINAL_STATES:
        hdr += f'  {fs_display[fs]:^{col_w}}'
    hdr += f'  {"Total":^{col_w}}'
    print(hdr)
    print('  ' + '-' * (16 + (col_w + 2) * 5))

    total_fs  = {fs: 0.0 for fs in FINAL_STATES}
    total_err = {fs: 0.0 for fs in FINAL_STATES}
    total_net, total_net_err = 0.0, 0.0

    for era in ERAS:
        if era not in results:
            print(f'  {ERA_LABELS[era]:<16}  [not found]')
            continue
        r   = results[era]
        row = f'  {ERA_LABELS[era]:<16}'
        for fs in FINAL_STATES:
            y = r['fs_yield'][fs]
            e = r['fs_err'][fs]
            cell = f'{y:.1f} ± {e:.1f}'
            row += f'  {cell:^{col_w}}'
            total_fs[fs]  += y
            total_err[fs] = np.sqrt(total_err[fs] ** 2 + e ** 2)
        net_cell = f'{r["net"]:.1f} ± {r["err"]:.1f}'
        row += f'  {net_cell:^{col_w}}'
        total_net     += r['net']
        total_net_err  = np.sqrt(total_net_err ** 2 + r['err'] ** 2)
        print(row)

    print('  ' + '-' * (16 + (col_w + 2) * 5))
    total_row = f'  {"Total":<16}'
    for fs in FINAL_STATES:
        cell = f'{total_fs[fs]:.1f} ± {total_err[fs]:.1f}'
        total_row += f'  {cell:^{col_w}}'
    total_row += f'  {total_net:.1f} ± {total_net_err:.1f}'
    print(total_row)
    print(sep)


# ─────────────────────────────────────────────────────────────────────────────
# LaTeX printing
# ─────────────────────────────────────────────────────────────────────────────

def latex_fake_rate_table(all_fr, flavor):
    """Return LaTeX tabular string for barrel/endcap fake rates vs pT."""
    n_eras = len(ERAS)
    col_spec = 'l' + 'rr' * n_eras
    lines = [
        r'\begin{table}[htbp]',
        r'\centering',
        r'\small',
        r'\caption{' + flavor.capitalize() + r' fake rates (barrel $|\eta|<1.5$ / endcap $1.5<|\eta|<2.5$) per era.}',
        r'\label{tab:fr_' + flavor + r'}',
        r'\begin{tabular}{' + col_spec + r'}',
        r'\hline',
    ]
    # Era header spanning 2 columns each
    era_header = r'$p_T$ [GeV]'
    for era in ERAS:
        era_header += r' & \multicolumn{2}{c}{' + ERA_LABELS[era].replace(' ', r'\ ') + r'}'
    lines.append(era_header + r' \\')
    # Sub-header barrel/endcap
    sub = ''
    for era in ERAS:
        sub += r' & Barrel & Endcap'
    lines.append(r' & ' + sub.lstrip(' & ') + r' \\')
    lines.append(r'\hline')

    pt_labels = None
    barrel_cols, endcap_cols = {}, {}
    for era in ERAS:
        if era not in all_fr:
            continue
        pts, b, _, e, _ = barrel_endcap_avg(all_fr[era], flavor)
        pt_labels = pts
        barrel_cols[era] = b
        endcap_cols[era] = e

    if pt_labels:
        for i, pt in enumerate(pt_labels):
            row = pt.replace('[', '$[').replace(')', ')$')
            for era in ERAS:
                b = barrel_cols.get(era, [0] * len(pt_labels))[i] * 100
                e = endcap_cols.get(era, [0] * len(pt_labels))[i] * 100
                row += f' & {b:.1f}\\% & {e:.1f}\\%'
            lines.append(row + r' \\')

    lines += [r'\hline', r'\end{tabular}', r'\end{table}', '']
    return '\n'.join(lines)


def latex_yield_table(results):
    """Return LaTeX tabular string for Z+X yields per era × final state."""
    lines = [
        r'\begin{table}[htbp]',
        r'\centering',
        r'\caption{Expected Z+X background yields estimated with the OS fake-rate method',
        r'  ($m_{4\ell} \in [100, 150]$\,GeV, ZZ-corrected). Statistical uncertainties only.}',
        r'\label{tab:zx_yields}',
        r'\begin{tabular}{lrrrrc}',
        r'\hline',
        r'Era & $4\mu$ & $4e$ & $2e2\mu$ & $2\mu2e$ & Total \\',
        r'\hline',
    ]

    total_fs  = {fs: 0.0 for fs in FINAL_STATES}
    total_err = {fs: 0.0 for fs in FINAL_STATES}
    total_net, total_net_err = 0.0, 0.0

    for era in ERAS:
        if era not in results:
            continue
        r = results[era]
        row = ERA_LABELS[era].replace(' ', r'\ ')
        for fs in FINAL_STATES:
            y = r['fs_yield'][fs]
            e = r['fs_err'][fs]
            row += f' & ${y:.1f} \\pm {e:.1f}$'
            total_fs[fs]  += y
            total_err[fs]  = np.sqrt(total_err[fs] ** 2 + e ** 2)
        row += f' & ${r["net"]:.1f} \\pm {r["err"]:.1f}$'
        total_net     += r['net']
        total_net_err  = np.sqrt(total_net_err ** 2 + r['err'] ** 2)
        lines.append(row + r' \\')

    lines.append(r'\hline')
    total_row = r'\textbf{Total}'
    for fs in FINAL_STATES:
        total_row += f' & $\\mathbf{{{total_fs[fs]:.1f} \\pm {total_err[fs]:.1f}}}$'
    total_row += f' & $\\mathbf{{{total_net:.1f} \\pm {total_net_err:.1f}}}$'
    lines += [total_row + r' \\', r'\hline', r'\end{tabular}', r'\end{table}', '']
    return '\n'.join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Three-way comparison (iBOF Table 10 style)
# ─────────────────────────────────────────────────────────────────────────────

# Reference values from iBOF Table 10 (UAntwerp and CJLST)
# Format: era -> fs -> (central, error)
_REF_UANTWERP = {
    '2022preEE':    {'4mu': (6.49, 2.25), '4e': (3.34, 0.18),  '2e2mu': (3.20, 1.31), '2mu2e': (3.85, 0.20)},
    '2022postEE':   {'4mu': (13.22, 4.99),'4e': (10.20, 0.58), '2e2mu': (14.94, 4.34),'2mu2e': (11.59, 0.72)},
    '2023preBPix':  {'4mu': (11.79, 5.65),'4e': (7.16, 0.38),  '2e2mu': (6.64, 4.01), '2mu2e': (9.72, 0.53)},
    '2023postBPix': {'4mu': (5.11, 2.96), '4e': (3.16, 0.18),  '2e2mu': (5.11, 2.67), '2mu2e': (5.71, 0.31)},
}

_REF_CJLST = {
    '2022preEE':    {'4mu': (10.48, 3.85), '4e': (3.85, 1.29),  '2e2mu': (7.11, 2.80),  '2mu2e': (4.66, 1.53)},
    '2022postEE':   {'4mu': (21.91, 7.33), '4e': (10.48, 3.27), '2e2mu': (20.20, 6.69), '2mu2e': (10.09, 3.14)},
    '2023preBPix':  {'4mu': (22.95, 7.87), '4e': (7.30, 2.32),  '2e2mu': (15.25, 5.61), '2mu2e': (9.85, 3.08)},
    '2023postBPix': {'4mu': (13.14, 5.05), '4e': (3.13, 1.08),  '2e2mu': (11.044, 4.16),'2mu2e': (6.45, 2.11)},
}


def _ref_total(ref, era):
    """Sum central values and errors in quadrature across final states."""
    vals = [ref[era][fs] for fs in FINAL_STATES]
    return sum(v[0] for v in vals), float(np.sqrt(sum(v[1]**2 for v in vals)))


def print_comparison_table(results,
                           title='WITH mass window: m4l in [100, 150] GeV',
                           note='ZZ-corrected | stat. unc. only'):
    """
    Print three-way comparison table (iBOF Table 10 style).
    Rows: era × analysis (UAntwerp / CJLST / Ours)
    Columns: 4μ, 4e, 2e2μ, 2μ2e, Total
    """
    sep = '═' * 105
    thin = '─' * 105

    print('\n' + sep)
    print('  Table 10 — Reducible Z+X background: UAntwerp vs CJLST vs This analysis')
    print(f'  {title}')
    print(f'  {note}')
    print(sep)

    col_w = 16
    fs_display = {'4mu': '4μ', '4e': '4e', '2e2mu': '2e2μ', '2mu2e': '2μ2e'}

    # Header
    hdr = f'  {"Era / Analysis":<22}'
    for fs in FINAL_STATES:
        hdr += f'  {fs_display[fs]:^{col_w}}'
    hdr += f'  {"Total":^{col_w}}'
    print(hdr)
    print('  ' + '─' * (22 + (col_w + 2) * 5))

    # Totals accumulators
    tot = {
        'ua':   {fs: (0.0, 0.0) for fs in FINAL_STATES},
        'cj':   {fs: (0.0, 0.0) for fs in FINAL_STATES},
        'ours': {fs: (0.0, 0.0) for fs in FINAL_STATES},
    }

    for era in ERAS:
        print(f'  {thin}')
        print(f'  {ERA_LABELS[era]}  ({ERA_LUMI[era]:.2f} fb⁻¹)')

        # UAntwerp row
        if era in _REF_UANTWERP:
            row = f'  {"  UAntwerp":<22}'
            ref = _REF_UANTWERP[era]
            for fs in FINAL_STATES:
                v, e = ref[fs]
                row += f'  {f"{v:.2f} ± {e:.2f}":^{col_w}}'
                tot['ua'][fs] = (tot['ua'][fs][0] + v,
                                 float(np.sqrt(tot['ua'][fs][1]**2 + e**2)))
            tot_v, tot_e = _ref_total(_REF_UANTWERP, era)
            row += f'  {f"{tot_v:.2f} ± {tot_e:.2f}":^{col_w}}'
            print(row)

        # CJLST row
        if era in _REF_CJLST:
            row = f'  {"  CJLST":<22}'
            ref = _REF_CJLST[era]
            for fs in FINAL_STATES:
                v, e = ref[fs]
                row += f'  {f"{v:.2f} ± {e:.2f}":^{col_w}}'
                tot['cj'][fs] = (tot['cj'][fs][0] + v,
                                 float(np.sqrt(tot['cj'][fs][1]**2 + e**2)))
            tot_v, tot_e = _ref_total(_REF_CJLST, era)
            row += f'  {f"{tot_v:.2f} ± {tot_e:.2f}":^{col_w}}'
            print(row)

        # Our results row
        row = f'  {"  This analysis":<22}'
        if era in results:
            r = results[era]
            for fs in FINAL_STATES:
                y = r['fs_yield'][fs]
                e = r['fs_err'][fs]
                row += f'  {f"{y:.2f} ± {e:.2f}":^{col_w}}'
                tot['ours'][fs] = (tot['ours'][fs][0] + y,
                                   float(np.sqrt(tot['ours'][fs][1]**2 + e**2)))
            row += f'  {("{:.2f} ± {:.2f}".format(r["net"], r["err"])):^{col_w}}'
        else:
            row += '  [not found]'
        print(row)

    # Total rows
    print(f'  {sep}')
    print(f'  {"Total (all eras)":<22}' + ' ' * (col_w + 2) * 5)

    for label, key in [('  UAntwerp', 'ua'), ('  CJLST', 'cj'), ('  This analysis', 'ours')]:
        row = f'  {label:<22}'
        net = 0.0
        net_e = 0.0
        for fs in FINAL_STATES:
            v, e = tot[key][fs]
            row += f'  {f"{v:.2f} ± {e:.2f}":^{col_w}}'
            net   += v
            net_e  = float(np.sqrt(net_e**2 + e**2))
        row += f'  {f"{net:.2f} ± {net_e:.2f}":^{col_w}}'
        print(row)

    print(sep)


def latex_comparison_table(results):
    """Return LaTeX tabular for the three-way comparison."""
    lines = [
        r'\begin{table}[htbp]',
        r'\centering',
        r'\small',
        r'\caption{Contribution of the reducible Z+X background in the signal region',
        r'  ($m_{4\ell} \in [100,150]$\,GeV), estimated with the OS fake-rate method.',
        r'  Results from UAntwerp, CJLST, and this analysis are compared.',
        r'  Statistical uncertainties only.}',
        r'\label{tab:zx_comparison}',
        r'\begin{tabular}{llrrrrc}',
        r'\hline',
        r'Era & Analysis & $4\mu$ & $4e$ & $2e2\mu$ & $2\mu2e$ & Total \\',
        r'\hline',
    ]

    def fmt(v, e):
        return f'${v:.2f} \\pm {e:.2f}$'

    for era in ERAS:
        era_str = ERA_LABELS[era].replace(' ', r'\ ')
        rows_for_era = []

        # UAntwerp
        if era in _REF_UANTWERP:
            ref = _REF_UANTWERP[era]
            tot_v, tot_e = _ref_total(_REF_UANTWERP, era)
            cells = ' & '.join(fmt(*ref[fs]) for fs in FINAL_STATES)
            rows_for_era.append(f'UAntwerp & {cells} & {fmt(tot_v, tot_e)}')

        # CJLST
        if era in _REF_CJLST:
            ref = _REF_CJLST[era]
            tot_v, tot_e = _ref_total(_REF_CJLST, era)
            cells = ' & '.join(fmt(*ref[fs]) for fs in FINAL_STATES)
            rows_for_era.append(f'CJLST & {cells} & {fmt(tot_v, tot_e)}')

        # Ours
        if era in results:
            r = results[era]
            cells = ' & '.join(fmt(r['fs_yield'][fs], r['fs_err'][fs]) for fs in FINAL_STATES)
            rows_for_era.append(f'This analysis & {cells} & {fmt(r["net"], r["err"])}')

        for i, row_content in enumerate(rows_for_era):
            era_col = f'\\multirow{{{len(rows_for_era)}}}{{*}}{{{era_str}}}' if i == 0 else ''
            lines.append(f'{era_col} & {row_content} \\\\')
        lines.append(r'\hline')

    # Totals
    lines.append(r'\hline')
    for label, key, ref_dict in [
        ('UAntwerp',     'ua',   _REF_UANTWERP),
        ('CJLST',        'cj',   _REF_CJLST),
        ('This analysis','ours', None),
    ]:
        if ref_dict is not None:
            fs_cells = []
            tot_v = tot_e = 0.0
            for fs in FINAL_STATES:
                v = sum(_REF_UANTWERP[e][fs][0] if ref_dict is _REF_UANTWERP else _REF_CJLST[e][fs][0]
                        for e in ERAS)
                e = float(np.sqrt(sum(
                    (_REF_UANTWERP[er][fs][1] if ref_dict is _REF_UANTWERP else _REF_CJLST[er][fs][1])**2
                    for er in ERAS)))
                fs_cells.append(fmt(v, e))
                tot_v += v
                tot_e = float(np.sqrt(tot_e**2 + e**2))
            cells = ' & '.join(fs_cells)
            lines.append(f'\\textbf{{Total}} & {label} & {cells} & {fmt(tot_v, tot_e)} \\\\')
        else:
            net = 0.0
            net_e = 0.0
            fs_cells = []
            for fs in FINAL_STATES:
                v = sum(results[er]['fs_yield'][fs] for er in ERAS if er in results)
                e = float(np.sqrt(sum(results[er]['fs_err'][fs]**2 for er in ERAS if er in results)))
                fs_cells.append(fmt(v, e))
                net  += v
                net_e = float(np.sqrt(net_e**2 + e**2))
            cells = ' & '.join(fs_cells)
            lines.append(f'\\textbf{{Total}} & {label} & {cells} & {fmt(net, net_e)} \\\\')

    lines += [r'\hline', r'\end{tabular}', r'\end{table}', '']
    return '\n'.join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fake-rates', required=True,
                        help='Directory containing per-era fake_rates.pkl files')
    parser.add_argument('--zx-input', required=True,
                        help='Z+X parquet output dir from estimate_zx_background.py')
    parser.add_argument('--latex', action='store_true',
                        help='Also print LaTeX table code')
    parser.add_argument('--compare', action='store_true',
                        help='Print three-way comparison table (UAntwerp / CJLST / Ours)')
    parser.add_argument('--zx-nomw', default=None,
                        help='Z+X parquet dir without mass window (for inclusive comparison table)')
    args = parser.parse_args()

    # ── Load fake rates ───────────────────────────────────────────────────────
    all_fr = {}
    for era in ERAS:
        path = os.path.join(args.fake_rates, era, 'fake_rates.pkl')
        if os.path.exists(path):
            all_fr[era] = load_fr(path)
        else:
            print(f'WARNING: fake rates not found for {era}')

    # ── Load yields ───────────────────────────────────────────────────────────
    results = load_yields(args.zx_input)

    # ── Print ASCII tables ────────────────────────────────────────────────────
    print('\n' + '═' * 80)
    print('  Z+X BACKGROUND ESTIMATION SUMMARY  (H+c → ZZ → 4ℓ, Run 3)')
    print('═' * 80)

    for flavor in ['electron', 'muon']:
        print_fake_rate_table(all_fr, flavor)

    print_yield_table(results)

    # ── Three-way comparison ──────────────────────────────────────────────────
    if args.compare:
        print('\n--- With m4l in [100, 150] GeV ---')
        print_comparison_table(results)

        if args.zx_nomw and os.path.exists(args.zx_nomw):
            results_nomw = load_yields(args.zx_nomw)
            print('\n--- Without mass window (inclusive m4l) ---')
            print_comparison_table(results_nomw,
                                   title='WITHOUT mass window (inclusive m4l)',
                                   note='UAntwerp/CJLST reference values are also inclusive')

    # ── Print LaTeX ───────────────────────────────────────────────────────────
    if args.latex:
        print('\n\n% ─── LaTeX tables ─────────────────────────────────────────')
        for flavor in ['electron', 'muon']:
            print(latex_fake_rate_table(all_fr, flavor))
        print(latex_yield_table(results))
        if args.compare:
            print(latex_comparison_table(results))


if __name__ == '__main__':
    main()
