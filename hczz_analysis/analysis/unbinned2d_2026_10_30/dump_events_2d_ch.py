#!/usr/bin/env python3
"""2026-10-30: per-event (final state, m4l, score) dump for the final-state-split ATLAS-style 2D fit.

Same as ../opt2d_2026_10_27/dump_events_2d.py (card-K builder, identical weights / systematics / JES-JER and
muon-shift samples / Z+X), with the final state added to the encoding:

    v = (c * N + i + s) / (3 N),  c = 0 (4e), 1 (4mu), 2 (2e2mu: both mixed orderings),
    i = index of m4l on FINE (105..160 GeV, 0.25 GeV, N = 220), s = mva_score_Signal
    decode: k = floor(v * 3N); c = k // N; i = k % N; s = v * 3N - k

Final state: MC from which of zz_mass_{4e,4mu,2mu2e,2e2mu} is finite; Z+X (CR parquets) from
|z1_l1_pdgid_<cr>| and |l3_pdgid_<cr>| (cr = 2p2f / 3p1f). Events without a final state get v = -1 (dropped
like out-of-window events) and are counted.
Usage: python3 dump_events_2d_ch.py --dump events_2d_ch.npz <stitched_decorr CLI>
"""
import sys, runpy
import numpy as np
SCRIPTS = "/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts"
FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
N = len(FINE) - 1
sys.path.insert(0, SCRIPTS)
sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm")
import eos_io as E
import pyarrow as pa
_orig_read = E.read_parquet
STATS = {"files": 0, "events": 0, "nochan": 0, "chan": np.zeros(3)}
MC_CH = ("zz_mass_4e", "zz_mass_4mu", "zz_mass_2mu2e", "zz_mass_2e2mu")


def _col(tab, c):
    return np.asarray(tab.column(c).to_numpy(zero_copy_only=False), dtype=float)


def _mass(tab):
    if "zz_mass_inclusive" in tab.column_names:
        return _col(tab, "zz_mass_inclusive")
    if "m4l" in tab.column_names:
        vals = tab.column("m4l").to_pylist()
        return np.array([(v[0] if len(v) else np.nan) if isinstance(v, (list, tuple)) else
                         (np.nan if v is None else v) for v in vals], dtype=float)
    raise KeyError("no m4l column")


def _zx_suffix(names):
    for cr in ("2p2f", "3p1f"):
        if f"z1_l1_pdgid_{cr}" in names and f"l3_pdgid_{cr}" in names:
            return cr
    return None


def _channel(tab):
    n = tab.column_names
    if all(c in n for c in MC_CH):
        e4, m4 = np.isfinite(_col(tab, "zz_mass_4e")), np.isfinite(_col(tab, "zz_mass_4mu"))
        mx = np.isfinite(_col(tab, "zz_mass_2mu2e")) | np.isfinite(_col(tab, "zz_mass_2e2mu"))
        return np.where(e4, 0, np.where(m4, 1, np.where(mx, 2, -1)))
    cr = _zx_suffix(n)
    if cr is not None:
        a, b = np.abs(_col(tab, f"z1_l1_pdgid_{cr}")), np.abs(_col(tab, f"l3_pdgid_{cr}"))
        return np.where((a == 11) & (b == 11), 0, np.where((a == 13) & (b == 13), 1,
                        np.where(((a == 11) & (b == 13)) | ((a == 13) & (b == 11)), 2, -1)))
    raise KeyError(f"no final-state columns in {n[:10]}...")


def read_parquet(path, columns=None):
    req = None if columns is None else list(columns)
    if req is not None and "mva_score_Signal" in req:
        avail = set(E.parquet_columns(path))
        for c in ("zz_mass_inclusive", "m4l"):
            if c in avail and c not in req:
                req.append(c)
                break
        if all(c in avail for c in MC_CH):
            req += [c for c in MC_CH if c not in req]
        else:
            cr = _zx_suffix(avail)
            if cr is not None:
                req += [c for c in (f"z1_l1_pdgid_{cr}", f"l3_pdgid_{cr}") if c not in req]
    tab = _orig_read(path, req)
    if "mva_score_Signal" not in tab.column_names:
        return tab
    s = _col(tab, "mva_score_Signal")
    m = _mass(tab)
    ch = _channel(tab)
    i = np.searchsorted(FINE, m, side="right") - 1
    ok = (s >= 0) & (i >= 0) & (i < N) & np.isfinite(m) & (ch >= 0)
    STATS["nochan"] += int(np.sum((ch < 0) & (i >= 0) & (i < N) & (s >= 0)))
    for c in range(3):
        STATS["chan"][c] += int(np.sum(ok & (ch == c)))
    v = np.where(ok, (np.clip(ch, 0, 2) * N + np.clip(i, 0, N - 1) + np.clip(s, 1e-6, 1.0 - 1e-6)) / (3 * N), -1.0)
    STATS["files"] += 1; STATS["events"] += len(v)
    idx = tab.column_names.index("mva_score_Signal")
    tab = tab.set_column(idx, "mva_score_Signal", pa.array(v))
    if columns is not None:
        tab = tab.select([c for c in tab.column_names if c in columns])
    return tab


E.read_parquet = read_parquet
print(f"[dump2dch] fine m4l grid {FINE[0]}..{FINE[-1]} GeV, {N} bins x 3 final states; "
      f"mva_score_Signal -> (c N + i + s) / {3 * N}")
sys.argv[0] = "/eos/user/s/snandaku/Analysis/unbinned_mva_2026_10_18/dump_events.py"
runpy.run_path(sys.argv[0], run_name="__main__")
print(f"[dump2dch] patched {STATS['files']} parquet reads, {STATS['events']} events; in window per final state "
      f"4e/4mu/2e2mu {STATS['chan'].astype(int).tolist()}; no final state (dropped) {STATS['nochan']}")
