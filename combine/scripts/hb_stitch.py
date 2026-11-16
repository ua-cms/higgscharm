"""
H+b background (HPlusB) as the LHCHWG-2026-007 (arXiv:2608.26863, sec. 4.4) flavour-scheme stitch of the massless 5FS
FxFx and the massive 4FS samples (2026-11-16), the same recipe as the H+c signal (create_datacards_ctag2d_100150_stitched.py):
  - split on n_genb10 (GenJet hadronFlavour == 5, pT > 10 GeV, no eta cut; per event in the parquets): 0 from the 5FS FxFx
    samples (HPlusBottom_<era>, HPlusBottom_new_<era>, <era>HB), >= 1 from the 4FS samples (HPlusBottom4FS_<era>);
  - each piece normalised to sigma_NLO * K * f, f = pre-jet-veto sumw fraction of the piece's own sample in its region
    (sidecars sumw_preveto_genb{0,1,2p}), K = NNLO/NLO of the inclusive bbH in the same scheme (MiNNLOPS, C. Biello,
    arXiv:2510.18815; T. Bevilacqua, H+c workshop 06-2026): 5FS 574.8/753.7 = 0.763, 4FS 519.6/397.6 = 1.307;
  - the sum rescaled per era to the inclusive NNLO bbH cross section (MiNNLOPS 5FS, 574.8 fb), S = total / sum(K sigma f).
  => w = weight_nominal * [n_genb10 in region] * sigma_s K_s S BR_H4L lumi / sumw_preveto_s(era)
An era without a 4FS sample (2024) takes the 5FS sample over its whole phase space (f = 1, so S K sigma_5FS = total).

Mechanics: the HPlusB samples come from their own scored directory (--hb-scored-dir, the hplusc_mva_4class_ctag2d_final_hb
production with n_genb10) and are read by the ordinary background loaders (all background systematics, c-tag decorrelation,
JES/JER), with three patches active only while they run: dc.norm_group gives HplusB_5FS / HplusB_4FS, dc.group_xs_pb the piece
cross section above, and eos_io.read_parquet drops the events outside the piece's n_genb10 region. The HPlusB samples of the
main --scored-dir (old production, no n_genb10) are skipped.

Uncertainties: the samples' own LHE weights (4FS fixed gridpack: own weights; 5FS old gridpack: scale/PDF/alphaS transferred
from the official H+b -> gamma gamma 5FS FxFx, analysis/corrections/hb_syst_transfer.py), so HCZZ_HB_TRANSFER semantics are
switched on: scalevar_muR/muF_HPlusB not renormalised (they carry the NLO normalisation, as the paper's stitched-sample scale
band, which also covers the FS uncertainty) + lhe_pdf/lhe_alphaS shapes; no QCDscale lnN on HPlusB.
"""
import os
import re
import json
import contextlib
import numpy as np

import eos_io as E
import create_datacards_ctag2d_100150 as dc

PIECES = ("5FS", "4FS")
GROUP = {"5FS": "HplusB_5FS", "4FS": "HplusB_4FS"}
CFG = None          # stitched CFG (hb_* options)
BR = None           # H -> ZZ -> 4l (l = e, mu, tau), JHUGen decay of both samples
XS_PB = {}          # (era, group) -> sigma_s K_s S BR [pb]
SUMW = {}           # (era, group) -> pre-veto sumw of the piece's samples (all events)
SUMMARY = {}


def piece_of(sample_name):
    """'4FS' for HPlusBottom4FS_*, '5FS' for the old-gridpack 5FS FxFx samples, None otherwise."""
    base = re.sub(r"_\d+$", "", sample_name)
    if base.startswith("HPlusBottom4FS_"):
        return "4FS"
    if dc.is_old_hplusbottom(sample_name):
        return "5FS"
    return None


def k_factors():
    return {"5FS": CFG.hb_nnlo_5fs_fb / CFG.hb_sigma_5fs_fb, "4FS": CFG.hb_nnlo_4fs_fb / CFG.hb_sigma_4fs_fb}


def region_mask(piece, ngenb, only_5fs):
    if only_5fs:
        return np.full(len(ngenb), piece == "5FS")
    return ngenb < 1 if piece == "5FS" else ngenb >= 1


def load_split(eras):
    """(era, piece) -> {'all', 'region', 'n_files'} from the sidecars of --hb-sumw-dir, deduplicated per (guid, chunk range)
    keeping the latest copy (as load_split_sumw). Pre-veto keys; an era without 4FS sidecars is 5FS-only."""
    chunk_re = re.compile(r'_(\d+)-(\d+)\.json$')
    out = {}
    for era in eras:
        era_dir = os.path.join(CFG.hb_sumw_dir, era)
        latest = {p: {} for p in PIECES}
        for part in E.listdir(era_dir):
            p = piece_of(part)
            if p is None:
                continue
            d = os.path.join(era_dir, part, "sumw")
            fs = E.files(d, ".json")
            if not fs and E.isdir(d):
                raise RuntimeError(f"sumw dir is empty on the server: {d}")
            for jf in fs:
                b = os.path.basename(jf)
                m = chunk_re.search(b)
                key = (b.split("_%2F")[0], (m.group(1), m.group(2)) if m else None)
                mt = E.mtime(jf)
                if key not in latest[p] or mt > latest[p][key][1]:
                    latest[p][key] = (jf, mt)
        if not latest["5FS"]:
            raise RuntimeError(f"no 5FS H+b sumw sidecars in {era_dir}")
        only_5fs = not latest["4FS"]
        for p in PIECES:
            tot = reg = 0.0
            for jf, _ in latest[p].values():
                rec = E.read_json(jf)
                tot += rec["sumw_preveto"]
                if only_5fs:
                    continue
                if "sumw_preveto_genb0" not in rec:
                    raise RuntimeError(f"H+b sidecar without sumw_preveto_genb* (needs the n_genb10 production): {jf}")
                g = {n: rec[f"sumw_preveto_genb{n}"] for n in ("0", "1", "2p")}
                if abs(sum(g.values()) - rec["sumw_preveto"]) > 1e-6 * abs(rec["sumw_preveto"]):
                    raise RuntimeError(f"pre-veto genb split does not add up to sumw_preveto: {jf}")
                reg += g["0"] if p == "5FS" else g["1"] + g["2p"]
            if only_5fs:
                reg = tot if p == "5FS" else 0.0
            out[(era, p)] = {"all": tot if latest[p] else 1.0, "region": reg, "n_files": len(latest[p]), "only_5fs": only_5fs}
    return out


def compute(eras, br):
    """Fill XS_PB / SUMW / SUMMARY for these eras."""
    global BR
    BR = br
    split = load_split(eras)
    K = k_factors()
    sigma_pb = {"5FS": CFG.hb_sigma_5fs_fb * 1e-3, "4FS": CFG.hb_sigma_4fs_fb * 1e-3}
    print(f"\n[H+b stitch] 5FS n_genb10 == 0 (sigma_NLO {CFG.hb_sigma_5fs_fb} fb, K {K['5FS']:.4f}) + 4FS n_genb10 >= 1 "
          f"(sigma_NLO {CFG.hb_sigma_4fs_fb} fb, K {K['4FS']:.4f}), total {CFG.hb_sigma_total_fb} fb, BR {br:.4e}\n"
          f"    scored: {CFG.hb_scored_dir}\n    sumw:   {CFG.hb_sumw_dir}")
    print(f"  {'era':<14} {'piece':<5} {'n_sidecars':>10} {'sumw_all':>12} {'f_region':>9} {'S':>7} {'xs_piece_fb':>11}")
    for era in eras:
        f = {p: split[(era, p)]["region"] / split[(era, p)]["all"] for p in PIECES}
        native = sum(K[p] * sigma_pb[p] * f[p] for p in PIECES)
        S = CFG.hb_sigma_total_fb * 1e-3 / native
        for p in PIECES:
            XS_PB[(era, GROUP[p])] = sigma_pb[p] * K[p] * S * br
            SUMW[(era, GROUP[p])] = split[(era, p)]["all"]
        SUMMARY[era] = {"sumw": {p: split[(era, p)] for p in PIECES}, "fraction_in_region": f, "K": K,
                        "native_total_fb": native * 1e3, "global_rescale_S": S,
                        "piece_xs_fb_after_S": {p: K[p] * sigma_pb[p] * f[p] * S * 1e3 for p in PIECES},
                        "only_5fs": split[(era, "5FS")]["only_5fs"]}
        for p in PIECES:
            print(f"  {era:<14} {p:<5} {split[(era, p)]['n_files']:>10} {split[(era, p)]['all']:>12.1f} {f[p]:>9.4f} "
                  f"{S:>7.4f} {SUMMARY[era]['piece_xs_fb_after_S'][p]:>11.3f}"
                  + ("   (no 4FS sample: 5FS over the whole phase space)" if SUMMARY[era]["only_5fs"] and p == "5FS" else ""))


def _sample_of(path):
    m = re.search(r"/(" + "|".join(map(re.escape, dc.ERAS)) + r")/([^/]+)/", path)
    return (m.group(1), m.group(2)) if m else (None, None)


@contextlib.contextmanager
def active():
    """While loading the HPlusB samples of --hb-scored-dir: only HPlusB is a process, piece groups / cross sections, and the
    n_genb10 region selection on every parquet read."""
    import create_datacards_ctag2d_100150_rawsf as rs
    orig_proc, orig_rs_proc = dc.get_process_from_path, rs.get_process_from_path
    orig_group, orig_xs, orig_read = dc.norm_group, dc.group_xs_pb, E.read_parquet
    stats = {"files": 0, "kept": 0, "read": 0}

    def proc_only_hb(p):
        q = orig_proc(p)
        return q if q == "HPlusB" and piece_of(os.path.basename(p.rstrip("/"))) else None

    def norm_group(sample_name):
        p = piece_of(sample_name)
        return GROUP[p] if (p and orig_proc(sample_name) == "HPlusB") else orig_group(sample_name)

    def group_xs_pb(era, group):
        if group in GROUP.values():
            if (era, group) not in XS_PB:
                raise KeyError(f"H+b stitch: no cross section for ({era}, {group})")
            return XS_PB[(era, group)]
        return orig_xs(era, group)

    def read_parquet(path, columns=None):
        era, sample = _sample_of(path)
        p = piece_of(sample) if sample else None
        if p is None or era not in SUMMARY:
            return orig_read(path, columns)
        req = None if columns is None else list(columns) + (["n_genb10"] if "n_genb10" not in columns else [])
        tab = orig_read(path, req)
        only_5fs = SUMMARY[era]["only_5fs"]
        if "n_genb10" not in tab.column_names:
            if not only_5fs:
                raise RuntimeError(f"H+b stitch: no n_genb10 column in {path}")
            mask = np.full(tab.num_rows, p == "5FS")
        else:
            ngenb = np.asarray(tab.column("n_genb10").to_numpy(zero_copy_only=False))
            mask = region_mask(p, ngenb, only_5fs)
        stats["files"] += 1
        stats["read"] += tab.num_rows
        stats["kept"] += int(mask.sum())
        import pyarrow as pa
        tab = tab.filter(pa.array(mask))
        if columns is not None:
            tab = tab.select([c for c in tab.column_names if c in columns])
        return tab

    dc.get_process_from_path = rs.get_process_from_path = proc_only_hb
    dc.norm_group, dc.group_xs_pb, E.read_parquet = norm_group, group_xs_pb, read_parquet
    try:
        yield
    finally:
        dc.get_process_from_path, rs.get_process_from_path = orig_proc, orig_rs_proc
        dc.norm_group, dc.group_xs_pb, E.read_parquet = orig_group, orig_xs, orig_read
        print(f"  [H+b stitch] {stats['files']} parquet reads, {stats['kept']}/{stats['read']} events in their piece's region")


class ShiftSumw(dict):
    """sumw lookup of the JES/JER loaders keyed (shift, era, group): the H+b pieces have one sumw for all shift dirs (the
    shifts are written by the same job as the nominal), divided by `div[era]` (the background table convention)."""
    def __init__(self, div):
        super().__init__()
        self.div = div

    def get(self, key, default=None):
        _, era, g = key
        return SUMW[(era, g)] / self.div.get(era, 1.0) if (era, g) in SUMW else default


def check_scale_weights(mc):
    """Print the HPlusB scale / PDF variations (in-window integrals); the transferred 5FS and the 4FS weights must be sane."""
    if "HPlusB" not in mc:
        return
    _, w, syst = mc["HPlusB"]
    tot = w.sum()
    if tot <= 0:
        raise RuntimeError("H+b stitch: no HPlusB events in the window")
    line = []
    for s, by in sorted(syst.items()):
        if s.startswith(("scalevar", "lhe_")):
            r = {d: by[d].sum() / tot for d in ("Up", "Down") if d in by}
            line.append(f"{s} {r.get('Up', 1):.3f}/{r.get('Down', 1):.3f}")
            if any(not 0.5 < v < 1.5 for v in r.values()):
                raise RuntimeError(f"H+b stitch: {s} variation out of range {r} (broken LHE weights?)")
    print(f"  HPlusB in-window {tot:.5f}; " + ", ".join(line))


def write_summary(out_dir):
    if out_dir and SUMMARY:
        os.makedirs(out_dir, exist_ok=True)
        cfg = {k: v for k, v in vars(CFG).items() if k.startswith("hb_")}
        with open(os.path.join(out_dir, "hb_stitch_summary.json"), "w") as fh:
            json.dump({"_config": cfg, "BR": BR, **SUMMARY}, fh, indent=1)
