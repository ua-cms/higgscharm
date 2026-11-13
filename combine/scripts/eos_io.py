"""
EOS access that does not trust the FUSE mount's directory listings (2026-09-25).

On lxplus929 and lxplus971 the eosxd FUSE mount served stale, EMPTY listings
for directories whose files `eos ls` / xrootd showed fine, and could not open
those files by name either. glob/os.listdir raise nothing, so the datacard
builders silently read a subset (the 09-24 decorr card lost 21% of
Other_Higgs CMS_res_m Up; the 09-25 decorr build lost whole sumw dirs).

Listings here come from the xrootd server (authoritative); reads try FUSE
first and fall back to xrootd. Every function raises on failure.
"""
import io
import os
import json

XRD_URL = "root://eosuser.cern.ch"
_fs = None
N_FALLBACK = {"listdir": 0, "read": 0}


def _filesystem():
    global _fs
    if _fs is None:
        from XRootD import client
        _fs = client.FileSystem(XRD_URL)
    return _fs


def listdir(path):
    """Entry names of an EOS directory from the server (no dot entries such as
    the .sys.v#.* version dirs). Returns [] if the directory does not exist."""
    from XRootD.client.flags import DirListFlags
    st, lst = _filesystem().dirlist(path, DirListFlags.STAT)
    if not st.ok:
        if st.errno == 3011 or "No such file" in st.message or "not found" in st.message.lower():
            return []
        raise OSError(f"xrootd dirlist failed for {path}: {st.message}")
    names = sorted(e.name for e in lst if not e.name.startswith("."))
    try:
        fuse = set(os.listdir(path))
    except OSError:
        fuse = set()
    if fuse != set(names):
        N_FALLBACK["listdir"] += 1
        if N_FALLBACK["listdir"] <= 20:
            print(f"  NOTE eos_io: FUSE listing of {path} differs from server "
                  f"({len(fuse)} vs {len(names)} entries); using server listing", flush=True)
    return names


def isdir(path):
    st, info = _filesystem().stat(path)
    if not st.ok:
        return False
    from XRootD.client.flags import StatInfoFlags
    return bool(info.flags & StatInfoFlags.IS_DIR)


def files(dirpath, suffix):
    """Full paths of the files in dirpath ending with suffix (server listing)."""
    return [os.path.join(dirpath, n) for n in listdir(dirpath) if n.endswith(suffix)]


def mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        st, info = _filesystem().stat(path)
        if not st.ok:
            raise OSError(f"xrootd stat failed for {path}: {st.message}")
        return float(info.modtime)


def read_bytes(path):
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except OSError:
        from XRootD import client
        N_FALLBACK["read"] += 1
        with client.File() as f:
            st, _ = f.open(f"{XRD_URL}/{path}")
            if not st.ok:
                raise OSError(f"xrootd open failed for {path}: {st.message}")
            st, data = f.read()
            if not st.ok:
                raise OSError(f"xrootd read failed for {path}: {st.message}")
        return data


def read_json(path):
    return json.loads(read_bytes(path))


def _parquet_source(path):
    if os.path.exists(path):
        return path
    return io.BytesIO(read_bytes(path))


_KAPPA_CLASSES = ("mva_score_qqZZ", "mva_score_ggZZ", "mva_score_Other_Higgs")


def _kappa():
    """2026-10-30: HCZZ_KAPPA="k_qqZZ,k_ggZZ,k_OH" -> mva_score_Signal is replaced on read by the kappa discriminant
    D = p_S / (p_S + k_qq p_qq + k_gg p_gg + k_OH p_OH) (kappa*.ipynb). Unset: unchanged."""
    v = os.environ.get("HCZZ_KAPPA")
    return tuple(float(x) for x in v.split(",")) if v else None


def read_parquet(path, columns=None):
    """pyarrow Table; columns missing from the file are silently dropped from
    the request (same as the builders' previous 'available columns' fallback)."""
    import pyarrow.parquet as pq
    src = _parquet_source(path)
    pf = pq.ParquetFile(src)
    avail = set(pf.schema_arrow.names)
    req = columns
    if columns is not None:
        columns = [c for c in columns if c in avail]
    pdf_fix = os.environ.get("HCZZ_PDF_HESSIAN_FIX") == "1" and any(c in avail for c in _PDF_COLS)
    if pdf_fix and columns is not None and any(c in columns for c in _PDF_COLS) and "weight_nominal" not in columns:
        return _drop(read_parquet(path, columns + ["weight_nominal"]), ["weight_nominal"])
    k = _kappa()
    if k is None or (columns is not None and "mva_score_Signal" not in columns) or "mva_score_Signal" not in avail:
        return _pdf_hessian(pf.read(columns=columns)) if pdf_fix else pf.read(columns=columns)
    if not all(c in avail for c in _KAPPA_CLASSES):
        raise KeyError(f"HCZZ_KAPPA set but {path} lacks one of {_KAPPA_CLASSES}")
    extra = [c for c in _KAPPA_CLASSES if columns is not None and c not in columns]
    tab = pf.read(columns=None if columns is None else columns + extra)
    import numpy as np
    import pyarrow as pa
    col = lambda c: np.asarray(tab.column(c).to_numpy(zero_copy_only=False), dtype=float)
    ps = col("mva_score_Signal")
    den = ps + sum(kk * col(c) for kk, c in zip(k, _KAPPA_CLASSES))
    d = np.where(ps >= 0, ps / np.where(den > 0, den, 1.0), ps)   # keep the builders' invalid-score (< 0) convention
    tab = tab.set_column(tab.column_names.index("mva_score_Signal"), "mva_score_Signal", pa.array(d))
    for c in extra:
        tab = tab.drop([c])
    return _pdf_hessian(tab) if pdf_fix else tab


# 2026-11-04: lhe_pdf Hessian fix. analysis/corrections/lhepdf.py (until 11-04) computed
# delta_pdf = sqrt(sum_k (w_k - w_0)^2 / 99), the MC-replica RMS, but every sample's LHEPdfWeight is an MC-Hessian
# set (LHA 325300-325402 official, 325500-325600 private H+c) whose uncertainty is the plain quadrature sum
# (PDF4LHC15, arXiv:1510.03865 eq. 20), i.e. the stored weight_lhe_pdf{Up,Down} = nom * (1 +- delta/sqrt(99)) are
# sqrt(99) too small (ZZto4L [105,160) rate +-0.41% stored vs +-4.07% Hessian; HIG-24-013 +3.1/-3.4%).
# With HCZZ_PDF_HESSIAN_FIX=1 (productions made before the source fix) Up/Down -> nom +- sqrt(99) |Up/Down - nom|,
# exact because delta >= 0. The combined weight_lhe_pdf_alphaS* columns are not used by the builders, left as is.
_PDF_COLS = ("weight_lhe_pdfUp", "weight_lhe_pdfDown")


def _drop(tab, cols):
    return tab.drop([c for c in cols if c in tab.column_names])


def _pdf_hessian(tab):
    import numpy as np
    import pyarrow as pa
    names = tab.column_names
    if "weight_nominal" not in names:
        return tab
    col = lambda c: np.asarray(tab.column(c).combine_chunks().to_numpy(zero_copy_only=False), dtype=float)
    nom = col("weight_nominal")
    for c, sign in (("weight_lhe_pdfUp", 1.0), ("weight_lhe_pdfDown", -1.0)):
        if c in names:
            w = col(c)
            tab = tab.set_column(names.index(c), c, pa.array(nom + sign * np.sqrt(99.0) * np.abs(w - nom)))
    return tab


def parquet_columns(path):
    import pyarrow.parquet as pq
    return pq.ParquetFile(_parquet_source(path)).schema_arrow.names
