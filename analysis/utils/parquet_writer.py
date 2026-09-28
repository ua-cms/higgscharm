import os
import io
import shutil
import pathlib
import numpy as np
import awkward as ak
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional


def _fix_null(typ):
    # Replace any bare Arrow `null` type with a concrete, nullable type.
    # Covers both flat null columns and the `list<item: null>` case that
    # arises when a jagged variable is all-empty-lists (no inner values for
    # awkward to infer a type from). Parquet refuses to write a null-typed
    # field unless it is nullable, hence "NullType Arrow field must be
    # nullable".
    if pa.types.is_null(typ):
        return pa.float64()
    if pa.types.is_list(typ):
        return pa.list_(pa.field("item", _fix_null(typ.value_type), nullable=True))
    if pa.types.is_large_list(typ):
        return pa.large_list(
            pa.field("item", _fix_null(typ.value_type), nullable=True)
        )
    return typ


def dump_pa_table(
    arrays: dict, fname: str, location: str, subdirs: Optional[List[str]] = None
):
    subdirs = subdirs or []
    xrd_prefix = "root://"
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD
            import XRootD.client

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err

    # /tmp is local to each worker node, always writable, and avoids AFS
    # quota issues and parallel job filename conflicts when multiple jobs
    # share the same working directory.
    local_file = os.path.join("/tmp", fname)

    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )

    out = {}
    for variable, array in arrays.items():
        try:
            if isinstance(array, np.ndarray):
                array = ak.Array(array)

            if array.ndim == 2:
                # Jagged array. Deliberately NOT using ak.to_arrow() here,
                # even after ak.packed(): it does a zero-copy conversion
                # that reuses awkward's internal offset/index buffers
                # directly. For certain IndexedOptionArray-wrapping-a-
                # ListOffsetArray shapes, those buffers can carry a
                # non-zero span behind a "null" flag even after packing --
                # pyarrow's array builder accepts this, but the stricter
                # Parquet writer rejects it with "Lists with non-zero
                # length null components are not supported".
                #
                # Instead, bypass ak.to_arrow() for jagged columns and let
                # pyarrow build the array fresh from a plain nested Python
                # list via pa.array(). Nothing is shared with awkward's
                # internal buffers, so the result always has clean,
                # canonical offsets.
                filled = ak.fill_none(array, [])
                converted = pa.array(ak.to_list(filled))
            else:
                # 1D -- fill any None with a scalar sentinel to avoid union
                # types, then repack for clean offsets/validity before the
                # (safe, since there's no nested list structure here)
                # zero-copy conversion.
                filled = ak.fill_none(array, -999.0)
                filled = ak.packed(filled)
                converted = ak.to_arrow(filled)

            out[variable] = converted

        except Exception as e:
            print(f"Warning: skipping variable '{variable}' due to conversion error: {e}")
            print(f"  type: {type(array)}, ndim: {getattr(array, 'ndim', 'unknown')}")
            continue

    if not out:
        print("Warning: no variables could be converted, skipping parquet write.")
        return

    table = pa.table(out)
    if len(table) == 0:
        return

    # debug: catch any remaining union type columns
    for col_name, col_type in zip(table.schema.names, table.schema.types):
        if pa.types.is_union(col_type):
            print(f"Warning: union type column '{col_name}' -> {col_type}")

    # debug: try writing each column individually first, so if the combined
    # write below still fails with a low-level Arrow error (e.g. the
    # Parquet-writer-only "null list components" error), we already know
    # which specific variable is responsible instead of only learning that
    # *some* column in the table is bad.
    for col_name in table.schema.names:
        try:
            pq.write_table(table.select([col_name]), io.BytesIO())
        except Exception as col_err:
            print(f"Warning: column '{col_name}' fails an isolated parquet write: {col_err}")

    # Sanitize the schema before writing: coerce null / list<null> fields to
    # concrete nullable types and force every field nullable. Casting an
    # all-empty list<null> to list<float64> keeps the (empty) lists intact,
    # so no data is lost.
    new_schema = pa.schema(
        [pa.field(f.name, _fix_null(f.type), nullable=True) for f in table.schema]
    )
    table = table.cast(new_schema)

    pq.write_table(table, local_file)
    try:
        if xrootd:
            copyproc = XRootD.client.CopyProcess()
            copyproc.add_job(local_file, destination, force=True)
            copyproc.prepare()
            status, response = copyproc.run()
            if status.status != 0:
                raise Exception(status.message)
            del copyproc
        else:
            dirname = os.path.dirname(destination)
            pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
            shutil.copy(local_file, destination)
            assert os.path.isfile(destination)
    finally:
        # Always clean up the temporary file, even if the copy failed.
        pathlib.Path(local_file).unlink(missing_ok=True)


# Function taken from HiggsDNA
# https://gitlab.cern.ch/HiggsDNA-project/HiggsDNA/-/blob/master/higgs_dna/utils/dumping_utils.py
def dump_ak_array(
    akarr: ak.Array,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
) -> None:
    """
    Dump an ak array to disk at location/'/'.join(subdirs)/fname.
    """
    subdirs = subdirs or []
    xrd_prefix = "root://"
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD  # type: ignore
            import XRootD.client  # type: ignore

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err

    local_file = os.path.join("/tmp", fname)

    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )
    ak.to_parquet(ak.fill_none(akarr, [], axis=0), local_file)
    try:
        if xrootd:
            copyproc = XRootD.client.CopyProcess()
            copyproc.add_job(local_file, destination, force=True)
            copyproc.prepare()
            status, response = copyproc.run()
            if status.status != 0:
                raise Exception(status.message)
            del copyproc
        else:
            dirname = os.path.dirname(destination)
            pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
            shutil.copy(local_file, destination)
            assert os.path.isfile(destination)
    finally:
        pathlib.Path(local_file).unlink(missing_ok=True)