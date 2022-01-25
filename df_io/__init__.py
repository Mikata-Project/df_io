"""Helpers for reading/writing Pandas DataFrames."""
import bz2
import os
import gzip
import io
import shutil
import tempfile
import numpy as np
import pandas as pd
import zstandard
from parallel_write import Writer
from smart_open import open


def _writer_wrapper(writer, fhs, writer_args, writer_options):
    """Wrap the last file object in a TextIOWrapper if needed."""
    try:
        writer(fhs[-1], *writer_args, **writer_options)
    except TypeError:
        # hack for https://github.com/pandas-dev/pandas/issues/19827
        # provide compatibility with older Pandas
        fhs.append(io.TextIOWrapper(fhs[-1]))
        writer(fhs[-1], *writer_args, **writer_options)


def read_df(path, fmt="csv", reader_args=[], reader_options={}, open_kw={}):
    """Read DataFrame."""
    reader_defaults = {"csv": {"encoding": "UTF_8"},
                       "json": {"orient": "records", "lines": True}}
    if not reader_options:
        reader_options = reader_defaults.get(fmt, {})
    pd_reader = getattr(pd, "read_{}".format(fmt))

    # pandas could read from S3 and even open some compressed formats, but
    # for testing (localstack) and consistency, we handle all cases the same:
    # we open the path with smart_open and stack a decompressor onto it
    with open(path, "rb", compression="disable", **open_kw) as _r:
        if path.endswith(".zstd") or path.endswith(".zst"):
            with zstandard.open(_r) as zs:
                # these readers try to seek, which is not supported
                # by the decompressor, so open a temporary file, uncompress data
                # to it and use that for reading with pandas
                if fmt in ["parquet", "feather"]:
                    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                        shutil.copyfileobj(zs, tmpfile)
                        tmpfile.flush()
                        tmpfile.seek(0)
                        return pd_reader(tmpfile, *reader_args, **reader_options)
                else:
                    return pd_reader(zs, *reader_args, **reader_options)
        elif path.endswith(".gz"):
            with gzip.GzipFile(fileobj=_r) as gz:
                return pd_reader(gz, *reader_args, **reader_options)
        elif path.endswith(".bz2"):
            with bz2.open(_r) as bz:
                return pd_reader(bz, *reader_args, **reader_options)
        else:
            return pd_reader(_r, *reader_args, **reader_options)


def write_df(df, path, copy_paths=[], fmt="csv", compress_level=6,
             chunksize=None, writer_args=[], writer_options={},
             zstd_options={"threads": -1}, open_kw={}):
    """
    Pandas DataFrame write helper

    Can write to local files and to S3 paths in any format, supported by the
    installed pandas version. Writer-specific arguments can be given in
    writer_args and writer_options.
    If the path parameter starts with s3://, it will try to do an S3 write,
    otherwise opens a local file with that path.

    Additional output files can be specified in `copy_paths` parameter, as
    a list of either local, or `s3://...` paths. The same output will be written
    there as to `path` in parallel to reduce overhead.
    """
    if compress_level is not None:
        zstd_options["level"] = compress_level

    writer_defaults = {"csv": {"index": False, "encoding": "UTF_8"},
                       "json": {"orient": "records", "lines": True,
                                "force_ascii": False}}
    if not writer_options and fmt in writer_defaults:
        writer_options = writer_defaults[fmt]

    filename = os.path.basename(path)
    _files = []
    # support S3 and local writes as well
    for _path in copy_paths + [path]:
        _files.append(open(_path, "wb", compression="disable", **open_kw))

    # depending on the compression status and the mode of the file object,
    # we may stack up to three file objects on top of each other. To track this,
    # we append them in order to fhs, which we'll use to flush/close in the
    # opposite order.
    fhs = [Writer(_files)]
    
    # if compression is enabled, we open the compression stream on the
    # top of the parallel_write object stored in fhs array's first element
    # and appending the new object to its tail.
    if filename.endswith(".gz"):
        fhs.append(gzip.GzipFile(filename, mode="wb",
                                 compresslevel=compress_level, fileobj=fhs[0]))
    if filename.endswith(".bz2"):
        fhs.append(bz2.open(fhs[0], mode="wb", compresslevel=compress_level))
    if filename.endswith(".zstd") or filename.endswith(".zst"):
        fhs.append(zstandard.open(fhs[0], mode="wb", closefd=False))
    writer = getattr(df, "to_{}".format(fmt))
    
    # for writing, we always use the last element in the stack, fhs[-1]
    if fmt in []:
        # add any future pandas writers here, which doesn't implement
        # writing to a (compressed) stream, for eg. because it seeks
        with tempfile.NamedTemporaryFile() as tmpfile:
            writer(tmpfile.name, *writer_args, **writer_options)
            tmpfile.seek(0)
            shutil.copyfileobj(tmpfile, fhs[-1])
    elif fmt == "csv":
        # CSV natively supports chunked writes
        _writer_wrapper(writer, fhs, writer_args, dict(writer_options, chunksize=chunksize))
    elif chunksize and fmt == "json" and writer_options.get("orient") == "records" and writer_options.get("lines"):
        # calculate the number of desired parts
        split_parts = int(max(1, len(df) / chunksize))
        # split the DF into parts
        for _df in np.array_split(df, split_parts):
            writer = getattr(_df, "to_{}".format(fmt))
            _writer_wrapper(writer, fhs, writer_args, writer_options)
            # we have to write a newline after every rounds, so won't get
            # the new round started in the same line
            try:
                # Try to adapt to the required mode by catching TypeError
                # Seems to be more reliable than trying to figure out the
                # binary/text type.
                fhs[-1].write(b"\n")
            except TypeError:
                fhs[-1].write("\n")
    else:
        # in all other cases we're just calling the writer
        _writer_wrapper(writer, fhs, writer_args, writer_options)
    # flush/close all file objects in reverse order
    for f in reversed(fhs):
        f.close()


__version__ = "0.0.11"
