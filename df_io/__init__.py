import bz2
import os
import gzip
import io
import shutil
import tempfile
import numpy as np
import pandas as pd
import s3fs
import zstandard
from parallel_write import Writer


def _writer_wrapper(writer, f, writer_args, writer_options):
    try:
        writer(f, *writer_args, **writer_options)
    except TypeError:
        # hack for https://github.com/pandas-dev/pandas/issues/19827
        # provide compatibility with older Pandas
        f = io.TextIOWrapper(f)
        writer(f, *writer_args, **writer_options)
    return f


def read_df(path, fmt="csv", reader_args=[], reader_options={}, s3fs_kwargs={}):
    reader_defaults = {"csv": {"encoding": "UTF_8"},
                       "json": {"orient": "records", "lines": True}}
    if not reader_options:
        reader_options = reader_defaults.get(fmt, {})
    pd_reader = getattr(pd, "read_{}".format(fmt))
    s3fs_kwargs = {**{"anon": False}, **s3fs_kwargs}
    # pandas can't (yet) read zstandard, implement it here
    if path.endswith(".zstd") or path.endswith(".zst"):
        if path.startswith("s3://"):
            s3 = s3fs.S3FileSystem(**s3fs_kwargs)
            _r = s3.open(path, "rb")
        else:
            _r = open(path, "rb")
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(_r) as compressor:
            # these readers try to seek, which is not supported
            # by the decompressor, so open a temporary file, uncompress data
            # to it and use that for reading with pandas
            if fmt in ["parquet", "feather"]:
                with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
                    shutil.copyfileobj(compressor, tmpfile)
                    tmpfile.flush()
                    tmpfile.seek(0)
                    return pd_reader(tmpfile, *reader_args, **reader_options)
            else:
                return pd_reader(compressor, *reader_args, **reader_options)
    # pandas could read from S3 and even open some compressed formats, but
    # for testing (localstack) and consistency, we handle all cases the same:
    # if the URL is an S3 one, wrap it with s3fs and handle decompression here
    # as well, instead of letting pandas do it
    elif path.endswith(".gz"):
        if path.startswith("s3://"):
            s3 = s3fs.S3FileSystem(**s3fs_kwargs)
            _r = s3.open(path, "rb")
        else:
            _r = open(path, "rb")
        with gzip.GzipFile(fileobj=_r) as gz:
            return pd_reader(gz, *reader_args, **reader_options)
    elif path.endswith(".bz2"):
        if path.startswith("s3://"):
            s3 = s3fs.S3FileSystem(**s3fs_kwargs)
            _r = s3.open(path, "rb")
        else:
            _r = open(path, "rb")
        with bz2.open(_r) as bz:
            return pd_reader(bz, *reader_args, **reader_options)
    else:
        if path.startswith("s3://"):
            s3 = s3fs.S3FileSystem(**s3fs_kwargs)
            _r = s3.open(path, "rb")
        else:
            _r = open(path, "rb")
        return pd_reader(_r, *reader_args, **reader_options)


def write_df(df, path, copy_paths=[], fmt="csv", compress_level=6,
             chunksize=None, writer_args=[], writer_options={},
             zstd_options={"threads": -1}, s3fs_kwargs={}):
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

    def flush_and_close(f):
        """
        Flush and close for the compressed case (non-compressed will receive
        a double flush).
        """
        try:
            f.flush()
            f.close()
        except ValueError:
            pass

    if compress_level is not None:
        zstd_options["level"] = compress_level

    writer_defaults = {"csv": {"index": False, "encoding": "UTF_8"},
                       "json": {"orient": "records", "lines": True, "force_ascii": False}}
    if not writer_options and fmt in writer_defaults:
        writer_options = writer_defaults[fmt]
    s3fs_kwargs = {**{"anon": False}, **s3fs_kwargs}

    filename = os.path.basename(path)
    _files = []
    # support S3 and local writes as well
    for _path in copy_paths + [path]:
        if _path.startswith("s3://"):
            s3 = s3fs.S3FileSystem(**s3fs_kwargs)
            _files.append(_w := s3.open(_path, "wb"))
        else:
            _files.append(_w := open(_path, "wb"))

    _w = Writer(_files)
    with _w as f:
        if filename.endswith(".gz"):
            f = gzip.GzipFile(filename, mode="wb", compresslevel=compress_level, fileobj=f)
        if filename.endswith(".bz2"):
            f = bz2.open(f, mode="wb", compresslevel=compress_level)
        if filename.endswith(".zstd") or filename.endswith(".zst"):
            cctx = zstandard.ZstdCompressor(**zstd_options)
            f = cctx.stream_writer(f, write_size=32 * 1024, closefd=False)
        writer = getattr(df, "to_{}".format(fmt))
        if fmt in []:
            # add any future pandas writers here, which doesn't implement
            # writing to a (compressed) stream, for eg. because it seeks
            with tempfile.NamedTemporaryFile() as tmpfile:
                writer(tmpfile.name, *writer_args, **writer_options)
                tmpfile.seek(0)
                shutil.copyfileobj(tmpfile, f)
        elif fmt == "csv":
            # CSV natively supports chunked writes
            _writer_wrapper(writer, f, writer_args, dict(writer_options, chunksize=chunksize))
        elif chunksize and fmt == "json" and writer_options.get("orient") == "records" and writer_options.get("lines"):
            # calculate the number of desired parts
            split_parts = int(max(1, len(df) / chunksize))
            # split the DF into parts
            for _df in np.array_split(df, split_parts):
                writer = getattr(_df, "to_{}".format(fmt))
                f = _writer_wrapper(writer, f, writer_args, writer_options)
                # we have to write a newline after every rounds, so won't get
                # the new round started in the same line
                try:
                    # Try to adapt to the required mode by catching TypeError
                    # Seems to be more reliable than trying to figure out the
                    # binary/text type.
                    f.write(b"\n")
                except TypeError:
                    f.write("\n")
        else:
            # in all other cases we're just calling the writer
            _writer_wrapper(writer, f, writer_args, writer_options)
        flush_and_close(f)


__version__ = "0.0.9"
