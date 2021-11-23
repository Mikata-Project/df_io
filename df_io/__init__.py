import os
import gzip
import io
import shutil
import tempfile
import numpy as np
import pandas as pd
import s3fs
import zstandard
from concurrent.futures import ThreadPoolExecutor, as_completed


def _writer_wrapper(writer, f, writer_args, writer_options):
    try:
        writer(f, *writer_args, **writer_options)
    except TypeError:
        # hack for https://github.com/pandas-dev/pandas/issues/19827
        # provide compatibility with older Pandas
        f = io.TextIOWrapper(f)
        writer(f, *writer_args, **writer_options)
    return f


class FileWriter:
    def __init__(self, files, max_workers=None):
        if max_workers is None:
            max_workers = len(files)
        self._files = files
        self._executor = ThreadPoolExecutor(max_workers)

    def write(self, data):
        futures = {self._executor.submit(f.write, data): f for f in self._files}
        for future in as_completed(futures):
            res = future.result()
        return res

    def close(self):
        map(lambda x: x.close(), self._files)

    def flush(self, *args, **kwargs):
        map(lambda x: x.flush(), self._files)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


def read_df(path, fmt="csv", reader_args=[], reader_options={}):
    pd_reader = getattr(pd, 'read_{}'.format(fmt))
    if path.endswith(".zstd"):
        if path.startswith('s3://'):
            s3 = s3fs.S3FileSystem(anon=False)
            _r = s3.open(path, 'rb')
        else:
            _r = open(path, 'rb')
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(_r) as compressor:
            return pd_reader(compressor, *reader_args, **reader_options)
    else:
        return pd_reader(path, *reader_args, **reader_options)


def write_df(df, s3_path, copy_paths=[], fmt="csv", gzip_level=9,
             chunksize=None, writer_args=[], writer_options={},
             zstd_options={"level": 5, "threads": -1}):
    """
    Pandas DataFrame write helper

    Can write to local files and to S3 paths in any format, supported by the
    installed pandas version. Writer-specific arguments can be given in
    writer_args and writer_options.
    If the s3_path parameter starts with s3://, it will try to do an S3 write,
    otherwise opens a local file with that path.

    Additional output files can be specified in `copy_paths` parameter, as
    a list of either local, or `s3://...` paths. The same output will be written
    there as to `s3_path` in parallel to reduce overhead.
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

    writer_defaults = {'csv': {'index': False, 'encoding': 'UTF_8'},
                       'json': {'orient': 'records', 'lines': True}
                       }
    if not writer_options and fmt in writer_defaults:
        writer_options = writer_defaults[fmt]

    filename = os.path.basename(s3_path)
    _files = []
    # support S3 and local writes as well
    for _path in [s3_path] + copy_paths:
        if _path.startswith('s3://'):
            s3 = s3fs.S3FileSystem(anon=False)
            _files.append(s3.open(_path, 'wb'))
        else:
            _files.append(open(_path, 'wb'))
    with FileWriter(_files) as f:
        if filename.endswith('.gz'):
            f = gzip.GzipFile(filename, mode='wb', compresslevel=gzip_level,
                              fileobj=f)
        if filename.endswith('.zstd'):
            cctx = zstandard.ZstdCompressor(**zstd_options)
            f = cctx.stream_writer(f, write_size=32 * 1024, closefd=False)
        writer = getattr(df, 'to_{}'.format(fmt))
        if fmt in ['pickle', 'parquet']:
            # These support writing only to path as of pandas 0.24.
            # Will be easier when this gets done:
            # https://github.com/pandas-dev/pandas/issues/15008
            with tempfile.NamedTemporaryFile() as tmpfile:
                writer(tmpfile.name, *writer_args, **writer_options)
                tmpfile.seek(0)
                shutil.copyfileobj(tmpfile, f)
        elif fmt == 'csv':
            # CSV natively supports chunked writes
            _writer_wrapper(writer, f, writer_args,
                            dict(writer_options, chunksize=chunksize))
        elif chunksize and fmt == 'json' and \
                writer_options.get('orient') == 'records' and \
                writer_options.get('lines'):
            # calculate the number of desired parts
            split_parts = int(max(1, len(df)/chunksize))
            # split the DF into parts
            for _df in np.array_split(df, split_parts):
                writer = getattr(_df, 'to_{}'.format(fmt))
                f = _writer_wrapper(writer, f, writer_args, writer_options)
                # we have to write a newline after every rounds, so won't get
                # the new round started in the same line
                try:
                    # Try to adapt to the required mode by catching TypeError
                    # Seems to be more reliable than trying to figure out the
                    # binary/text type.
                    f.write(b'\n')
                except TypeError:
                    f.write('\n')
        else:
            # in all other cases we're just calling the writer
            _writer_wrapper(writer, f, writer_args, writer_options)
        flush_and_close(f)


__version__ = '0.0.7'
