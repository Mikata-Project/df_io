import os
import gzip
import shutil
import tempfile
from io import TextIOWrapper
import numpy as np
import s3fs


def _writer_wrapper(writer, f, writer_args, writer_options):
    try:
        writer(f, *writer_args, **writer_options)
    except TypeError:
        # hack for https://github.com/pandas-dev/pandas/issues/19827
        f = TextIOWrapper(f)
        writer(f, *writer_args, **writer_options)
    return f


def write_df(df, s3_path, fmt='csv', gzip_level=9, chunksize=None,
             writer_args=[], writer_options={}):
    """
    Pandas DataFrame write helper

    Can write to local files and to S3 paths in any format, supported by the
    installed pandas version. Writer-specific arguments can be given in
    writer_args and writer_options.
    If the s3_path parameter starts with s3://, it will try to do an S3 write,
    otherwise opens a local file with that path.
    """

    def flush_and_close(f):
        """
        Flush and close for the gzip case (non-gzip will receive a double
        flush).
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
    # support S3 and local writes as well
    if s3_path.startswith('s3://'):
        s3 = s3fs.S3FileSystem(anon=False)
        _w = s3.open(s3_path, 'wb')
    else:
        _w = open(s3_path, 'wb')
    with _w as f:
        if filename.endswith('.gz'):
            f = gzip.GzipFile(filename, mode='wb', compresslevel=gzip_level,
                              fileobj=f)
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
                f.write('\n')
        else:
            # in all other cases we're just calling the writer
            _writer_wrapper(writer, f, writer_args, writer_options)
        flush_and_close(f)


__version__ = '0.0.0'
