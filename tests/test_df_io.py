# -*- coding: utf-8 -*-
import boto3
import pandas as pd
import pytest
import df_io
import random
from moto import mock_s3
import tempfile
import os
import itertools


fmts = ["csv", "feather", "json", "parquet", "pickle"]
compress = ["", ".gz", ".bz2", ".zst", ".zstd"]
compresslevel = [None] + list(range(1, 20))
chunksize = [None, 32]


def skip(fmt, compress, compresslevel, chunksize):
    """Decide whether we should skip a combination or not."""
    if not compress and compresslevel:
        return True
    if compress and not compresslevel:
        return True
    if compresslevel and compress not in (".zst", ".zstd") and compresslevel > 9:
        return True
    if chunksize and fmt not in ("csv", "json"):
        return True
    return False


def products():
    """Return test cases which make sense."""
    products = itertools.product(fmts, compress, compresslevel, chunksize)
    return [args for args in products if not skip(*args)]


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    print("RUINNING")
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    

@pytest.fixture
def df(length=1000):
    """Return a test DataFrame."""
    df = pd.DataFrame({"col1": random.sample(range(0, length), length),
                       "utf-8": ["árvíztűrő_tükörfúrógép"] * length
                       })
    return df


def compare_df(df1, df2):
    """Compare DataFrames."""
    assert df1.equals(df2)


@pytest.mark.parametrize("fmt,ext,compresslevel,chunksize", products())
def test_file_path_read_write(df, fmt, ext, compresslevel, chunksize):
    """Test write to a single file."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, f"{fmt}{ext}")
        df_io.write_df(df, path, fmt=fmt, compress_level=compresslevel,
                       chunksize=chunksize)
        assert os.path.exists(path)
        assert os.path.getsize(path) > 0
        df_read = df_io.read_df(path, fmt=fmt)
        compare_df(df, df_read)


@pytest.mark.parametrize("fmt,ext,compresslevel,chunksize", products())
def test_copy_paths(df, fmt, ext, compresslevel, chunksize):
    """Test write to many files in parallel."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, f"{fmt}{ext}")
        copy_paths = [os.path.join(d, f"{fmt}.{i}{ext}") for i in range(8)]
        df_io.write_df(df, path, copy_paths=copy_paths, fmt=fmt,
                       compress_level=compresslevel, chunksize=chunksize)
        for p in [path] + copy_paths:
            assert os.path.exists(p)
            assert os.path.getsize(p) > 0
            df_read = df_io.read_df(p, fmt=fmt)
            compare_df(df, df_read)


def test_csv_encoding(df):
    """Test if we can pass options."""
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "test.csv")
        with pytest.raises(UnicodeEncodeError):
            df_io.write_df(df, path, fmt="csv", writer_options={"encoding": "iso-8859-1"})
        with pytest.raises(LookupError):
            df_io.write_df(df, path, fmt="csv", writer_options={"encoding": "regergheh34h"})


@mock_s3
@pytest.mark.parametrize("fmt,ext,compresslevel,chunksize", products())
def test_s3_read_write(df, aws_credentials, fmt, ext, compresslevel, chunksize):
    """Test S3 read/write."""
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="df_io_test")

    path = f"s3://df_io_test/{fmt}{ext}"
    df_io.write_df(df, path, fmt=fmt, compress_level=compresslevel,
                   chunksize=chunksize)
    df_read = df_io.read_df(path, fmt=fmt)
    compare_df(df, df_read)