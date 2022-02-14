# df_io
Python helpers for doing IO with Pandas DataFrames

# Available methods
## read_df

* bzip2/gzip/zstandard compression
* passing parameters to Pandas' readers
* reading from anything, which `smart_open` supports (local files, AWS S3 etc)
* most of the available formats, Pandas supports

## write_df

This method supports:
* streaming writes
* chunked writes
* bzip2/gzip/zstandard compression
* passing parameters to Pandas' writers
* writing to anything, which `smart_open` supports (local files, AWS S3 etc)
* most of the available formats, Pandas supports

# Documentation

[API doc](https://github.com/Mikata-Project/df_io/tree/master/docs/df_io.md)

### Examples

Write a Pandas DataFrame (df) to an S3 path in CSV format (the default):

```python
import df_io

df_io.write_df(df, 's3://bucket/dir/mydata.csv')
```

The same with gzip compression:

```python
df_io.write_df(df, 's3://bucket/dir/mydata.csv.gz')
```

With zstandard compression using pickle:

```python
df_io.write_df(df, 's3://bucket/dir/mydata.pickle.zstd', fmt='pickle')
```


Using JSON lines:

```python
df_io.write_df(df, 's3://bucket/dir/mydata.json.gz', fmt='json')
```

Passing writer parameters:

```python
df_io.write_df(df, 's3://bucket/dir/mydata.json.gz', fmt='json', writer_options={'lines': False})
```

Chunked write (splitting the df into equally sized parts and creating/writing outputs for them):

```python
df_io.write_df(df, 's3://bucket/dir/mydata.json.gz', fmt='json', chunksize=10000)
```
