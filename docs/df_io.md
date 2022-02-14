<!-- markdownlint-disable -->

<a href="../df_io/__init__.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `df_io`
Helpers for reading/writing Pandas DataFrames. 


---

<a href="../df_io/__init__.py#L26"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>function</kbd> `read_df`

```python
read_df(path, fmt='csv', reader_args=[], reader_options={}, open_kw={})
```

Read DataFrame. 



**Args:**
 
 - <b>`path`</b> (str):  The path to read from. Can be anything, which `smart_open` supports, like `s3://bucket/file`.  Compression type is inferred  



**Kwargs:**
 
 - <b>`fmt`</b> (str):  The format to read. Should work with most of Pandas `read_*` methods. 
 - <b>`reader_args`</b> (list):  Argument list for the Pandas `read_$fmt` method. 
 - <b>`reader_options`</b> (dict):  Keyword arguments for the Pandas `read_$fmt` method. 
 - <b>`open_kw`</b> (dict):  Keyword arguments for `smart_open`. 

**Returns:**
 The read Pandas DataFrame. 


---

<a href="../df_io/__init__.py#L74"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>function</kbd> `write_df`

```python
write_df(
    df,
    path,
    copy_paths=[],
    fmt='csv',
    compress_level=6,
    chunksize=None,
    writer_args=[],
    writer_options={},
    zstd_options={'threads': -1},
    open_kw={}
)
```

Write Pandas DataFrame. 

Can write to local files and to S3 paths in any format, supported by the installed pandas version. Writer-specific arguments can be given in writer_args and writer_options. If the path parameter starts with s3://, it will try to do an S3 write, otherwise opens a local file with that path. 

Additional output files can be specified in `copy_paths` parameter, as a list of either local, or `s3://...` paths. The same output will be written there as to `path` in parallel to reduce overhead. 



**Args:**
 
 - <b>`df`</b> (pandas.DataFrame):  The DataFrame to write. 
 - <b>`path`</b> (str):  The path to write to. Can be anything, which `smart_open` supports, like `s3://bucket/file`. 



**Kwargs:**
 
 - <b>`copy_paths`</b> (list[str]):  Place a copy to these paths as well. Writes in parallel. 
 - <b>`fmt`</b> (str):  The format to write. Should work with most of Pandas `write_*` methods. 
 - <b>`compress_level`</b> (int):  Compress level, passed through to the compressor. gzip/bzip2: 1-9, zstd: 1-22. 
 - <b>`chunksize`</b> (int):  Break DataFrame into `chunksize` sized chunks and write those.  
 - <b>`writer_args`</b> (list):  Argument list for the Pandas `write_$fmt` method. 
 - <b>`writer_options`</b> (dict):  Keyword arguments for the Pandas `write_$fmt` method. 
 - <b>`zstd_options`</b> (dict):  Keyword arguments for the `zstd` compressor. 
 - <b>`open_kw`</b> (dict):  Keyword arguments for `smart_open`. 

**Returns:**
 None 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
