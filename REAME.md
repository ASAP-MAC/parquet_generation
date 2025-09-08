# Parquet Generation

This repo contains files involved in the process of creating formatted parquet files from output stored in `gs://metagenomics-mac`. To briefly summarize, Python and Jinja2 are used to generate SQL code according to file information detailed in YAML configs. The resulting SQL code uses DuckDB with the `httpfs` extension to pull files from the Google Bucket and consolidate them into parquet files.

## File Types

Files in this repo fall into four types.

* **YAML config**
  * Location: `/config`
  * The config scripts contain information on the layout and desired organization of the output files stored in `gs://metagenomics-mac`.
* **Jinja template**
  * Location: `/templates`
  * The templates contain SQL script formatted according to [Jinja Templating Syntax](https://jinja.palletsprojects.com/en/stable/templates/). These statements will be expanded according to YAML config values.
* **Python generator script**
  * Location: `/generate_sql.py`
  * This script combines the YAML config and Jinja template files to produce SQL code. A number of variables within the script can be adjusted to select specific data types, sample IDs, and output paths.
* **SQL output**
  * Location: `/output`
  * This is the plain SQL output that is generated when `generate_sql.py` is run. If a run includes both chunked and non-chunked data types, the output for that run will include a `run_name.sql` file and a `run_name_chunked/` directory that contains individual files for each chunked data type. See more under "Chunking".
* **DuckDB Singularity Definition file:**
  * Location: `/duckdb.def`
  * This Definition file can be used to build a Singularity container suitable for running the SQL output. This is not required, but may be useful.

## Config Parameters

The base parameters described in the YAML config files are as follows:

* **table_name:** base name to use in naming DuckDB tables. Improves SQL readability.
* **description:** brief description of the files being processed to include as a SQL comment.
* **input_path_suffix:** path to the relevant files within the Google Bucket, starting from an indivual sample's directory.
* **chunk_samples:** whether or not to chunk the provided samples, as a Boolean.
* **chunk_size:** if `chunk_samples: true`, the approximate number of samples to include in each chunk.
* **columns:** names and types of the columns in the file, as a name-value pair (`column_name: TYPE`).
* **header:** custom names for any header rows found in the delimited file, identified by index (`row_name: i`).
* **columns_to_split:** details on how to split multi-value columns. Includes the column name, delimiter as a single character or character set, and custom names for the parts that result from the split, by index. Example structure below:
``` yaml
column_name:
  delimiter: '[|.]'
  parts:
    column_name_first_part: 1
    column_name_second_part: 2
    column_name_third_part: 3
```
* **columns_to_sort:** names of columns to sort by. A separate parquet file will be created for each of these.

## Memory Management and Chunking

For some data types with a large base file size, DuckDB will use a very large amount of memory handling all samples at once. If this is causing issues with executing the SQL, processing the samples in smaller chunks may be desired. To do this, provide `chunk_samples: true` and `chunk_size: 750` (or another number) in the YAML config for that data type. When rendering the SQL, that data type will get its own `.sql` script in which each chunk is retrieved, parsed, and formatted separately, then merged and sorted at the end.

In these scenarios, it is also important to set `mem_limit` and `threads` appropriately within `generate_sql.py`. These values will vary based on available resources but can help optimize processing and avoid memory overload.