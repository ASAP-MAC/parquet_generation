# Parquet Generation

This repo contains files involved in the process of creating formatted parquet files from output stored in `gs://metagenomics-mac`. To briefly summarize, Python and Jinja2 are used to generate SQL code according to file information detailed in YAML configs. The resulting SQL code uses DuckDB with the `httpfs` extension to pull files from the Google Bucket and consolidate them into parquet files. These parquet files are then hosted on Hugging Face in the [waldronlab/metagenomics_mac](https://huggingface.co/datasets/waldronlab/metagenomics_mac) and [waldronlab/metagenomics_mac_examples](https://huggingface.co/datasets/waldronlab/metagenomics_mac_examples/tree/main) repositories and easily accessible with the [parkinsonsMetagenomicData](https://github.com/ASAP-MAC/parkinsonsMetagenomicData) R package.

## How To Use This Repo

You will need:

* Output files from the [curatedMetagenomicsNextflow pipeline](https://github.com/seandavi/curatedMetagenomicsNextflow), stored in a Google Bucket (by default, this repository uses `gs://metagenomics-mac`)
* A list of sample IDs, unless you plan on pulling information for every sample run through the pipeline. These sample IDs will be the same as the sample IDs provided to the [curatedMetagenomicsNextflow pipeline](https://github.com/seandavi/curatedMetagenomicsNextflow)
* Credentials for the Google Bucket: specifically, you need the "key_id" and "secret"
* A local directory to store final parquet files
* A DuckDB installation (`duckdb.def` can be used to build a DuckDB Singularity container if necessary)
* Ideas for how you want to transform each type of output file into a parquet file

Once you have all of these things, here is what you will need to customize within this repo.

* YAML config files (`/config/`): Each output data type has its own entry within either `metaphlan_configs.yaml` or `humann_configs.yaml`. Make sure the input path suffixes are correct, and make any other desired adjustments.
* `generate_sql.py`:
    * `requested_types` (line 52): uncomment each data type you want to generate a SQL script for, or uncomment line 51 to generate scripts for all types.
    * shared variables (line 89): set the credentials for your Google Bucket and the prefixes of the paths to reach the output files within the bucket. Also set `tmp_dir`, which specifies a location with lots of space for DuckDB to use, and `outfile_prefix`, which specifies the path to a directory where the final parquet files will be stored (prior to uploading to any hosting sites).
    * `sample_ids` (line 111): paste your list of sample IDs here, or leave it as `"*"` to access all samples within the Google Bucket.

Now you are ready to generate the SQL scripts. Run the command `python3 generate_sql.py generated_script_name`. This command is also listed in the "Usage" section at the top of `generate_sql.py`.
The `generated_script_name` argument is simply what you would like your resulting SQL script to be called. For example, providing "all_metaphlan_types" would result in a SQL script called "all_metaphlan_types.sql".

Once your SQL scripts have been generated, simply run them using your DuckDB installation. Each script generates with a "-- Usage:" line at the top.
For example:

`-- Usage: duckdb goose.duckdb < metaphlan_all.sql`

In this line, "goose.duckdb" is the name of the database file you want DuckDB to work in. This file can be one that already exists, or it will generate it if it does not exist. This file can be discarded afterwards or retained to be reused for further scripts. By default, no tables are retained within the file.

If you are using a Singularity container created by the included `duckdb.def` file, you can run this interactively within the container or use the following `singularity run` command. This can be useful for running the script as a batch job on an HPC. You will of course need to set the environment variables appropriately.

`singularity run --bind /parquet_output_location:/parquet_output_location $SIF_PATH $DB_FILE < $SQL_SCRIPT`

Running the SQL script will create your requested parquet files and save them to the location you specified. At this point, you are free to do what you like with them. For the parquet files available through the [parkinsonsMetagenomicData package](https://github.com/ASAP-MAC/parkinsonsMetagenomicData), they are pushed to a Hugging Face repository. If you are doing the same, here is the basic command used for your convenience:

`hf upload waldronlab/metagenomics_mac --repo-type=dataset --include="relative_abundance*.parquet" --commit-message="adding new samples to relative abundance"`

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
