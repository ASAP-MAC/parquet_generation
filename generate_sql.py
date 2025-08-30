# Usage:
# python3 generate_sql.py generated_script_name

import yaml
import sys
import math
import copy
import os
from jinja2 import Environment, FileSystemLoader
from google.cloud import storage

# Set generated file name
outfile = sys.argv[1] if len(sys.argv) > 1 else "generated_script"

# Function to get all available UUIDS
def get_uuids_from_data_type(suffix, bucket_name="metagenomics-mac", prefix="results/cMDv4/"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
     
    uuids = set()
    blobs = bucket.list_blobs(match_glob=f"{prefix}*{suffix}")
     
    uuids = {
    blob.name.replace(prefix, "").replace(suffix, "")
    for blob in blobs
    }
     
    return list(uuids)

# Function to chunk list to approximate size
def chunk_list(lst, chunk_size):
    n = len(lst)
    num_chunks = math.ceil(n / chunk_size)
    size_per_chunk = math.ceil(n / num_chunks)
     
    return [lst[i:i + size_per_chunk] for i in range(0, n, size_per_chunk)]

# Load config
with open("config/humann_configs.yaml") as f:
    configs = yaml.safe_load(f)

env = Environment(
    loader=FileSystemLoader("templates"),
    trim_blocks=True,
    lstrip_blocks=True
)
basic_template = env.get_template("basic_template.sql.j2")
chunking_template = env.get_template("chunking_template.sql.j2")

# Pull individual configs for requested data types
#requested_types = "*"
requested_types = [
#    "relative_abundance",
#    "viral_clusters",
#    "marker_abundance",
#    "marker_presence",
    "genefamilies",
    "genefamilies_cpm",
    "genefamilies_cpm_stratified",
#    "genefamilies_cpm_unstratified",
    "genefamilies_relab",
    "genefamilies_relab_stratified",
#    "genefamilies_relab_unstratified",
    "genefamilies_stratified",
#    "genefamilies_unstratified",
#    "pathabundance",
#    "pathabundance_cpm",
#    "pathabundance_relab",
#    "pathabundance_stratified",
#    "pathabundance_unstratified",
#    "pathabundance_cpm_stratified",
#    "pathabundance_relab_stratified",
#    "pathabundance_cpm_unstratified",
#    "pathabundance_relab_unstratified",
#    "pathcoverage",
#    "pathcoverage_stratified",
#    "pathcoverage_unstratified"
]

if isinstance(requested_types, str):
    requested_types = [requested_types]

data_types = [
    {"name": name, **details}
    for name, details in configs.items()
    if "*" in requested_types or name in requested_types
]

# Set shared variables
key_id = "KEY_ID"
secret = "SECRET"
base_prefix = "gs://metagenomics-mac/results/cMDv4/"
tmp_dir = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb"
mem_limit = "200GB"
threads = "6"
gbucket = "metagenomics-mac"
bucket_prefix = "results/cMDv4/"
outfile_prefix = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/"
#test_ids = [
#    "d9cc81ea-c39e-46a6-a6f9-eb5584b87706",
#    "38d449c8-1462-4d30-ba87-d032d95942ce",
#    "5f8d4254-7653-46e3-814e-ed72cdfcb4d0",
#    "0a73759e-825f-4276-9348-66fb6a6e2f86",
#    "8793b1dc-3ba1-4591-82b8-4297adcfa1d7",
#    "6821bf5f-ad59-4204-9d78-9cf9cac97329",
#    "8eb9f7ae-88c2-44e5-967e-fe7f6090c7af",
#    "cc1f30a0-45d9-41b1-b592-7d0892919ee7",
#    "4985aa08-6138-4146-8ae3-952716575395",
#    "fb7e8210-002a-4554-b265-873c4003e25f"
#]
sample_ids = "*"

# Split into chunked vs non-chunked
chunked_types = [dt for dt in data_types if dt.get("chunk_samples") and sample_ids == "*"]
non_chunked_types = [dt for dt in data_types if not dt.get("chunk_samples")]

# Render single script for each chunked type
if chunked_types:
    for dt in chunked_types:
        chunk_size = dt.get("chunk_size", 500)
        print(f"{dt['name']} will be chunked into size ~{chunk_size}")
         
        # Retrieve UUIDs dynamically
        uuids = get_uuids_from_data_type(dt["input_path_suffix"], gbucket, bucket_prefix)
        chunks = chunk_list(uuids, chunk_size)
         
        # Add chunks to config
        dt["uuid_chunks"] = chunks
         
        # Duplicate config for each chunk with UUIDS
        chunk_configs = []
        for i, chunk in enumerate(chunks):
            new_config = copy.deepcopy(dt)
            new_config["name"] = f"{dt['name']}_chunk{i}"
            new_config["table_name"] = f"{dt['table_name']}_chunk{i}"
            new_config["uuids"] = chunk
            chunk_configs.append(new_config)
         
        # Render script with all chunks
        sql_script = chunking_template.render(
            outfile = outfile,
            key_id = key_id,
            secret = secret,
            base_prefix = base_prefix,
            outfile_prefix = outfile_prefix,
            tmp_dir = tmp_dir,
            mem_limit = mem_limit,
            threads = threads,
            data_type = dt
        )
        os.makedirs(f"output/{outfile}_chunked", exist_ok=True)
        with open(f"output/{outfile}_chunked/{dt['name']}_chunked.sql", "w") as f:
            f.write(sql_script)


# Render single script with all non-chunked types
if non_chunked_types:
    sql_script = basic_template.render(
            outfile = outfile,
            key_id = key_id,
            secret = secret,
            base_prefix = base_prefix,
            sample_ids = sample_ids,
            outfile_prefix = outfile_prefix,
            tmp_dir = tmp_dir,
            mem_limit = mem_limit,
            threads = threads,
            data_types = non_chunked_types
        )
    with open(f"output/{outfile}.sql", "w") as f:
        f.write(sql_script)