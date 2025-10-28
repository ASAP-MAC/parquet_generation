# Usage:
# python3 generate_resorts.py generated_script_name

import yaml
import sys
from jinja2 import Environment, FileSystemLoader

# Set generated file name
outfile = sys.argv[1] if len(sys.argv) > 1 else "generated_script"

# Load config
with open("config/humann_configs.yaml") as f:
    configs = yaml.safe_load(f)

env = Environment(
    loader=FileSystemLoader("templates"),
    trim_blocks=True,
    lstrip_blocks=True
)
basic_template = env.get_template("merge_template.sql.j2")

# Pull individual configs for requested data types
requested_types = "*"
#requested_types = [
#    "relative_abundance",
#    "viral_clusters",
#    "marker_abundance",
#    "marker_presence",
#    "genefamilies",
#    "genefamilies_cpm",
#    "genefamilies_cpm_stratified",
#    "genefamilies_cpm_unstratified",
#    "genefamilies_relab",
#    "genefamilies_relab_stratified",
#    "genefamilies_relab_unstratified",
#    "genefamilies_stratified",
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
#]

if isinstance(requested_types, str):
    requested_types = [requested_types]

data_types = [
    {"name": name, **details}
    for name, details in configs.items()
    if "*" in requested_types or name in requested_types
]

# Set shared variables
tmp_dir = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb"
mem_limit = "200GB"
threads = "6"
infile_1_prefix = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/"
infile_2_prefix = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/"
outfile_prefix = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/"
base_sort = "uuid"

# Render single script
sql_script = basic_template.render(
        outfile = outfile,
        infile_1_prefix = infile_1_prefix,
        infile_2_prefix = infile_2_prefix,
        outfile_prefix = outfile_prefix,
        tmp_dir = tmp_dir,
        mem_limit = mem_limit,
        threads = threads,
        data_types = data_types,
        base_sort = base_sort
    )
with open(f"output/{outfile}.sql", "w") as f:
    f.write(sql_script)