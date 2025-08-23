# Usage:
# python3 generate_sql.py generated_script_name

import yaml
import sys
from jinja2 import Environment, FileSystemLoader

# Set generated file name
outfile = sys.argv[1] if len(sys.argv) > 1 else "generated_script"

# Load config
with open("config/metaphlan_configs.yaml") as f:
    configs = yaml.safe_load(f)

env = Environment(
    loader=FileSystemLoader("templates"),
    trim_blocks=True,
    lstrip_blocks=True
)
template = env.get_template("metaphlan_humann_template.sql.j2")

# Pull individual configs for requested data types
requested_types = "*"

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
test_ids = [
    "d9cc81ea-c39e-46a6-a6f9-eb5584b87706",
    "38d449c8-1462-4d30-ba87-d032d95942ce",
    "5f8d4254-7653-46e3-814e-ed72cdfcb4d0",
    "0a73759e-825f-4276-9348-66fb6a6e2f86",
    "8793b1dc-3ba1-4591-82b8-4297adcfa1d7",
    "6821bf5f-ad59-4204-9d78-9cf9cac97329",
    "8eb9f7ae-88c2-44e5-967e-fe7f6090c7af",
    "cc1f30a0-45d9-41b1-b592-7d0892919ee7",
    "4985aa08-6138-4146-8ae3-952716575395",
    "fb7e8210-002a-4554-b265-873c4003e25f"
]
quoted_test_ids = [f"'{tid}'" for tid in test_ids]
outfile_prefix = "/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/examples/"

# Render master script with all configs
sql_script = template.render(
        outfile = outfile,
        key_id = key_id,
        secret = secret,
        base_prefix = base_prefix,
        test_ids = quoted_test_ids,
        outfile_prefix = outfile_prefix,
        data_types = data_types
    )
with open(f"output/{outfile}.sql", "w") as f:
    f.write(sql_script)