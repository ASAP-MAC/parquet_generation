#!/bin/bash
# create_examples.sh

# Usage: bash create_examples.sh

source /shares/CIBIO-Storage/CM/scratch/tools/20231211_2023.09_anaconda3/.conda
conda activate metagenomicsMAC

input_list=(
    "d9cc81ea-c39e-46a6-a6f9-eb5584b87706"
    "38d449c8-1462-4d30-ba87-d032d95942ce"
    "5f8d4254-7653-46e3-814e-ed72cdfcb4d0"
    "0a73759e-825f-4276-9348-66fb6a6e2f86"
    "8793b1dc-3ba1-4591-82b8-4297adcfa1d7"
    "6821bf5f-ad59-4204-9d78-9cf9cac97329"
    "8eb9f7ae-88c2-44e5-967e-fe7f6090c7af"
    "cc1f30a0-45d9-41b1-b592-7d0892919ee7"
    "4985aa08-6138-4146-8ae3-952716575395"
    "fb7e8210-002a-4554-b265-873c4003e25f"
)

for i in "${!input_list[@]}"; do
  gcloud storage cp gs://metagenomics-mac/results/cMDv4/${input_list[$i]}/metaphlan_lists/metaphlan_unknown_list.tsv.gz ../extdata/example_raw_data/example_metaphlan_relative_abundance_$((i+1)).tsv.gz  
done

for f in ../extdata/example_raw_data/*.gz; do
  gunzip "$f"
done

for f in ../extdata/example_raw_data/*; do
  if [ -f "$f" ]; then
    head -n 100 "$f" > "short_$f"
  fi
done

rm ../extdata/example_raw_data/example*