-- Usage: duckdb goose.duckdb < resort_full.sql

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';
PRAGMA memory_limit='200GB';

-- Loop over data types:

-- genefamilies: gene_family_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_uuid.parquet'
    ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_gene_family_uniref.parquet';

-- genefamilies_cpm: gene_family_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_uuid.parquet'
    ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_gene_family_uniref.parquet';

-- genefamilies_cpm_stratified: gene_family_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_stratified_uuid.parquet'
    ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_stratified_gene_family_uniref.parquet';

-- genefamilies_relab: gene_family_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_uuid.parquet'
    ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_gene_family_uniref.parquet';

-- genefamilies_relab_stratified: gene_family_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_stratified_uuid.parquet'
    ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_stratified_gene_family_uniref.parquet';

-- genefamilies_stratified: gene_family_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_stratified_uuid.parquet'
    ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_stratified_gene_family_uniref.parquet';

-- pathabundance: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_pathway_uniref.parquet';

-- pathabundance_cpm: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_pathway_uniref.parquet';

-- pathabundance_relab: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_pathway_uniref.parquet';

-- pathabundance_stratified: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_stratified_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_stratified_pathway_uniref.parquet';

-- pathabundance_cpm_stratified: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_stratified_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_stratified_pathway_uniref.parquet';

-- pathabundance_relab_stratified: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_stratified_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_stratified_pathway_uniref.parquet';

-- pathcoverage: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_pathway_uniref.parquet';

-- pathcoverage_stratified: pathway_uniref
COPY
    (SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_stratified_uuid.parquet'
    ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_stratified_pathway_uniref.parquet';
