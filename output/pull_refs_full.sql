-- Usage: duckdb goose.duckdb < resort_full.sql

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';
PRAGMA memory_limit='200GB';

-- HUMAnN: gene_family

CREATE OR REPLACE TABLE genefamilies_distinct AS 
    SELECT DISTINCT gene_family
    FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_uuid.parquet'
    ORDER BY gene_family ASC;

COPY
    (SELECT
        gene_family,
        string_split_regex(gene_family, '[|.]')[1] AS gene_family_uniref,
        string_split_regex(gene_family, '[|.]')[2] AS gene_family_genus,
        string_split_regex(gene_family, '[|.]')[3] AS gene_family_species
    FROM genefamilies_distinct)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/gene_family_ref.parquet';

-- HUMAnN: pathway

CREATE OR REPLACE TABLE pathways_distinct AS 
    SELECT DISTINCT pathway
    FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_uuid.parquet'
    ORDER BY pathway ASC;

COPY
    (SELECT
        pathway,
        string_split_regex(pathway, '[|.]')[1] AS pathway_uniref,
        string_split_regex(pathway, '[|.]')[2] AS pathway_genus,
        string_split_regex(pathway, '[|.]')[3] AS pathway_species
    FROM pathways_distinct)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathway_ref.parquet';

-- MetaPhlAn: clade_name

CREATE OR REPLACE TABLE clade_names_distinct AS 
    SELECT DISTINCT clade_name, NCBI_tax_id
    FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/relative_abundance_uuid.parquet'
    ORDER BY clade_name ASC;

COPY
    (SELECT
        clade_name,
        string_split_regex(clade_name, '[|]')[1] AS clade_name_kingdom,
        string_split_regex(clade_name, '[|]')[2] AS clade_name_phylum,
        string_split_regex(clade_name, '[|]')[3] AS clade_name_class,
        string_split_regex(clade_name, '[|]')[4] AS clade_name_order,
        string_split_regex(clade_name, '[|]')[5] AS clade_name_family,
        string_split_regex(clade_name, '[|]')[6] AS clade_name_genus,
        string_split_regex(clade_name, '[|]')[7] AS clade_name_species,
        string_split_regex(clade_name, '[|]')[8] AS clade_name_terminal,
        NCBI_tax_id,
        string_split_regex(NCBI_tax_id, '[|]')[1] AS NCBI_tax_id_kingdom,
        string_split_regex(NCBI_tax_id, '[|]')[2] AS NCBI_tax_id_phylum,
        string_split_regex(NCBI_tax_id, '[|]')[3] AS NCBI_tax_id_class,
        string_split_regex(NCBI_tax_id, '[|]')[4] AS NCBI_tax_id_order,
        string_split_regex(NCBI_tax_id, '[|]')[5] AS NCBI_tax_id_family,
        string_split_regex(NCBI_tax_id, '[|]')[6] AS NCBI_tax_id_genus,
        string_split_regex(NCBI_tax_id, '[|]')[7] AS NCBI_tax_id_species,
        string_split_regex(NCBI_tax_id, '[|]')[8] AS NCBI_tax_id_terminal
    FROM clade_names_distinct)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/clade_name_ref.parquet';

-- MetaPhlAn: genome_name

CREATE OR REPLACE TABLE genome_names_distinct AS 
    SELECT DISTINCT genome_name
    FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/viral_clusters_uuid.parquet'
    ORDER BY genome_name ASC;

COPY
    (SELECT * FROM genome_names_distinct)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genome_name_ref.parquet';

-- MetaPhlAn: uniref_marker

CREATE OR REPLACE TABLE uniref_markers_distinct AS 
    SELECT DISTINCT uniref
    FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_abundance_uuid.parquet'
    ORDER BY uniref ASC;

COPY
    (SELECT * FROM uniref_markers_distinct)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/uniref_marker_ref.parquet';