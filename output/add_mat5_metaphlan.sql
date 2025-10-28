-- Usage: duckdb goose.duckdb < add_mat5_metaphlan.sql

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';
PRAGMA memory_limit='200GB';

-- Loop over data types:
-- relative_abundance

CREATE OR REPLACE TABLE relative_abundance AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/relative_abundance_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/relative_abundance_uuid.parquet';

COPY
    (SELECT * FROM relative_abundance ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/relative_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM relative_abundance ORDER BY clade_name_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/relative_abundance_clade_name_species.parquet'
    (format parquet, compression 'zstd');

DROP TABLE relative_abundance;

-- viral_clusters

CREATE OR REPLACE TABLE viral_clusters AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/viral_clusters_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/viral_clusters_uuid.parquet';

COPY
    (SELECT * FROM viral_clusters ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/viral_clusters_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM viral_clusters ORDER BY genome_name ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/viral_clusters_genome_name.parquet'
    (format parquet, compression 'zstd');

DROP TABLE viral_clusters;

-- marker_abundance

CREATE OR REPLACE TABLE marker_abundance AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/marker_abundance_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/marker_abundance_uuid.parquet';

COPY
    (SELECT * FROM marker_abundance ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM marker_abundance ORDER BY uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_abundance_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE marker_abundance;

-- marker_presence

CREATE OR REPLACE TABLE marker_presence AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/marker_presence_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/marker_presence_uuid.parquet';

COPY
    (SELECT * FROM marker_presence ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_presence_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM marker_presence ORDER BY uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_presence_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE marker_presence;

