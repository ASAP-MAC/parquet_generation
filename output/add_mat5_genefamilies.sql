-- Usage: duckdb goose.duckdb < add_mat5_humann.sql

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';
PRAGMA memory_limit='300GB';

-- Loop over data types:
-- genefamilies

CREATE OR REPLACE TABLE genefamilies AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_uuid.parquet';

COPY
    (SELECT * FROM genefamilies ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_gene_family_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies;

-- genefamilies_cpm

CREATE OR REPLACE TABLE genefamilies_cpm AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_cpm_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_cpm ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_gene_family_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_cpm;

-- genefamilies_cpm_stratified

CREATE OR REPLACE TABLE genefamilies_cpm_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_cpm_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_stratified_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_cpm_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_stratified ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_stratified_gene_family_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_cpm_stratified;

-- genefamilies_cpm_unstratified

CREATE OR REPLACE TABLE genefamilies_cpm_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_cpm_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_unstratified_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_cpm_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_unstratified ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_unstratified_gene_family.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_cpm_unstratified;

-- genefamilies_relab

CREATE OR REPLACE TABLE genefamilies_relab AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_relab_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_relab ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_gene_family_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_relab;

-- genefamilies_relab_stratified

CREATE OR REPLACE TABLE genefamilies_relab_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_relab_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_stratified_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_relab_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_stratified ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_stratified_gene_family_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_relab_stratified;

-- genefamilies_relab_unstratified

CREATE OR REPLACE TABLE genefamilies_relab_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_relab_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_unstratified_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_relab_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_unstratified ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_unstratified_gene_family.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_relab_unstratified;

-- genefamilies_stratified

CREATE OR REPLACE TABLE genefamilies_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_stratified_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_stratified ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_stratified_gene_family_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_stratified;

-- genefamilies_unstratified

CREATE OR REPLACE TABLE genefamilies_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/genefamilies_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_unstratified_uuid.parquet';

COPY
    (SELECT * FROM genefamilies_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_unstratified ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_unstratified_gene_family.parquet'
    (format parquet, compression 'zstd');

DROP TABLE genefamilies_unstratified;
