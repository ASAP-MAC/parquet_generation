-- Usage: duckdb goose.duckdb < add_mat5_humann.sql

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';
PRAGMA memory_limit='200GB';

-- Loop over data types:
-- pathabundance

CREATE OR REPLACE TABLE pathabundance AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_uuid.parquet';

COPY
    (SELECT * FROM pathabundance ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance;

-- pathabundance_cpm

CREATE OR REPLACE TABLE pathabundance_cpm AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_cpm_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_cpm ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_cpm;

-- pathabundance_relab

CREATE OR REPLACE TABLE pathabundance_relab AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_relab_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_relab ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_relab;

-- pathabundance_stratified

CREATE OR REPLACE TABLE pathabundance_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_stratified_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_stratified ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_stratified_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_stratified;

-- pathabundance_unstratified

CREATE OR REPLACE TABLE pathabundance_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_unstratified_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_unstratified ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_unstratified;

-- pathabundance_cpm_stratified

CREATE OR REPLACE TABLE pathabundance_cpm_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_cpm_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_stratified_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_cpm_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_stratified ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_stratified_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_cpm_stratified;

-- pathabundance_relab_stratified

CREATE OR REPLACE TABLE pathabundance_relab_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_relab_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_stratified_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_relab_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_stratified ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_stratified_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_relab_stratified;

-- pathabundance_cpm_unstratified

CREATE OR REPLACE TABLE pathabundance_cpm_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_cpm_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_unstratified_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_cpm_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_unstratified ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_cpm_unstratified;

-- pathabundance_relab_unstratified

CREATE OR REPLACE TABLE pathabundance_relab_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathabundance_relab_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_unstratified_uuid.parquet';

COPY
    (SELECT * FROM pathabundance_relab_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_unstratified ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathabundance_relab_unstratified;

-- pathcoverage

CREATE OR REPLACE TABLE pathcoverage AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathcoverage_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_uuid.parquet';

COPY
    (SELECT * FROM pathcoverage ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathcoverage;

-- pathcoverage_stratified

CREATE OR REPLACE TABLE pathcoverage_stratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathcoverage_stratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_stratified_uuid.parquet';

COPY
    (SELECT * FROM pathcoverage_stratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_stratified ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_stratified_pathway_uniref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathcoverage_stratified;

-- pathcoverage_unstratified

CREATE OR REPLACE TABLE pathcoverage_unstratified AS
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac_old/pathcoverage_unstratified_uuid.parquet'
UNION ALL
SELECT * FROM '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_unstratified_uuid.parquet';

COPY
    (SELECT * FROM pathcoverage_unstratified ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_unstratified ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');

DROP TABLE pathcoverage_unstratified;

