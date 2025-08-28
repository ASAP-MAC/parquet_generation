-- Usage: duckdb goose.duckdb < full_humann.sql
install httpfs;
load httpfs;
CREATE SECRET metagenomics_mac (
    TYPE gcs,
    KEY_ID 'KEY_ID',
    SECRET 'SECRET'
);

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';

-- Wildcard mode: read all matching files
SET VARIABLE test_prefixes = list_value('gs://metagenomics-mac/results/cMDv4/*');

-- Loop over data types:

-- HUMAnN: genefamilies (out_genefamilies.tsv.gz)

SET VARIABLE genefamilies_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_scan',
    rejects_table='genefamilies_errors');

CREATE OR REPLACE TABLE genefamilies_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_joined AS SELECT
    string_split_regex(t.gene_family, '[|.]')[1] AS gene_family_uniref,
    string_split_regex(t.gene_family, '[|.]')[2] AS gene_family_genus,
    string_split_regex(t.gene_family, '[|.]')[3] AS gene_family_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies AS t
INNER JOIN genefamilies_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_joined ORDER BY gene_family_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_gene_family_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies;
DROP TABLE genefamilies_scan;
DROP TABLE genefamilies_errors;
DROP TABLE genefamilies_headers;
DROP TABLE genefamilies_joined;

-- HUMAnN: genefamilies_cpm (out_genefamilies_cpm.tsv.gz)

SET VARIABLE genefamilies_cpm_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_cpm AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_cpm.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_cpm_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_cpm_scan',
    rejects_table='genefamilies_cpm_errors');

CREATE OR REPLACE TABLE genefamilies_cpm_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_cpm_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_cpm_joined AS SELECT
    string_split_regex(t.gene_family, '[|.]')[1] AS gene_family_uniref,
    string_split_regex(t.gene_family, '[|.]')[2] AS gene_family_genus,
    string_split_regex(t.gene_family, '[|.]')[3] AS gene_family_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_cpm AS t
INNER JOIN genefamilies_cpm_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_cpm_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_cpm_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_joined ORDER BY gene_family_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_gene_family_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_cpm;
DROP TABLE genefamilies_cpm_scan;
DROP TABLE genefamilies_cpm_errors;
DROP TABLE genefamilies_cpm_headers;
DROP TABLE genefamilies_cpm_joined;

-- HUMAnN: genefamilies_cpm_stratified (out_genefamilies_cpm_stratified.tsv.gz)

SET VARIABLE genefamilies_cpm_stratified_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_cpm_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_cpm_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_cpm_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_cpm_stratified_scan',
    rejects_table='genefamilies_cpm_stratified_errors');

CREATE OR REPLACE TABLE genefamilies_cpm_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_cpm_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_cpm_stratified_joined AS SELECT
    string_split_regex(t.gene_family, '[|.]')[1] AS gene_family_uniref,
    string_split_regex(t.gene_family, '[|.]')[2] AS gene_family_genus,
    string_split_regex(t.gene_family, '[|.]')[3] AS gene_family_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_cpm_stratified AS t
INNER JOIN genefamilies_cpm_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_cpm_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_cpm_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_stratified_joined ORDER BY gene_family_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_stratified_gene_family_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_cpm_stratified;
DROP TABLE genefamilies_cpm_stratified_scan;
DROP TABLE genefamilies_cpm_stratified_errors;
DROP TABLE genefamilies_cpm_stratified_headers;
DROP TABLE genefamilies_cpm_stratified_joined;

-- HUMAnN: genefamilies_cpm_unstratified (out_genefamilies_cpm_unstratified.tsv.gz)

SET VARIABLE genefamilies_cpm_unstratified_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_cpm_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_cpm_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_cpm_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_cpm_unstratified_scan',
    rejects_table='genefamilies_cpm_unstratified_errors');

CREATE OR REPLACE TABLE genefamilies_cpm_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_cpm_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_cpm_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_cpm_unstratified AS t
INNER JOIN genefamilies_cpm_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_cpm_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_cpm_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_unstratified_joined ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_cpm_unstratified_gene_family.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_cpm_unstratified;
DROP TABLE genefamilies_cpm_unstratified_scan;
DROP TABLE genefamilies_cpm_unstratified_errors;
DROP TABLE genefamilies_cpm_unstratified_headers;
DROP TABLE genefamilies_cpm_unstratified_joined;

-- HUMAnN: genefamilies_relab (out_genefamilies_relab.tsv.gz)

SET VARIABLE genefamilies_relab_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_relab AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_relab.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_relab_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_relab_scan',
    rejects_table='genefamilies_relab_errors');

CREATE OR REPLACE TABLE genefamilies_relab_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_relab_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_relab_joined AS SELECT
    string_split_regex(t.gene_family, '[|.]')[1] AS gene_family_uniref,
    string_split_regex(t.gene_family, '[|.]')[2] AS gene_family_genus,
    string_split_regex(t.gene_family, '[|.]')[3] AS gene_family_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_relab AS t
INNER JOIN genefamilies_relab_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_relab_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_relab_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_joined ORDER BY gene_family_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_gene_family_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_relab;
DROP TABLE genefamilies_relab_scan;
DROP TABLE genefamilies_relab_errors;
DROP TABLE genefamilies_relab_headers;
DROP TABLE genefamilies_relab_joined;

-- HUMAnN: genefamilies_relab_stratified (out_genefamilies_relab_stratified.tsv.gz)

SET VARIABLE genefamilies_relab_stratified_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_relab_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_relab_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_relab_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_relab_stratified_scan',
    rejects_table='genefamilies_relab_stratified_errors');

CREATE OR REPLACE TABLE genefamilies_relab_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_relab_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_relab_stratified_joined AS SELECT
    string_split_regex(t.gene_family, '[|.]')[1] AS gene_family_uniref,
    string_split_regex(t.gene_family, '[|.]')[2] AS gene_family_genus,
    string_split_regex(t.gene_family, '[|.]')[3] AS gene_family_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_relab_stratified AS t
INNER JOIN genefamilies_relab_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_relab_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_relab_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_stratified_joined ORDER BY gene_family_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_stratified_gene_family_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_relab_stratified;
DROP TABLE genefamilies_relab_stratified_scan;
DROP TABLE genefamilies_relab_stratified_errors;
DROP TABLE genefamilies_relab_stratified_headers;
DROP TABLE genefamilies_relab_stratified_joined;

-- HUMAnN: genefamilies_relab_unstratified (out_genefamilies_relab_unstratified.tsv.gz)

SET VARIABLE genefamilies_relab_unstratified_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_relab_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_relab_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_relab_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_relab_unstratified_scan',
    rejects_table='genefamilies_relab_unstratified_errors');

CREATE OR REPLACE TABLE genefamilies_relab_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_relab_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_relab_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_relab_unstratified AS t
INNER JOIN genefamilies_relab_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_relab_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_relab_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_unstratified_joined ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_relab_unstratified_gene_family.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_relab_unstratified;
DROP TABLE genefamilies_relab_unstratified_scan;
DROP TABLE genefamilies_relab_unstratified_errors;
DROP TABLE genefamilies_relab_unstratified_headers;
DROP TABLE genefamilies_relab_unstratified_joined;

-- HUMAnN: genefamilies_stratified (out_genefamilies_stratified.tsv.gz)

SET VARIABLE genefamilies_stratified_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_stratified_scan',
    rejects_table='genefamilies_stratified_errors');

CREATE OR REPLACE TABLE genefamilies_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_stratified_joined AS SELECT
    string_split_regex(t.gene_family, '[|.]')[1] AS gene_family_uniref,
    string_split_regex(t.gene_family, '[|.]')[2] AS gene_family_genus,
    string_split_regex(t.gene_family, '[|.]')[3] AS gene_family_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_stratified AS t
INNER JOIN genefamilies_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_stratified_joined ORDER BY gene_family_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_stratified_gene_family_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_stratified;
DROP TABLE genefamilies_stratified_scan;
DROP TABLE genefamilies_stratified_errors;
DROP TABLE genefamilies_stratified_headers;
DROP TABLE genefamilies_stratified_joined;

-- HUMAnN: genefamilies_unstratified (out_genefamilies_unstratified.tsv.gz)

SET VARIABLE genefamilies_unstratified_columns =
    struct_pack(
        gene_family := 'VARCHAR',
        rpk_abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE genefamilies_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_genefamilies_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('genefamilies_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='genefamilies_unstratified_scan',
    rejects_table='genefamilies_unstratified_errors');

CREATE OR REPLACE TABLE genefamilies_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM genefamilies_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE genefamilies_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM genefamilies_unstratified AS t
INNER JOIN genefamilies_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN genefamilies_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM genefamilies_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_unstratified_joined ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/genefamilies_unstratified_gene_family.parquet'
    (format parquet, compression 'zstd');


DROP TABLE genefamilies_unstratified;
DROP TABLE genefamilies_unstratified_scan;
DROP TABLE genefamilies_unstratified_errors;
DROP TABLE genefamilies_unstratified_headers;
DROP TABLE genefamilies_unstratified_joined;

-- HUMAnN: pathabundance (out_pathabundance.tsv.gz)

SET VARIABLE pathabundance_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_scan',
    rejects_table='pathabundance_errors');

CREATE OR REPLACE TABLE pathabundance_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance AS t
INNER JOIN pathabundance_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance;
DROP TABLE pathabundance_scan;
DROP TABLE pathabundance_errors;
DROP TABLE pathabundance_headers;
DROP TABLE pathabundance_joined;

-- HUMAnN: pathabundance_cpm (out_pathabundance_cpm.tsv.gz)

SET VARIABLE pathabundance_cpm_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_cpm AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_cpm.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_cpm_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_cpm_scan',
    rejects_table='pathabundance_cpm_errors');

CREATE OR REPLACE TABLE pathabundance_cpm_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_cpm_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_cpm_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_cpm AS t
INNER JOIN pathabundance_cpm_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_cpm_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_cpm_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_cpm;
DROP TABLE pathabundance_cpm_scan;
DROP TABLE pathabundance_cpm_errors;
DROP TABLE pathabundance_cpm_headers;
DROP TABLE pathabundance_cpm_joined;

-- HUMAnN: pathabundance_relab (out_pathabundance_relab.tsv.gz)

SET VARIABLE pathabundance_relab_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_relab AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_relab.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_relab_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_relab_scan',
    rejects_table='pathabundance_relab_errors');

CREATE OR REPLACE TABLE pathabundance_relab_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_relab_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_relab_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_relab AS t
INNER JOIN pathabundance_relab_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_relab_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_relab_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_relab;
DROP TABLE pathabundance_relab_scan;
DROP TABLE pathabundance_relab_errors;
DROP TABLE pathabundance_relab_headers;
DROP TABLE pathabundance_relab_joined;

-- HUMAnN: pathabundance_stratified (out_pathabundance_stratified.tsv.gz)

SET VARIABLE pathabundance_stratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_stratified_scan',
    rejects_table='pathabundance_stratified_errors');

CREATE OR REPLACE TABLE pathabundance_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_stratified_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_stratified AS t
INNER JOIN pathabundance_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_stratified_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_stratified_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_stratified;
DROP TABLE pathabundance_stratified_scan;
DROP TABLE pathabundance_stratified_errors;
DROP TABLE pathabundance_stratified_headers;
DROP TABLE pathabundance_stratified_joined;

-- HUMAnN: pathabundance_unstratified (out_pathabundance_unstratified.tsv.gz)

SET VARIABLE pathabundance_unstratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_unstratified_scan',
    rejects_table='pathabundance_unstratified_errors');

CREATE OR REPLACE TABLE pathabundance_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_unstratified AS t
INNER JOIN pathabundance_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_unstratified;
DROP TABLE pathabundance_unstratified_scan;
DROP TABLE pathabundance_unstratified_errors;
DROP TABLE pathabundance_unstratified_headers;
DROP TABLE pathabundance_unstratified_joined;

-- HUMAnN: pathabundance_cpm_stratified (out_pathabundance_cpm_stratified.tsv.gz)

SET VARIABLE pathabundance_cpm_stratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_cpm_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_cpm_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_cpm_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_cpm_stratified_scan',
    rejects_table='pathabundance_cpm_stratified_errors');

CREATE OR REPLACE TABLE pathabundance_cpm_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_cpm_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_cpm_stratified_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_cpm_stratified AS t
INNER JOIN pathabundance_cpm_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_cpm_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_cpm_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_stratified_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_stratified_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_cpm_stratified;
DROP TABLE pathabundance_cpm_stratified_scan;
DROP TABLE pathabundance_cpm_stratified_errors;
DROP TABLE pathabundance_cpm_stratified_headers;
DROP TABLE pathabundance_cpm_stratified_joined;

-- HUMAnN: pathabundance_relab_stratified (out_pathabundance_relab_stratified.tsv.gz)

SET VARIABLE pathabundance_relab_stratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_relab_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_relab_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_relab_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_relab_stratified_scan',
    rejects_table='pathabundance_relab_stratified_errors');

CREATE OR REPLACE TABLE pathabundance_relab_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_relab_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_relab_stratified_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_relab_stratified AS t
INNER JOIN pathabundance_relab_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_relab_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_relab_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_stratified_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_stratified_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_relab_stratified;
DROP TABLE pathabundance_relab_stratified_scan;
DROP TABLE pathabundance_relab_stratified_errors;
DROP TABLE pathabundance_relab_stratified_headers;
DROP TABLE pathabundance_relab_stratified_joined;

-- HUMAnN: pathabundance_cpm_unstratified (out_pathabundance_cpm_unstratified.tsv.gz)

SET VARIABLE pathabundance_cpm_unstratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_cpm_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_cpm_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_cpm_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_cpm_unstratified_scan',
    rejects_table='pathabundance_cpm_unstratified_errors');

CREATE OR REPLACE TABLE pathabundance_cpm_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_cpm_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_cpm_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_cpm_unstratified AS t
INNER JOIN pathabundance_cpm_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_cpm_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_cpm_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_cpm_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_cpm_unstratified;
DROP TABLE pathabundance_cpm_unstratified_scan;
DROP TABLE pathabundance_cpm_unstratified_errors;
DROP TABLE pathabundance_cpm_unstratified_headers;
DROP TABLE pathabundance_cpm_unstratified_joined;

-- HUMAnN: pathabundance_relab_unstratified (out_pathabundance_relab_unstratified.tsv.gz)

SET VARIABLE pathabundance_relab_unstratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathabundance_relab_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathabundance_relab_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathabundance_relab_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathabundance_relab_unstratified_scan',
    rejects_table='pathabundance_relab_unstratified_errors');

CREATE OR REPLACE TABLE pathabundance_relab_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathabundance_relab_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathabundance_relab_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathabundance_relab_unstratified AS t
INNER JOIN pathabundance_relab_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathabundance_relab_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathabundance_relab_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathabundance_relab_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathabundance_relab_unstratified;
DROP TABLE pathabundance_relab_unstratified_scan;
DROP TABLE pathabundance_relab_unstratified_errors;
DROP TABLE pathabundance_relab_unstratified_headers;
DROP TABLE pathabundance_relab_unstratified_joined;

-- HUMAnN: pathcoverage (out_pathcoverage.tsv.gz)

SET VARIABLE pathcoverage_columns =
    struct_pack(
        pathway := 'VARCHAR',
        coverage := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathcoverage AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathcoverage.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathcoverage_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathcoverage_scan',
    rejects_table='pathcoverage_errors');

CREATE OR REPLACE TABLE pathcoverage_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathcoverage_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathcoverage_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathcoverage AS t
INNER JOIN pathcoverage_scan AS s ON t.filename = s.file_path
INNER JOIN pathcoverage_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathcoverage_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathcoverage;
DROP TABLE pathcoverage_scan;
DROP TABLE pathcoverage_errors;
DROP TABLE pathcoverage_headers;
DROP TABLE pathcoverage_joined;

-- HUMAnN: pathcoverage_stratified (out_pathcoverage_stratified.tsv.gz)

SET VARIABLE pathcoverage_stratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        coverage := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathcoverage_stratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathcoverage_stratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathcoverage_stratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathcoverage_stratified_scan',
    rejects_table='pathcoverage_stratified_errors');

CREATE OR REPLACE TABLE pathcoverage_stratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathcoverage_stratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathcoverage_stratified_joined AS SELECT
    string_split_regex(t.pathway, '[|.]')[1] AS pathway_uniref,
    string_split_regex(t.pathway, '[|.]')[2] AS pathway_genus,
    string_split_regex(t.pathway, '[|.]')[3] AS pathway_species,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathcoverage_stratified AS t
INNER JOIN pathcoverage_stratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathcoverage_stratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathcoverage_stratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_stratified_joined ORDER BY pathway_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_stratified_pathway_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathcoverage_stratified;
DROP TABLE pathcoverage_stratified_scan;
DROP TABLE pathcoverage_stratified_errors;
DROP TABLE pathcoverage_stratified_headers;
DROP TABLE pathcoverage_stratified_joined;

-- HUMAnN: pathcoverage_unstratified (out_pathcoverage_unstratified.tsv.gz)

SET VARIABLE pathcoverage_unstratified_columns =
    struct_pack(
        pathway := 'VARCHAR',
        coverage := 'DOUBLE'
    );

CREATE OR REPLACE TABLE pathcoverage_unstratified AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/humann/out_pathcoverage_unstratified.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('pathcoverage_unstratified_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='pathcoverage_unstratified_scan',
    rejects_table='pathcoverage_unstratified_errors');

CREATE OR REPLACE TABLE pathcoverage_unstratified_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM pathcoverage_unstratified_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        humann_header := split_part(full_header, '|', 1)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE pathcoverage_unstratified_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM pathcoverage_unstratified AS t
INNER JOIN pathcoverage_unstratified_scan AS s ON t.filename = s.file_path
INNER JOIN pathcoverage_unstratified_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM pathcoverage_unstratified_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/pathcoverage_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathcoverage_unstratified;
DROP TABLE pathcoverage_unstratified_scan;
DROP TABLE pathcoverage_unstratified_errors;
DROP TABLE pathcoverage_unstratified_headers;
DROP TABLE pathcoverage_unstratified_joined;

