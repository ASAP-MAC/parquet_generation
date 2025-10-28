-- Usage: duckdb goose.duckdb < Mat5_humann.sql
install httpfs;
load httpfs;
CREATE SECRET metagenomics_mac (
    TYPE gcs,
    KEY_ID 'KEY_ID',
    SECRET 'SECRET'
);

PRAGMA temp_directory='/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/tmp_duckdb';
PRAGMA memory_limit='200GB';

SET VARIABLE sample_ids = list_value(
        '46eb4a06-5f1e-4d2d-adb4-0706a1e55022',
        'bd8bae56-2727-4375-9f7a-ccae511e80c3',
        'fba6b6f6-96c9-4311-8378-39768eb3c9df',
        '794e107b-afd1-4ac0-b783-4a9c93dcbf42',
        'd53cd95f-5228-4a82-8d89-a4ab5492f8c7',
        'e46cb9e6-9bbd-4940-ac99-10c4c569ec4a',
        '152da9a0-aa87-4ff1-b949-809049b4d9a8',
        'ad039b54-b0f6-4c32-a05b-1602376f225b',
        'ec3569ff-f48f-43e5-8296-3bfa7de98fe4',
        '77b0f00d-7bab-4ad9-a1db-da90c94d2e03',
        '93683ce9-7ab3-4e3e-9567-784a56e5e470',
        '805afc59-0812-4c0d-9707-ca6b2b5cf297',
        'a4afb74d-e5e0-44c2-9d35-017c73cc520c',
        'd4bd721d-7493-4988-a2c4-c8cfbb2ae2dc',
        'fbbee3c6-6c3e-4433-909a-3023267c7a86',
        '2fd57a92-7998-41bc-8719-7cea2c98593d',
        '97be28ae-1528-4d96-89ed-fbe1194930b2',
        '253ca89a-7674-4535-b32e-ef636a917d53',
        '0499f376-ecd2-430b-90a9-4b7a430056b8',
        '591d4f4a-e797-457c-ba1b-957addf98e6f',
        'e9ab3dd5-9445-43dd-afe0-dfa287b05c89',
        '8e896d35-988d-414f-bf3d-ff876f98bfe4',
        '9ee92513-af3a-40ee-b807-2f4f38dd7ccd',
        '7af2c0a1-701b-49f9-b971-c4d5d0be4dd2',
        'e72a025f-c920-4f24-bdef-eace6f900078',
        'bd49ae7c-c646-4faa-a333-a248b9df31f8',
        '6f17aaf0-6da8-4421-a903-30c4ecbfafd3',
        '99703085-9be7-4dda-9d14-61543909482b',
        '0eb77d2e-036e-4e3f-a9d0-ffb23d97be01',
        '8904f901-e376-4008-84fc-539c0f3dcf0a',
        '19acccb1-2116-402b-8b07-0ffa198771e7',
        '3ab22da4-eafe-4eb4-ab16-f072fdf432e3',
        '4eb25aec-1d81-4740-842c-313f98582ebb',
        '45ba615c-7e2d-483f-9d56-322c9820c6d9',
        '58296f8f-cc59-4703-bccd-f57afff7f0ad',
        '56e89e3f-7a54-4cb6-9efa-1a3e45e6221d',
        '28f0142a-0434-4a3b-8a01-9199672ad601',
        '528eb812-5899-45b0-ab47-c40efa22c7b7',
        'b38488f3-ad61-43a3-99d5-980815464046',
        '38b30301-744e-4dc8-af45-f381a3fda6a2',
        '78550dee-6b6c-4fd2-bf73-3f435cb913f8',
        '87095a40-28c5-4d20-a764-490e4a6f4fa0',
        '6d0fcec4-a829-49f3-9704-85a1aaab7d49',
        '3af7a5a2-40ce-4dbc-81f5-56746c891392',
        'ebe2a1e7-eb6f-4c1e-bfac-b2cd72730bd5',
        'ab9622fa-f701-4fb0-9704-ec46c071d8a7',
        '86057d76-c775-4500-89ea-aef25f7c4372',
        '4a82e087-a8bd-4d6f-aee7-b54cdb9011f6',
        '70f83e64-3fe2-4880-8a36-d19b634ee6ab',
        '3f3b14ee-348e-4ce4-bfce-9033564e2cc7');
SET VARIABLE test_prefixes = list_transform(getvariable('sample_ids'), lambda x : concat('gs://metagenomics-mac/results/cMDv4/', x));

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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_joined ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_gene_family_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_joined ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_gene_family_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_stratified_joined ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_stratified_gene_family_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_cpm_unstratified_joined ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_cpm_unstratified_gene_family.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_joined ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_gene_family_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_stratified_joined ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_stratified_gene_family_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_relab_unstratified_joined ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_relab_unstratified_gene_family.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_stratified_joined ORDER BY gene_family_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_stratified_gene_family_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM genefamilies_unstratified_joined ORDER BY gene_family ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/genefamilies_unstratified_gene_family.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_stratified_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_stratified_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_unstratified_pathway.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_stratified_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_stratified_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_stratified_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_stratified_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_cpm_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_cpm_unstratified_pathway.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathabundance_relab_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathabundance_relab_unstratified_pathway.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_stratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_stratified_joined ORDER BY pathway_uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_stratified_pathway_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_unstratified_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM pathcoverage_unstratified_joined ORDER BY pathway ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/pathcoverage_unstratified_pathway.parquet'
    (format parquet, compression 'zstd');


DROP TABLE pathcoverage_unstratified;
DROP TABLE pathcoverage_unstratified_scan;
DROP TABLE pathcoverage_unstratified_errors;
DROP TABLE pathcoverage_unstratified_headers;
DROP TABLE pathcoverage_unstratified_joined;

