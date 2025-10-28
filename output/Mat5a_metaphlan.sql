-- Usage: duckdb goose.duckdb < Mat5_metaphlan.sql
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

-- MetaPhlAn: relative_abundance (metaphlan_unknown_list.tsv.gz)

SET VARIABLE relative_abundance_columns =
    struct_pack(
        clade_name := 'VARCHAR',
        NCBI_tax_id := 'VARCHAR',
        relative_abundance := 'DOUBLE',
        additional_species := 'VARCHAR'
    );

CREATE OR REPLACE TABLE relative_abundance AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/metaphlan_lists/metaphlan_unknown_list.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('relative_abundance_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='relative_abundance_scan',
    rejects_table='relative_abundance_errors');

CREATE OR REPLACE TABLE relative_abundance_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM relative_abundance_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        db_version := split_part(full_header, '|', 1),
        command := split_part(full_header, '|', 2),
        reads_processed := split_part(full_header, '|', 3),
        metaphlan_header := split_part(full_header, '|', 4),
        original_columns := split_part(full_header, '|', 5)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE relative_abundance_joined AS SELECT
    string_split_regex(t.clade_name, '[|]')[1] AS clade_name_kingdom,
    string_split_regex(t.clade_name, '[|]')[2] AS clade_name_phylum,
    string_split_regex(t.clade_name, '[|]')[3] AS clade_name_class,
    string_split_regex(t.clade_name, '[|]')[4] AS clade_name_order,
    string_split_regex(t.clade_name, '[|]')[5] AS clade_name_family,
    string_split_regex(t.clade_name, '[|]')[6] AS clade_name_genus,
    string_split_regex(t.clade_name, '[|]')[7] AS clade_name_species,
    string_split_regex(t.clade_name, '[|]')[8] AS clade_name_terminal,
    string_split_regex(t.NCBI_tax_id, '[|]')[1] AS NCBI_tax_id_kingdom,
    string_split_regex(t.NCBI_tax_id, '[|]')[2] AS NCBI_tax_id_phylum,
    string_split_regex(t.NCBI_tax_id, '[|]')[3] AS NCBI_tax_id_class,
    string_split_regex(t.NCBI_tax_id, '[|]')[4] AS NCBI_tax_id_order,
    string_split_regex(t.NCBI_tax_id, '[|]')[5] AS NCBI_tax_id_family,
    string_split_regex(t.NCBI_tax_id, '[|]')[6] AS NCBI_tax_id_genus,
    string_split_regex(t.NCBI_tax_id, '[|]')[7] AS NCBI_tax_id_species,
    string_split_regex(t.NCBI_tax_id, '[|]')[8] AS NCBI_tax_id_terminal,
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM relative_abundance AS t
INNER JOIN relative_abundance_scan AS s ON t.filename = s.file_path
INNER JOIN relative_abundance_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM relative_abundance_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/relative_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM relative_abundance_joined ORDER BY clade_name_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/relative_abundance_clade_name_species.parquet'
    (format parquet, compression 'zstd');


DROP TABLE relative_abundance;
DROP TABLE relative_abundance_scan;
DROP TABLE relative_abundance_errors;
DROP TABLE relative_abundance_headers;
DROP TABLE relative_abundance_joined;

-- MetaPhlAn: viral_clusters (metaphlan_viruses_list.tsv.gz)

SET VARIABLE viral_clusters_columns =
    struct_pack(
        m_group_cluster := 'VARCHAR',
        genome_name := 'VARCHAR',
        length := 'INTEGER',
        breadth_of_coverage := 'DOUBLE',
        depth_of_coverage_mean := 'DOUBLE',
        depth_of_coverage_median := 'DOUBLE',
        m_group_type_k_u := 'VARCHAR',
        first_genome_in_cluster := 'VARCHAR',
        other_genomes := 'VARCHAR'
    );

CREATE OR REPLACE TABLE viral_clusters AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/metaphlan_lists/metaphlan_viruses_list.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('viral_clusters_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='viral_clusters_scan',
    rejects_table='viral_clusters_errors');

CREATE OR REPLACE TABLE viral_clusters_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM viral_clusters_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        db_version := split_part(full_header, '|', 1),
        command := split_part(full_header, '|', 2),
        metaphlan_header := split_part(full_header, '|', 3),
        original_columns := split_part(full_header, '|', 4)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE viral_clusters_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM viral_clusters AS t
INNER JOIN viral_clusters_scan AS s ON t.filename = s.file_path
INNER JOIN viral_clusters_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM viral_clusters_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/viral_clusters_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM viral_clusters_joined ORDER BY genome_name ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/viral_clusters_genome_name.parquet'
    (format parquet, compression 'zstd');


DROP TABLE viral_clusters;
DROP TABLE viral_clusters_scan;
DROP TABLE viral_clusters_errors;
DROP TABLE viral_clusters_headers;
DROP TABLE viral_clusters_joined;

-- MetaPhlAn: marker_abundance (marker_abundance.tsv.gz)

SET VARIABLE marker_abundance_columns =
    struct_pack(
        uniref := 'VARCHAR',
        abundance := 'DOUBLE'
    );

CREATE OR REPLACE TABLE marker_abundance AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/metaphlan_markers/marker_abundance.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('marker_abundance_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='marker_abundance_scan',
    rejects_table='marker_abundance_errors');

CREATE OR REPLACE TABLE marker_abundance_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM marker_abundance_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        db_version := split_part(full_header, '|', 1),
        command := split_part(full_header, '|', 2),
        reads_processed := split_part(full_header, '|', 3),
        metaphlan_header := split_part(full_header, '|', 4)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE marker_abundance_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM marker_abundance AS t
INNER JOIN marker_abundance_scan AS s ON t.filename = s.file_path
INNER JOIN marker_abundance_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM marker_abundance_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/marker_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM marker_abundance_joined ORDER BY uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/marker_abundance_uniref.parquet'
    (format parquet, compression 'zstd');


DROP TABLE marker_abundance;
DROP TABLE marker_abundance_scan;
DROP TABLE marker_abundance_errors;
DROP TABLE marker_abundance_headers;
DROP TABLE marker_abundance_joined;

-- MetaPhlAn: marker_presence (marker_presence.tsv.gz)

SET VARIABLE marker_presence_columns =
    struct_pack(
        uniref := 'VARCHAR',
        presence := 'INTEGER'
    );

CREATE OR REPLACE TABLE marker_presence AS SELECT * FROM read_csv(
    list_transform(getvariable('test_prefixes'), lambda x : concat(x, '/metaphlan_markers/marker_presence.tsv.gz')),
    filename=True,
    auto_detect=False,
    columns=getvariable('marker_presence_columns'),
    delim='\t',
    skip=0,
    store_rejects=True,
    rejects_scan='marker_presence_scan',
    rejects_table='marker_presence_errors');

CREATE OR REPLACE TABLE marker_presence_headers AS
WITH deduped_lines AS (
    SELECT DISTINCT scan_id, file_id, line, csv_line
    FROM marker_presence_errors
)
SELECT
    scan_id,
    file_id,
    string_agg(csv_line, '|' ORDER BY line) AS full_header,
    struct_pack(
        db_version := split_part(full_header, '|', 1),
        command := split_part(full_header, '|', 2),
        reads_processed := split_part(full_header, '|', 3),
        metaphlan_header := split_part(full_header, '|', 4)
    ) AS nested_header
FROM deduped_lines
GROUP BY scan_id, file_id;

CREATE OR REPLACE TABLE marker_presence_joined AS SELECT
    t.* EXCLUDE (t.filename),
    split_part(t.filename, '/', 6) AS uuid,
    UNNEST(h.nested_header)
FROM marker_presence AS t
INNER JOIN marker_presence_scan AS s ON t.filename = s.file_path
INNER JOIN marker_presence_headers AS h ON s.file_id = h.file_id;

COPY
    (SELECT * FROM marker_presence_joined ORDER BY uuid ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/marker_presence_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM marker_presence_joined ORDER BY uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/new_samples/marker_presence_uniref.parquet'
    (format parquet, compression 'zstd');


DROP TABLE marker_presence;
DROP TABLE marker_presence_scan;
DROP TABLE marker_presence_errors;
DROP TABLE marker_presence_headers;
DROP TABLE marker_presence_joined;

