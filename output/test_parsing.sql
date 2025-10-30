-- Usage: duckdb goose.duckdb < test_parsing.sql
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
        '5f8d4254-7653-46e3-814e-ed72cdfcb4d0',
        '0a73759e-825f-4276-9348-66fb6a6e2f86',
        '8793b1dc-3ba1-4591-82b8-4297adcfa1d7',
        '6821bf5f-ad59-4204-9d78-9cf9cac97329',
        '8eb9f7ae-88c2-44e5-967e-fe7f6090c7af',
        'cc1f30a0-45d9-41b1-b592-7d0892919ee7',
        '4985aa08-6138-4146-8ae3-952716575395',
        'fb7e8210-002a-4554-b265-873c4003e25f');
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
        chocophlan_version := split_part(full_header, '|', 1),
        command := split_part(full_header, '|', 2),
        number_reads := split_part(full_header, '|', 3),
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

UPDATE relative_abundance_joined
SET
    chocophlan_version = regexp_extract(chocophlan_version, '[^#]+'),
    command = regexp_extract(command, '[^#]+'),
    number_reads = regexp_extract(number_reads, '\d+'),
    metaphlan_header = regexp_extract(metaphlan_header, '[^#]+'),
    original_columns = regexp_extract(original_columns, '[^#]+');

ALTER TABLE relative_abundance_joined ALTER COLUMN number_reads TYPE INTEGER;

ALTER TABLE relative_abundance_joined
ADD
    feature_key VARCHAR;

UPDATE relative_abundance_joined
SET
    feature_key = md5(concat(
        clade_name,
        NCBI_tax_id,
        additional_species));

CREATE OR REPLACE TABLE relative_abundance_samples AS SELECT DISTINCT
    uuid,
    chocophlan_version,
    command,
    number_reads,
    metaphlan_header,
    original_columns
FROM relative_abundance_joined
ORDER BY uuid;

CREATE OR REPLACE TABLE relative_abundance_features AS SELECT DISTINCT
    feature_key,
    clade_name,
    clade_name_kingdom,
    clade_name_phylum,
    clade_name_class,
    clade_name_order,
    clade_name_family,
    clade_name_genus,
    clade_name_species,
    clade_name_terminal,
    NCBI_tax_id,
    NCBI_tax_id_kingdom,
    NCBI_tax_id_phylum,
    NCBI_tax_id_class,
    NCBI_tax_id_order,
    NCBI_tax_id_family,
    NCBI_tax_id_genus,
    NCBI_tax_id_species,
    NCBI_tax_id_terminal,
    additional_species
FROM relative_abundance_joined
ORDER BY clade_name;

CREATE OR REPLACE TABLE relative_abundance_data AS SELECT
    uuid,
    feature_key,
    relative_abundance
FROM relative_abundance_joined;

COPY
    (SELECT * FROM relative_abundance_data ORDER BY uuid ASC)
TO '/home/kaelyn.long/cMD_pipeline/output_handling/parquets/relative_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM relative_abundance_data ORDER BY feature_key ASC)
TO '/home/kaelyn.long/cMD_pipeline/output_handling/parquets/relative_abundance_feature_key.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM relative_abundance_samples
    UNION
    SELECT * FROM '/home/kaelyn.long/cMD_pipeline/output_handling/parquets/relative_abundance_sample_ref.parquet')
TO '/home/kaelyn.long/cMD_pipeline/output_handling/parquets/relative_abundance_sample_ref.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM relative_abundance_features
    UNION
    SELECT * FROM '/home/kaelyn.long/cMD_pipeline/output_handling/parquets/relative_abundance_feature_ref.parquet')
TO '/home/kaelyn.long/cMD_pipeline/output_handling/parquets/relative_abundance_feature_ref.parquet'
    (format parquet, compression 'zstd');

DROP TABLE relative_abundance;
DROP TABLE relative_abundance_scan;
DROP TABLE relative_abundance_errors;
DROP TABLE relative_abundance_headers;
DROP TABLE relative_abundance_joined;
DROP TABLE relative_abundance_data;
DROP TABLE relative_abundance_samples;
DROP TABLE relative_abundance_features;
