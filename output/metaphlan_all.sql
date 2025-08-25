-- Usage: duckdb goose.duckdb < metaphlan_all.sql
install httpfs;
load httpfs;
CREATE SECRET metagenomics_mac (
    TYPE gcs,
    KEY_ID 'KEY_ID',
    SECRET 'SECRET'
);

-- Wildcard mode: read all matching files
SET VARIABLE test_prefixes = list_value('gs://metagenomics-mac/results/cMDv4/*');

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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/relative_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM relative_abundance_joined ORDER BY clade_name_species ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/relative_abundance_clade_name_species.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/viral_clusters_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM viral_clusters_joined ORDER BY genome_name ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/viral_clusters_genome_name.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_abundance_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM marker_abundance_joined ORDER BY uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_abundance_uniref.parquet'
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
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_presence_uuid.parquet'
    (format parquet, compression 'zstd');

COPY
    (SELECT * FROM marker_presence_joined ORDER BY uniref ASC)
TO '/shares/CIBIO-Storage/CM/scratch/users/kaelyn.long/retrieve/parquets/metagenomics_mac/marker_presence_uniref.parquet'
    (format parquet, compression 'zstd');


DROP TABLE marker_presence;
DROP TABLE marker_presence_scan;
DROP TABLE marker_presence_errors;
DROP TABLE marker_presence_headers;
DROP TABLE marker_presence_joined;

