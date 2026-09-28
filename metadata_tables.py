# Databricks notebook source
# MAGIC %md
# MAGIC _1. **metadata_source_details** :- This table contain the information about the source and data given by the source eg:- Location,Credential,Extension,Cadence etc_

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE apnimed_dw_dev.metadata.metadata_source_details (
# MAGIC     source_id             BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     source_name           STRING NOT NULL,
# MAGIC     vendor_name           STRING,
# MAGIC     transmission_method   STRING,
# MAGIC     load_type             STRING,
# MAGIC     sftp_host             STRING,
# MAGIC     sftp_port             STRING,
# MAGIC     sftp_username         STRING,
# MAGIC     sftp_password         STRING,
# MAGIC     remote_dir            STRING,
# MAGIC     adls_dir              STRING,
# MAGIC     file_compressed       BOOLEAN,
# MAGIC     compression_type      STRING,
# MAGIC     sheet_name            STRING,
# MAGIC     skip_row              INT,
# MAGIC     skip_column           INT,
# MAGIC     header                BOOLEAN,
# MAGIC     delimiter             STRING,
# MAGIC     db_volume             STRING,
# MAGIC     cadence_days          INT,
# MAGIC     last_ingested_at      TIMESTAMP,
# MAGIC     is_active             BOOLEAN NOT NULL DEFAULT TRUE,
# MAGIC     created_at            TIMESTAMP NOT NULL DEFAULT current_timestamp()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO apnimed_dw_dev.metadata.metadata_source_details (
# MAGIC     source_name,
# MAGIC     vendor_name,
# MAGIC     transmission_method,
# MAGIC     load_type,
# MAGIC     sftp_host,
# MAGIC     sftp_port,
# MAGIC     sftp_username,
# MAGIC     sftp_password,
# MAGIC     remote_dir,
# MAGIC     adls_dir,
# MAGIC     file_compressed,
# MAGIC     compression_type,
# MAGIC     sheet_name,
# MAGIC     skip_row,
# MAGIC     skip_column,
# MAGIC     header,
# MAGIC     delimiter,
# MAGIC     db_volume,
# MAGIC     cadence_days
# MAGIC )
# MAGIC VALUES (
# MAGIC     'iqvia_onekey',
# MAGIC     'iqvia',
# MAGIC     'sftp',
# MAGIC     'overwrite',
# MAGIC     'iqvia_onekey_host',
# MAGIC     'iqvia_onekey_port',
# MAGIC     'iqvia_onekey_sftp_username',
# MAGIC     'iqvia_onekey_sftp_password',
# MAGIC     'inbound/iqvia_inbound/onekey_inbound',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     'zip',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     '|',
# MAGIC     '/Volumes/apnimed_dw_dev/bronze/iqvia-file-container/from_vendor/onekey',
# MAGIC     180
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC _2. **metadata_source_file_to_table_mapping** :- This table contains the folder/file pattern of source files and the name of the tables these files will create._

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE apnimed_dw_dev.metadata.metadata_sourcefile_to_table_mapping (
# MAGIC     mapping_id            BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     source_name           STRING NOT NULL,
# MAGIC     folder_file_pattern   STRING NOT NULL,   -- matches EITHER a delivery folder name, or one/many flat file names, depending on how this source delivers data
# MAGIC     table_name            STRING NOT NULL,   -- bare canonical name only — lndg_/stg_ prefixes are derived in code, never stored
# MAGIC     is_active             BOOLEAN NOT NULL DEFAULT TRUE,
# MAGIC     created_at            TIMESTAMP NOT NULL DEFAULT current_timestamp()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO apnimed_dw_dev.metadata.metadata_sourcefile_to_table_mapping
# MAGIC (source_name, folder_file_pattern, table_name)
# MAGIC VALUES
# MAGIC ('iqvia_onekey', '^OK\.HCE_DETL_MAST\.\d{8}\.txt$', 'hce_detl_mast'),
# MAGIC ('iqvia_onekey', '^OK\.HCE_DETL_MAST_XREF\.\d{8}\.txt$', 'hce_detl_mast_xref'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.BED_FACT\.\d{8}\.txt$', 'hco_bed_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.BUS_DETL_FACT\.\d{8}\.txt$', 'hco_bus_detl_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.FNCL_FACT\.\d{8}\.txt$', 'hco_fncl_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.ISN_FACT\.\d{8}\.txt$', 'hco_isn_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.ORG_ACCS_CNTL\.\d{8}\.txt$', 'hco_org_accs_cntl'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.ORG_ALT_ADDR\.\d{8}\.txt$', 'hco_org_alt_addr'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.ORG_DETL_MAST\.\d{8}\.txt$', 'hco_org_detl_mast'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.ORG_HRS\.\d{8}\.txt$', 'hco_org_hrs'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.PRFSNL_AFFL\.\d{8}\.txt$', 'hco_prfsnl_affl'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.PRFSNL_CTCT_AFFL\.\d{8}\.txt$', 'hco_prfsnl_ctct_affl'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.REF_AFFL_TYP\.\d{8}\.txt$', 'hco_ref_affl_typ'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.REF_BUS_DETL\.\d{8}\.txt$', 'hco_ref_bus_detl'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.REF_BUS_DETL_DMN\.\d{8}\.txt$', 'hco_ref_bus_detl_dmn'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.REF_COT_TYP\.\d{8}\.txt$', 'hco_ref_cot_typ'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.REF_RLTN_TYP\.\d{8}\.txt$', 'hco_ref_rltn_typ'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.REF_TITL_TYP\.\d{8}\.txt$', 'hco_ref_titl_typ'),
# MAGIC ('iqvia_onekey', '^OK\.HCO\.RLTN_FACT\.\d{8}\.txt$', 'hco_rltn_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.ALTNAME\.\d{8}\.txt$', 'hcp_altname'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.ISN_ADDR\.\d{8}\.txt$', 'hcp_isn_addr'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.ISN_FACT\.\d{8}\.txt$', 'hcp_isn_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PRFSNL_ADDR\.\d{8}\.txt$', 'hcp_prfsnl_addr'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PRFSNL_COM\.\d{8}\.txt$', 'hcp_prfsnl_com'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PRFSNL_DETL_FACT\.\d{8}\.txt$', 'hcp_prfsnl_detl_fact'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PRFSNL_DETL_MAST\.\d{8}\.txt$', 'hcp_prfsnl_detl_mast'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PRFSNL_PGM_PRTP\.\d{8}\.txt$', 'hcp_prfsnl_pgm_prtp'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PRFSNL_RSTR\.\d{8}\.txt$', 'hcp_prfsnl_rstr'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.PROFESSION\.\d{8}\.txt$', 'hcp_profession'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.REF_PRFSNL_DETL\.\d{8}\.txt$', 'hcp_ref_prfsnl_detl'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.REF_PRFSNL_DETL_DMN\.\d{8}\.txt$', 'hcp_ref_prfsnl_detl_dmn'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.REF_PRFSNL_PRFSN\.\d{8}\.txt$', 'hcp_ref_prfsnl_prfsn'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.REF_PRFSNL_SPCL\.\d{8}\.txt$', 'hcp_ref_prfsnl_spcl'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.REF_SLN_KEY\.\d{8}\.txt$', 'hcp_ref_sln_key'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.REF_VALID_SPCL_PRFSN\.\d{8}\.txt$', 'hcp_ref_valid_spcl_prfsn'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.SLN_CSR_ADDRESS\.\d{8}\.txt$', 'hcp_sln_csr_address'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.SLN_CSR_LIC\.\d{8}\.txt$', 'hcp_sln_csr_lic'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.SLN_CSR_REGULATORY\.\d{8}\.txt$', 'hcp_sln_csr_regulatory'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.SPECIALTY\.\d{8}\.txt$', 'hcp_specialty'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.SRC_DETL\.\d{8}\.txt$', 'hcp_src_detl'),
# MAGIC ('iqvia_onekey', '^OK\.HCP\.SUBSPECIALTY\.\d{8}\.txt$', 'hcp_subspecialty'),
# MAGIC ('iqvia_onekey', '^OK\.Master_Addr\.\d{8}\.txt$', 'master_addr'),
# MAGIC ('iqvia_onekey', '^OK\.RECORD_COUNTS\.\d{8}\.txt$', 'record_counts'),
# MAGIC ('iqvia_onekey', '^OK\.REF_HCE_TYP\.\d{8}\.txt$', 'ref_hce_typ'),
# MAGIC ('iqvia_onekey', '^OK\.REF_ISN_CATG_TYP\.\d{8}\.txt$', 'ref_isn_catg_typ'),
# MAGIC ('iqvia_onekey', '^OK\.REF_ISN_TRMTN_TYP\.\d{8}\.txt$', 'ref_isn_trmtn_typ'),
# MAGIC ('iqvia_onekey', '^OK\.REF_ISN_TYP\.\d{8}\.txt$', 'ref_isn_typ'),
# MAGIC ('iqvia_onekey', '^OK\.REF_MDATA_CLMN\.\d{8}\.txt$', 'ref_mdata_clmn'),
# MAGIC ('iqvia_onekey', '^OK\.REF_MDATA_DMN\.\d{8}\.txt$', 'ref_mdata_dmn'),
# MAGIC ('iqvia_onekey', '^OK\.REF_MDATA_DMN_VAL\.\d{8}\.txt$', 'ref_mdata_dmn_val'),
# MAGIC ('iqvia_onekey', '^OK\.REF_MDATA_REF\.\d{8}\.txt$', 'ref_mdata_ref'),
# MAGIC ('iqvia_onekey', '^OK\.REF_MDATA_TBLS\.\d{8}\.txt$', 'ref_mdata_tbls');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO apnimed_dw_dev.metadata.metadata_sourcefile_to_table_mapping
# MAGIC (source_name, folder_file_pattern, table_name)
# MAGIC VALUES
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_CNT_M\d{8}$', 'osa_count_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_COMM_M\d{8}$', 'osa_patient_high_deductible_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_DXCD_M\d{8}$', 'osa_diagnosis_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_FACT_M\d{8}$', 'osa_pharmacy_fact'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_FCLT_M\d{8}$', 'osa_facility_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_GEO_M\d{8}$', 'osa_geography_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_MPD_M\d{8}$', 'osa_patient_medicare_part_d_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_MXPT_M\d{8}$', 'osa_patient_mdc_activity_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_MX_M\d{8}$', 'osa_medical_fact'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_PATD_M\d{8}$', 'osa_patient_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_PLN_M\d{8}$', 'osa_plan_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_PRD_M\d{8}$', 'osa_product_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_PVR_M\d{8}$', 'osa_provider_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_PXCD_M\d{8}$', 'osa_procedure_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_REJ_M\d{8}$', 'osa_reject_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_RXPT_M\d{8}$', 'osa_patient_phc_activity_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_RXSUM_M\d{8}$', 'osa_phc_summary_dim'),
# MAGIC ('iqvia_laad', '^LAD_568345_LM33398_SVC_M\d{8}$', 'osa_service_dim');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE apnimed_dw_dev.metadata.metadata_sourcefile_to_table_mapping
# MAGIC SET folder_file_pattern = 'call2_discussion__v', table_name = 'call2_discussion__v' 
# MAGIC WHERE source_name = 'veeva_crm' AND table_name = 'call2discussion__v';

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE apnimed_dw_dev.metadata.metadata_source_details_1
# MAGIC SET last_ingested_at = NULL
# MAGIC WHERE source_name = 'veeva_crm'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM apnimed_dw_dev.metadata.metadata_sourcefile_to_table_mapping

# COMMAND ----------

# MAGIC %md
# MAGIC _3. **metadata_file_download_log**:- Tracks each file download attempt (status, timing, errors) for auditing ingestion runs._

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE apnimed_dw_dev.metadata.metadata_file_download_log (
# MAGIC     log_id           BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     source_name      STRING NOT NULL,
# MAGIC     table_name       STRING NOT NULL,
# MAGIC     file_name        STRING NOT NULL,
# MAGIC     remote_path      STRING NOT NULL,
# MAGIC     local_path       STRING NOT NULL,
# MAGIC     file_size_bytes  BIGINT,
# MAGIC     status           STRING NOT NULL,   -- STARTED / SUCCESS / FAILED
# MAGIC     error_message    STRING,
# MAGIC     started_at       TIMESTAMP NOT NULL,
# MAGIC     completed_at     TIMESTAMP,
# MAGIC     run_id           STRING,            
# MAGIC     created_at       TIMESTAMP NOT NULL DEFAULT current_timestamp()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %md
# MAGIC _4. **metadata_source_item_fingerprint** :- Tracks the last known file fingerprint per table, used to skip re-downloading unchanged data in case of SFTP and ADLS. In case of API that does not give metadata filename and size it will save downwared processing._

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE apnimed_dw_dev.metadata.metadata_source_item_fingerprint (
# MAGIC     fingerprint_id     BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     source_name        STRING NOT NULL,
# MAGIC     table_name         STRING NOT NULL,
# MAGIC     fingerprint        STRING NOT NULL,   -- MD5 of sorted "filename:size" pairs for everything matched to this table this run
# MAGIC     last_pulled_at     TIMESTAMP NOT NULL,
# MAGIC     created_at         TIMESTAMP NOT NULL DEFAULT current_timestamp(),
# MAGIC     updated_at         TIMESTAMP NOT NULL DEFAULT current_timestamp()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %md
# MAGIC _5. **metadata_table_column_mapping** :- Defines the column-level mapping and casting rules — source column, target column, and target data type — for each source table, driving how landing data gets typed into staging._

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE apnimed_dw_dev.metadata.metadata_table_column_mapping (
# MAGIC     column_config_id      BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     source_name           STRING NOT NULL,
# MAGIC     interface_name        STRING NOT NULL,  -- raw source name, e.g. LAAD_M33398_OSA_COUNT_DIM
# MAGIC     table_name            STRING NOT NULL, -- bare canonical name, e.g. osa_count_dim — joins to metadata_sourcefile_to_table_mapping.table_name
# MAGIC     seq_id                INT,
# MAGIC     source_column_name    STRING NOT NULL,
# MAGIC     target_column_name    STRING NOT NULL,
# MAGIC     source_data_type      STRING,
# MAGIC     column_length         STRING,   -- kept as STRING since source can hold 'N/A'
# MAGIC     column_precision      STRING,
# MAGIC     is_primary_key        BOOLEAN,
# MAGIC     is_active             BOOLEAN NOT NULL DEFAULT TRUE,
# MAGIC     created_at            TIMESTAMP NOT NULL DEFAULT current_timestamp()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

catalog = "apnimed_dw_dev"
metadata_schema = "metadata"
column_mapping_table = f"{catalog}.{metadata_schema}.metadata_table_column_mapping"


def derive_table_name(interface_name, source_name):
    """Bare canonical name only — matches metadata_sourcefile_to_table_mapping's
    own convention. lndg_/stg_ prefixes are derived in code, never stored."""
    if source_name == "iqvia_laad":
        return interface_name.replace("LAAD_M33398_", "", 1).lower()
    else:
        normalized = interface_name.replace(".", "_")
        stripped = normalized[3:] if normalized.startswith("OK_") else normalized
        return stripped.lower()


def load_config_json(path, source_name):
    with open(path) as f:
        config = json.load(f)

    rows = []
    for interface_name, iface in config["interfaces"].items():
        table_name = derive_table_name(interface_name, source_name)
        primary_keys = set(iface.get("primary_keys", []))

        for col_id, col in iface["columns"].items():
            target_col = col["final_column_name"]
            rows.append((
                source_name,
                interface_name,
                table_name,
                int(col_id),
                col.get("header_name", col.get("column_name")),
                target_col,
                col["data_type"],
                str(col.get("column_length")),
                str(col.get("column_precision")),
                target_col in primary_keys,
                True,
            ))
    return rows


all_rows = []
all_rows += load_config_json("/Volumes/apnimed_dw_dev/metadata/iqvia/laad/table_config.json", "iqvia_laad")
all_rows += load_config_json("/Volumes/apnimed_dw_dev/metadata/iqvia/onekey/table_config.json", "iqvia_onekey")

schema = StructType([
    StructField("source_name", StringType(), nullable=False),
    StructField("interface_name", StringType(), nullable=False),
    StructField("table_name", StringType(), nullable=False),
    StructField("seq_id", IntegerType(), nullable=True),
    StructField("source_column_name", StringType(), nullable=False),
    StructField("target_column_name", StringType(), nullable=False),
    StructField("source_data_type", StringType(), nullable=True),
    StructField("column_length", StringType(), nullable=True),
    StructField("column_precision", StringType(), nullable=True),
    StructField("is_primary_key", BooleanType(), nullable=True),
    StructField("is_active", BooleanType(), nullable=False),
])

df = spark.createDataFrame(all_rows, schema=schema)
df.write.mode("overwrite").saveAsTable(column_mapping_table)

print(f"Loaded {df.count()} column definitions into {column_mapping_table}")

# Sanity check — confirm table counts match expectations
distinct_tables = df.select("source_name", "interface_name").distinct().groupBy("source_name").count().collect()
for row in distinct_tables:
    print(f"  {row['source_name']}: {row['count']} distinct tables")