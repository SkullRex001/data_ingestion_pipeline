# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

catalog = "apnimed_dw_dev"
bronze_schema = "bronze"
metadata_schema = "metadata"
column_config_table = f"{catalog}.{metadata_schema}.metadata_table_column_mapping"
source_table = f"{catalog}.{metadata_schema}.metadata_source_details"


def derive_landing_table_name(source_name, table_name):
    return f"lndg_{source_name}_{table_name.lower()}"


def derive_staging_table_name(source_name, table_name):
    return f"stg_{source_name}_{table_name.lower()}"


def build_cast_expr(source_col_ref, data_type, length, precision):
    dt = (data_type or "").strip()

    if dt == "VARCHAR":
        return f"CAST({source_col_ref} AS STRING)"
    elif dt == "NUMBER":
        length_ok = length not in (None, "N/A", "", "None")
        precision_ok = precision not in (None, "N/A", "", "None", "0")
        if length_ok and precision_ok:
            return f"CAST({source_col_ref} AS DECIMAL({length},{precision}))"
        elif length_ok:
            return f"CAST({source_col_ref} AS DECIMAL({length},0))"
        else:
            return f"CAST({source_col_ref} AS DECIMAL(38,0))"
    elif dt == "Big Integer":
        return f"CAST({source_col_ref} AS BIGINT)"
    elif dt == "Decimal":
        return f"CAST({source_col_ref} AS DECIMAL(38,4))"
    elif dt.startswith("Date#"):
        fmt = dt.split("#", 1)[1]
        return f"TO_DATE({source_col_ref}, '{fmt}')"
    elif dt == "DateTime":
        return f"TO_TIMESTAMP({source_col_ref})"
    else:
        return f"CAST({source_col_ref} AS STRING)"


def get_load_type_by_source():
    rows = spark.table(source_table).select("source_name", "load_type").collect()
    return {r["source_name"]: (r["load_type"] or "overwrite").strip().lower() for r in rows}


def write_staging(df_staged, full_staging, load_type, primary_keys):
    table_exists = spark.catalog.tableExists(full_staging)
    if not table_exists:
        df_staged.write.saveAsTable(full_staging)
        return "created"

    if load_type == "overwrite":
        spark.sql(f"TRUNCATE TABLE {full_staging}")
        df_staged.write.mode("append").saveAsTable(full_staging)
        return "truncated + loaded"
    elif load_type == "append":
        df_staged.write.mode("append").saveAsTable(full_staging)
        return "appended"
    elif load_type == "delta":
        if not primary_keys:
            raise ValueError("load_type is 'delta' but no primary key columns are defined")
        merge_condition = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in primary_keys])
        delta_table = DeltaTable.forName(spark, full_staging)
        (delta_table.alias("target").merge(df_staged.alias("source"), merge_condition)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        return f"merged on [{', '.join(primary_keys)}]"
    else:
        raise ValueError(f"Unrecognized load_type: '{load_type}'")


def process_staging_table(source_name, table_name, column_rows, load_type):
    landing_table_name = derive_landing_table_name(source_name, table_name)
    staging_table_name = derive_staging_table_name(source_name, table_name)
    full_landing = f"{catalog}.{bronze_schema}.{landing_table_name}"
    full_staging = f"{catalog}.{bronze_schema}.{staging_table_name}"

    if not spark.catalog.tableExists(full_landing):
        return ("SKIPPED", full_landing, full_staging, "Landing table does not exist")

    try:
        df = spark.table(full_landing)
        actual_cols = {c.lower(): c for c in df.columns}

        select_exprs = []
        missing_cols = []
        primary_keys = []

        for row in sorted(column_rows, key=lambda r: r["seq_id"] or 0):
            source_col = row["source_column_name"]
            target_col = row["target_column_name"]
            actual_col = actual_cols.get(source_col.lower())

            if actual_col is None:
                missing_cols.append(source_col)
                cast_expr = build_cast_expr("NULL", row["source_data_type"], row["column_length"], row["column_precision"])
            else:
                cast_expr = build_cast_expr(f"`{actual_col}`", row["source_data_type"], row["column_length"], row["column_precision"])

            select_exprs.append(F.expr(cast_expr).alias(target_col))
            if row["is_primary_key"]:
                primary_keys.append(target_col)

        for meta_col in ["run_id", "source_file", "is_active"]:
            if meta_col in df.columns:
                select_exprs.append(F.col(meta_col))

        select_exprs.append(F.current_timestamp().alias("created_at"))
        select_exprs.append(F.current_timestamp().alias("updated_at"))
        select_exprs.append(F.lit(f"{source_name.upper()}_STAGING_LOAD").alias("created_by"))

        df_staged = df.select(*select_exprs)
        row_count = df_staged.count()

        write_result = write_staging(df_staged, full_staging, load_type, primary_keys)

        status = "SUCCESS" if not missing_cols else "SUCCESS_WITH_GAPS"
        detail = f"{row_count} rows, {write_result}"
        if missing_cols:
            detail += f" | missing columns filled as NULL: {missing_cols}"
        return (status, full_landing, full_staging, detail)

    except Exception as e:
        # Full error, not just the first line — a truncated message
        # already cost real debugging time earlier on the SFTP side.
        return ("FAILED", full_landing, full_staging, str(e))


load_type_by_source = get_load_type_by_source()

config_df = spark.table(column_config_table).filter(F.col("is_active") == True)
config_rows = [row.asDict() for row in config_df.collect()]

tables_grouped = {}
for row in config_rows:
    key = (row["source_name"], row["table_name"])
    tables_grouped.setdefault(key, []).append(row)

results = []
for (source_name, table_name), column_rows in tables_grouped.items():
    load_type = load_type_by_source.get(source_name, "overwrite")
    status, full_landing, full_staging, detail = process_staging_table(source_name, table_name, column_rows, load_type)
    print(f"{status:<20} | [{load_type}] {full_staging} | {detail}")
    results.append((source_name, table_name, load_type, status, detail))

succeeded = [r for r in results if r[3] in ("SUCCESS", "SUCCESS_WITH_GAPS")]
skipped = [r for r in results if r[3] == "SKIPPED"]
failed = [r for r in results if r[3] == "FAILED"]

print("\n" + "=" * 70)
print("STAGING LAYER RUN SUMMARY")
print("=" * 70)
print(f"Total tables in config     : {len(results)}")
print(f"Succeeded                    : {len(succeeded)}")
print(f"Skipped (landing missing)     : {len(skipped)}")
print(f"Failed                          : {len(failed)}")

if skipped:
    print("\nSkipped — no landing table found:")
    for source_name, table_name, load_type, _, _ in skipped:
        print(f"  - [{source_name}] {table_name}")

if failed:
    print("\nFailed — needs attention:")
    for source_name, table_name, load_type, _, detail in failed:
        print(f"  - [{source_name}] {table_name}: {detail}")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

catalog = "apnimed_dw_dev"
bronze_schema = "bronze"
metadata_schema = "metadata"
column_config_table = f"{catalog}.{metadata_schema}.metadata_table_column_mapping"
source_table = f"{catalog}.{metadata_schema}.metadata_source_details"
watermark_table = f"{catalog}.{metadata_schema}.metadata_staging_watermark"


def derive_landing_table_name(source_name, table_name):
    return f"lndg_{source_name}_{table_name.lower()}"


def derive_staging_table_name(source_name, table_name):
    return f"stg_{source_name}_{table_name.lower()}"


def build_cast_expr(source_col_ref, data_type, length, precision):
    dt = (data_type or "").strip()

    if dt == "VARCHAR":
        return f"CAST({source_col_ref} AS STRING)"
    elif dt == "NUMBER":
        length_ok = length not in (None, "N/A", "", "None")
        precision_ok = precision not in (None, "N/A", "", "None", "0")
        if length_ok and precision_ok:
            return f"CAST({source_col_ref} AS DECIMAL({length},{precision}))"
        elif length_ok:
            return f"CAST({source_col_ref} AS DECIMAL({length},0))"
        else:
            return f"CAST({source_col_ref} AS DECIMAL(38,0))"
    elif dt == "Big Integer":
        return f"CAST({source_col_ref} AS BIGINT)"
    elif dt == "Decimal":
        return f"CAST({source_col_ref} AS DECIMAL(38,4))"
    elif dt.startswith("Date#"):
        fmt = dt.split("#", 1)[1]
        return f"TO_DATE({source_col_ref}, '{fmt}')"
    elif dt == "DateTime":
        return f"TO_TIMESTAMP({source_col_ref})"
    else:
        return f"CAST({source_col_ref} AS STRING)"


def get_load_type_by_source():
    rows = spark.table(source_table).select("source_name", "load_type").collect()
    return {r["source_name"]: (r["load_type"] or "overwrite").strip().lower() for r in rows}


# get the last run_id processed for this (source, table), or None if never processed
def get_last_watermark(source_name, table_name):
    rows = (
        spark.table(watermark_table)
        .filter((F.col("source_name") == source_name) & (F.col("table_name") == table_name))
        .collect()
    )
    return rows[0]["last_run_id"] if rows else None


# only called AFTER a successful staging write — moves the watermark forward
def update_watermark(source_name, table_name, new_run_id):
    exists = (
        spark.table(watermark_table)
        .filter((F.col("source_name") == source_name) & (F.col("table_name") == table_name))
        .count() > 0
    )
    if exists:
        spark.sql(f"""
            UPDATE {watermark_table}
            SET last_run_id = '{new_run_id}', updated_at = current_timestamp()
            WHERE source_name = '{source_name}' AND table_name = '{table_name}'
        """)
    else:
        spark.sql(f"""
            INSERT INTO {watermark_table} (source_name, table_name, last_run_id)
            VALUES ('{source_name}', '{table_name}', '{new_run_id}')
        """)


def write_staging(df_staged, full_staging, load_type, primary_keys):
    table_exists = spark.catalog.tableExists(full_staging)
    if not table_exists:
        df_staged.write.saveAsTable(full_staging)
        return "created"

    if load_type == "overwrite":
        spark.sql(f"TRUNCATE TABLE {full_staging}")
        df_staged.write.mode("append").saveAsTable(full_staging)
        return "truncated + loaded"
    elif load_type == "append":
        df_staged.write.mode("append").saveAsTable(full_staging)
        return "appended"
    elif load_type == "delta":
        if not primary_keys:
            raise ValueError("load_type is 'delta' but no primary key columns are defined")
        merge_condition = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in primary_keys])
        delta_table = DeltaTable.forName(spark, full_staging)
        (delta_table.alias("target").merge(df_staged.alias("source"), merge_condition)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        return f"merged on [{', '.join(primary_keys)}]"
    else:
        raise ValueError(f"Unrecognized load_type: '{load_type}'")


def process_staging_table(source_name, table_name, column_rows, load_type):
    landing_table_name = derive_landing_table_name(source_name, table_name)
    staging_table_name = derive_staging_table_name(source_name, table_name)
    full_landing = f"{catalog}.{bronze_schema}.{landing_table_name}"
    full_staging = f"{catalog}.{bronze_schema}.{staging_table_name}"

    if not spark.catalog.tableExists(full_landing):
        return ("SKIPPED", full_landing, full_staging, "Landing table does not exist")

    try:
        df = spark.table(full_landing)
        has_run_id = "run_id" in df.columns
        last_watermark = get_last_watermark(source_name, table_name) if has_run_id else None
        staging_exists = spark.catalog.tableExists(full_staging)

        # --- freshness check, BEFORE any casting work is done ---
        if has_run_id:
            if load_type in ("overwrite", "delta"):
                current_max_run_id = df.agg(F.max("run_id")).first()[0]
                if current_max_run_id is not None and current_max_run_id == last_watermark and staging_exists:
                    return ("SKIPPED_UNCHANGED", full_landing, full_staging,
                            f"No new data since run_id={last_watermark}")
            elif load_type == "append":
                if last_watermark is not None:
                    df = df.filter(F.col("run_id") > last_watermark)
                if df.count() == 0:
                    return ("SKIPPED_UNCHANGED", full_landing, full_staging,
                            f"No new rows since run_id={last_watermark}")
        # if this landing table has no run_id column at all, freshness can't be
        # determined — falls through and processes the full table, same as before

        actual_cols = {c.lower(): c for c in df.columns}

        select_exprs = []
        missing_cols = []
        primary_keys = []

        for row in sorted(column_rows, key=lambda r: r["seq_id"] or 0):
            source_col = row["source_column_name"]
            target_col = row["target_column_name"]
            actual_col = actual_cols.get(source_col.lower())

            if actual_col is None:
                missing_cols.append(source_col)
                cast_expr = build_cast_expr("NULL", row["source_data_type"], row["column_length"], row["column_precision"])
            else:
                cast_expr = build_cast_expr(f"`{actual_col}`", row["source_data_type"], row["column_length"], row["column_precision"])

            select_exprs.append(F.expr(cast_expr).alias(target_col))
            if row["is_primary_key"]:
                primary_keys.append(target_col)

        for meta_col in ["run_id", "source_file", "is_active"]:
            if meta_col in df.columns:
                select_exprs.append(F.col(meta_col))

        select_exprs.append(F.current_timestamp().alias("created_at"))
        select_exprs.append(F.current_timestamp().alias("updated_at"))
        select_exprs.append(F.lit(f"{source_name.upper()}_STAGING_LOAD").alias("created_by"))

        df_staged = df.select(*select_exprs)
        row_count = df_staged.count()

        write_result = write_staging(df_staged, full_staging, load_type, primary_keys)

        # --- only after a successful write, move the watermark forward ---
        if has_run_id and load_type in ("overwrite", "append", "delta"):
            new_max_run_id = df.agg(F.max("run_id")).first()[0]
            if new_max_run_id is not None:
                update_watermark(source_name, table_name, new_max_run_id)

        status = "SUCCESS" if not missing_cols else "SUCCESS_WITH_GAPS"
        detail = f"{row_count} rows, {write_result}"
        if missing_cols:
            detail += f" | missing columns filled as NULL: {missing_cols}"
        return (status, full_landing, full_staging, detail)

    except Exception as e:
        # Full error, not just the first line — a truncated message
        # already cost real debugging time earlier on the SFTP side.
        return ("FAILED", full_landing, full_staging, str(e))


load_type_by_source = get_load_type_by_source()

config_df = spark.table(column_config_table).filter(F.col("is_active") == True)
config_rows = [row.asDict() for row in config_df.collect()]

tables_grouped = {}
for row in config_rows:
    key = (row["source_name"], row["table_name"])
    tables_grouped.setdefault(key, []).append(row)

results = []
for (source_name, table_name), column_rows in tables_grouped.items():
    load_type = load_type_by_source.get(source_name, "overwrite")
    status, full_landing, full_staging, detail = process_staging_table(source_name, table_name, column_rows, load_type)
    print(f"{status:<20} | [{load_type}] {full_staging} | {detail}")
    results.append((source_name, table_name, load_type, status, detail))

succeeded = [r for r in results if r[3] in ("SUCCESS", "SUCCESS_WITH_GAPS")]
skipped_no_landing = [r for r in results if r[3] == "SKIPPED"]
skipped_unchanged = [r for r in results if r[3] == "SKIPPED_UNCHANGED"]
failed = [r for r in results if r[3] == "FAILED"]

print("\n" + "=" * 70)
print("STAGING LAYER RUN SUMMARY")
print("=" * 70)
print(f"Total tables in config      : {len(results)}")
print(f"Succeeded                   : {len(succeeded)}")
print(f"Skipped (landing missing)   : {len(skipped_no_landing)}")
print(f"Skipped (no new data)       : {len(skipped_unchanged)}")
print(f"Failed                      : {len(failed)}")

if skipped_no_landing:
    print("\nSkipped — no landing table found:")
    for source_name, table_name, load_type, _, _ in skipped_no_landing:
        print(f"  - [{source_name}] {table_name}")

if skipped_unchanged:
    print("\nSkipped — no new data since last run:")
    for source_name, table_name, load_type, _, detail in skipped_unchanged:
        print(f"  - [{source_name}] {table_name}: {detail}")

if failed:
    print("\nFailed — needs attention:")
    for source_name, table_name, load_type, _, detail in failed:
        print(f"  - [{source_name}] {table_name}: {detail}")