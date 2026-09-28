# Databricks notebook source
import re
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, MapType

catalog = "apnimed_dw_dev"
bronze_schema = "bronze"
metadata_schema = "metadata"
mapping_table = f"{catalog}.{metadata_schema}.metadata_sourcefile_to_table_mapping"
source_table = f"{catalog}.{metadata_schema}.metadata_source_details"

RESERVED_COLS = {"created_at", "updated_at", "created_by", "is_active", "run_id", "source_file"}

# get source details
def get_active_sources():
    rows = (
        spark.table(source_table)
        .filter(F.col("is_active") == True)
        .select("source_name", "db_volume", "delimiter", "header",
                "sheet_name", "skip_row", "skip_column")
        .collect()
    )
    return [r.asDict() for r in rows]

# construct landing table name
def derive_landing_table_name(source_name, table_name):
    return f"lndg_{source_name}_{table_name.lower()}"


def get_known_table_names(compiled_mappings):
    return {table_name for _, table_name in compiled_mappings}


def resolve_table_name(folder_name, compiled_mappings):
    for pattern, table_name in compiled_mappings:
        if pattern.match(folder_name):
            return table_name
    return None

# cast all columns to string or json or array of strong, depending on data type 
def stringify_all_columns(df):
    exprs = []
    for field in df.schema.fields:
        if field.name in RESERVED_COLS:
            exprs.append(F.col(f"`{field.name}`"))
            continue
        if isinstance(field.dataType, (ArrayType, StructType, MapType)):
            exprs.append(F.to_json(F.col(f"`{field.name}`")).alias(field.name))
        else:
            exprs.append(F.col(f"`{field.name}`").cast("string").alias(field.name))
    return df.select(*exprs)


# Reads a table folder in any supported format and returns a DataFrame
def read_table_folder(file_paths, delimiter, header, sheet_name, skip_row, skip_column):
    extensions = set(p.rsplit(".", 1)[-1].lower() for p in file_paths)

    if len(extensions) != 1:
        raise ValueError(f"Folder contains mixed file types, expected exactly one: {extensions}")

    ext = extensions.pop()
    header_flag = bool(header) if header is not None else True
    delim = delimiter if delimiter else ","

    if ext == "parquet":
        df = spark.read.parquet(*file_paths)
        return df.withColumn("source_file", F.col("_metadata.file_path"))

    elif ext in ("csv", "txt"):
        df = (
            spark.read
            .option("header", header_flag)
            .option("delimiter", delim)
            .csv(file_paths)
        )
        return df.withColumn("source_file", F.col("_metadata.file_path"))

    elif ext in ("xlsx", "xls"):
        sheet = sheet_name if sheet_name else 0
        skiprows = int(skip_row) if skip_row else 0
        skipcols = int(skip_column) if skip_column else 0

        pandas_frames = []
        for path in file_paths:
            pdf = pd.read_excel(
                path,
                sheet_name=sheet,
                header=0 if header_flag else None,
                skiprows=skiprows,
            )
            if skipcols > 0:
                pdf = pdf.iloc[:, skipcols:]
            pdf["source_file"] = path
            pandas_frames.append(pdf)

        combined_pdf = pd.concat(pandas_frames, ignore_index=True) if len(pandas_frames) > 1 else pandas_frames[0]
        source_file_col = combined_pdf["source_file"]
        combined_pdf = combined_pdf.drop(columns=["source_file"]).astype(str)
        combined_pdf["source_file"] = source_file_col.astype(str)
        return spark.createDataFrame(combined_pdf)

    elif ext == "json":
        dfs = []
        for path in file_paths:
            df = (
                spark.read
                .option("multiLine", True)
                .option("primitivesAsString", "true")
                .json(path)
            )
            df = stringify_all_columns(df)
            df = df.withColumn("source_file", F.lit(path))
            dfs.append(df)

        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)
        return combined

    else:
        raise ValueError(f"Unsupported file format: {ext}")

# This is the main function
def process_source(src, run_by_label):
    source_name = src["source_name"]
    db_volume = src["db_volume"]

    if not db_volume:
        print(f"SKIPPED SOURCE | {source_name} | db_volume is empty in metadata_source_details")
        return []

    archive_base_path = f"{db_volume}/archive"

    mapping_rows = (
        spark.table(mapping_table)
        .filter((F.col("source_name") == source_name) & (F.col("is_active") == True))
        .select("folder_file_pattern", "table_name")
        .collect()
    )
    compiled_mappings = [(re.compile(r["folder_file_pattern"]), r["table_name"]) for r in mapping_rows]
    if not compiled_mappings:
        print(f"SKIPPED SOURCE | {source_name} | No active mapping rows found")
        return []

    known_table_names = get_known_table_names(compiled_mappings)

    try:
        all_entries = [f.name.rstrip("/") for f in dbutils.fs.ls(db_volume) if f.isDir()]
        date_folders = [d for d in all_entries if d != "archive"]
    except Exception as e:
        print(f"SKIPPED SOURCE | {source_name} | Could not list {db_volume}: {e}")
        return []

    results = []
    for date_folder in date_folders:
        date_path = f"{db_volume}/{date_folder}"
        try:
            table_folders = [f.name.rstrip("/") for f in dbutils.fs.ls(date_path) if f.isDir()]
        except Exception as e:
            print(f"SKIPPED DATE | {source_name}/{date_folder} | {e}")
            continue

        for table_folder in table_folders:
            table_folder_path = f"{date_path}/{table_folder}"
            table_name = table_folder if table_folder in known_table_names else None
            full_landing_table = f"{catalog}.{bronze_schema}.{derive_landing_table_name(source_name, table_name)}" if table_name else None

            if table_name is None:
                print(f"UNMAPPED    | {source_name}/{date_folder}/{table_folder} | Add a mapping row")
                results.append((source_name, date_folder, table_folder, None, "UNMAPPED", "No mapping match"))
                continue

            try:
                files = [f.path for f in dbutils.fs.ls(table_folder_path) if not f.isDir()]
                if not files:
                    results.append((source_name, date_folder, table_folder, table_name, "SKIPPED", "Empty folder"))
                    continue
                df = read_table_folder(
                    files,
                    delimiter=src["delimiter"],
                    header=src["header"],
                    sheet_name=src["sheet_name"],
                    skip_row=src["skip_row"],
                    skip_column=src["skip_column"],
                )

                df = stringify_all_columns(df)

                df_final = (
                    df.withColumn("created_at", F.current_timestamp())
                      .withColumn("updated_at", F.current_timestamp())
                      .withColumn("created_by", F.lit(run_by_label))
                      .withColumn("is_active", F.lit(True))
                      .withColumn("run_id", F.lit(date_folder))
                )
                row_count = df_final.count()

                # Landing is always append-or-create, never overwrite.
                df_final.write.mode("append").option("mergeSchema", "true").saveAsTable(full_landing_table)

                archive_path = f"{archive_base_path}/{date_folder}/{table_folder}"
                dbutils.fs.mv(table_folder_path, archive_path, recurse=True)

                print(f"SUCCESS | {full_landing_table} | {row_count} rows appended | archived to {archive_path}")
                results.append((source_name, date_folder, table_folder, table_name, "SUCCESS", f"{row_count} rows"))

            except Exception as e:
                error_msg = str(e).splitlines()[0]
                print(f"FAILED  | {full_landing_table} | {error_msg}")
                results.append((source_name, date_folder, table_folder, table_name, "FAILED", error_msg))

    return results

# main function is called here for each source
all_results = []
for src in get_active_sources():
    run_by_label = f"{src['source_name'].upper()}_INGESTION"
    all_results.extend(process_source(src, run_by_label))

succeeded = [r for r in all_results if r[4] == "SUCCESS"]
unmapped = [r for r in all_results if r[4] == "UNMAPPED"]
failed = [r for r in all_results if r[4] == "FAILED"]

print("\n" + "=" * 70)
print("LANDING SUMMARY")
print("=" * 70)
print(f"Succeeded : {len(succeeded)}")
print(f"Unmapped  : {len(unmapped)}")
print(f"Failed    : {len(failed)}")

if unmapped:
    print("\nFolders with no mapping row — add these to metadata_sourcefile_to_table_mapping:")
    for source_name, date_folder, table_folder, _, _, _ in unmapped:
        print(f"  - [{source_name}] {table_folder}")

if failed:
    print("\nFailed — needs attention:")
    for source_name, date_folder, table_folder, table_name, _, detail in failed:
        print(f"  - [{source_name}] {date_folder}/{table_folder}: {detail}")