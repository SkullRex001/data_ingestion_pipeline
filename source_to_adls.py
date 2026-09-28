# Databricks notebook source
# DBTITLE 1,STEEPROCK_OLA_HARDCODED
# =============================================================================
# SteepRock OLA — raw JSON landing, adapted to plug into pull_from_api().
# Auth, retry/backoff, gap tracking, atomic writes, nested pagination, and
# completeness verification are all unchanged from the reference script —
# only the path source and the top-level entry point changed.
# =============================================================================

import requests
import os
import json
import time
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
ROOT_DOMAIN = "https://api.ola.steeprockinc.com"
PEOPLE_URL = f"{ROOT_DOMAIN}/v1/people/"
TOKEN_URL = f"{ROOT_DOMAIN}/oauth2/token"

SECRET_SCOPE = "apnimed-dev-secret-scope"
OLA_CLIENT_ID_KEY = "steeprock-ola-client-id"
OLA_CLIENT_SECRET_KEY = "steeprock-ola-password"

# NOT hardcoded — set per-run via set_steeprock_ola_path(), sourced from
# metadata_source_details.db_volume where source_name='steeprock_ola'.
# flush_batch/write_object_manifest below reference this as a module global,
# so it must be set BEFORE any pull_* function runs in a given call.
SR_PATH = None
API_DOMAIN = "OLA"


def set_steeprock_ola_path(db_volume):
    global SR_PATH
    SR_PATH = db_volume


FULL_PEOPLE_FIELDS = (
    "uri,id,ola_display_url,display_name,first_name,middle_name,last_name,"
    "patronymic,credentials,npi,country_code,specialties,emails,phones,"
    "primary_org.{uri,id,name,npi,address1,address2,city,state,postal_code,"
    "country_code,lat,lng},image_url,bio,gender,social_handles,custom_tags"
)

NESTED_BASE_FIELDS = "id,npi,congress_abstracts,articles,social_posts,focused_claims"
NESTED_SUBRESOURCE_FIELDS = ["focused_claims", "social_posts", "articles", "congress_abstracts"]

PROFILES_UNIVERSE_FIELDS = (
    "uri,id,ola_display_url,display_name,first_name,middle_name,last_name,"
    "patronymic,credentials,npi,country_code,specialties,emails,phones,"
    "primary_org.{uri,id,name,npi,address1,address2,city,state,postal_code,"
    "country_code,lat,lng},image_url,social_handles,"
    "congress_abstracts,articles,social_posts,focused_claims"
)

# COST WARNING: steeprock_kol_nested_universe and steeprock_profiles_universe
# both walk 4 nested paginated sub-resources for EVERY person in the full
# ~700K-person universe. TEST_MAX_PEOPLE caps this for a cheap validation
# run — set to None for a full, unscoped run.
TEST_MAX_PEOPLE = None

PROFILES_LIMIT = 200
PEOPLE_LIMIT = 200
NPI_CHUNK_SIZE = 100
NESTED_SOURCE_TABLE = "apnimed_dw_dev.bronze.cross_ref_hcp"

FLUSH_EVERY_PAGES = 25
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5

LOG_TABLE = "apnimed_dw_dev.bronze.steeprock_ola_ingestion_log"
LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("object", StringType(), True),
    StructField("url", StringType(), True),
    StructField("requested_at", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("count_returned", IntegerType(), True),
    StructField("error_message", StringType(), True),
])

UNMATCHED_NPI_TABLE = "apnimed_dw_dev.bronze.steeprock_kol_nested_unmatched_npis"
UNMATCHED_NPI_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("object", StringType(), True),
    StructField("npi", StringType(), True),
    StructField("chunk_index", IntegerType(), True),
    StructField("chunk_range", StringType(), True),
    StructField("checked_at", StringType(), True),
])


# -----------------------------------------------------------------------------
# Auth — OAuth2 client_credentials, cached Bearer token
# -----------------------------------------------------------------------------
_token_cache = {"access_token": None, "expires_at": 0}


def get_access_token(force_refresh=False):
    now = time.time()
    if not force_refresh and _token_cache["access_token"] and now < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    client_id = dbutils.secrets.get(scope=SECRET_SCOPE, key=OLA_CLIENT_ID_KEY)
    client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=OLA_CLIENT_SECRET_KEY)

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": "api:access",
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            last_error = e
            wait = min(2 ** attempt, 30)
            print(f"  auth attempt {attempt}/{MAX_RETRIES} failed ({e}), retrying in {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            data = resp.json()
            _token_cache["access_token"] = data["access_token"]
            _token_cache["expires_at"] = now + data.get("expires_in", 3600) - 60
            return _token_cache["access_token"]

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            last_error = RuntimeError(f"auth status {resp.status_code}: {resp.text[:200]}")
        else:
            raise RuntimeError(f"OLA authentication failed permanently: {resp.status_code} {resp.text[:200]}")

        wait = min(2 ** attempt, 30)
        print(f"  auth attempt {attempt}/{MAX_RETRIES} failed, retrying in {wait}s")
        time.sleep(wait)

    raise RuntimeError(f"OLA authentication failed after {MAX_RETRIES} attempts: {last_error}")


def auth_headers():
    return {"Authorization": f"Bearer {get_access_token()}"}


def atomic_write_json(file_path, data, max_attempts=3):
    tmp_path = f"{file_path}.tmp"
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, file_path)
            return
        except Exception as e:
            last_error = e
            print(f"  write attempt {attempt}/{max_attempts} failed: {e}")
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"Failed to write {file_path} after {max_attempts} attempts: {last_error}")


def make_log_entry(run_id, object_name, url):
    return {
        "run_id": run_id, "object": object_name, "url": url,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "status_code": None, "count_returned": None, "error_message": None,
    }


def flush_log(log_entries):
    if not log_entries:
        return
    df = spark.createDataFrame(log_entries, schema=LOG_SCHEMA)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(LOG_TABLE)
    log_entries.clear()


def flush_unmatched_npis(unmatched_entries):
    if not unmatched_entries:
        return
    df = spark.createDataFrame(unmatched_entries, schema=UNMATCHED_NPI_SCHEMA)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(UNMATCHED_NPI_TABLE)
    unmatched_entries.clear()


def flush_batch(buffer, object_name, run_id, batch_num, created_by, include_ingest_audit=True):
    run_date = datetime.now(timezone.utc).strftime("%m%d%Y")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_folder = os.path.join(SR_PATH, API_DOMAIN, run_date, object_name)
    os.makedirs(object_folder, exist_ok=True)

    file_name = f"batch{batch_num:05d}_{run_id}_{ts}.json"
    file_path = os.path.join(object_folder, file_name)

    row_count = len(buffer)
    landed_at = datetime.now(timezone.utc).isoformat()

    enriched = []
    for row in buffer:
        row = dict(row)
        row["load_timestamp"] = landed_at
        if include_ingest_audit:
            row["_ingest_file_nm"] = file_name
            row["_ingest_row_cnt"] = row_count
            row["_ingest_created_at"] = landed_at
            row["_ingest_updated_at"] = landed_at
            row["_ingest_created_by"] = created_by
            row["_ingest_is_active"] = True
            row["_ingest_run_id"] = run_id
            row["_ingest_source_file"] = file_path
        enriched.append(row)

    atomic_write_json(file_path, enriched)
    return file_path


def write_object_manifest(object_name, result, run_id):
    manifest_dir = os.path.join(SR_PATH, API_DOMAIN, "_manifests", run_id)
    os.makedirs(manifest_dir, exist_ok=True)
    path = os.path.join(manifest_dir, f"{object_name}.json")
    atomic_write_json(path, {
        "run_id": run_id, "object": object_name, "result": result,
        "verification_status": "NOT YET VERIFIED",
        "committed_at": datetime.now(timezone.utc).isoformat(),
    })
    return path


# -----------------------------------------------------------------------------
# Robust GET — shared retry/backoff/re-auth wrapper for every call
# -----------------------------------------------------------------------------
def robust_get(url, params, run_id, object_name, log_entries):
    for attempt in range(1, MAX_RETRIES + 1):
        entry = make_log_entry(run_id, object_name, url)
        try:
            resp = requests.get(url, headers=auth_headers(), params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            wait = min(2 ** attempt, 60)
            entry["status_code"] = None
            entry["error_message"] = f"request exception: {e}, retrying in {wait}s"
            log_entries.append(entry)
            print(f"  request failed ({e}), retrying in {wait}s")
            time.sleep(wait)
            continue

        entry["status_code"] = resp.status_code

        if resp.status_code == 200:
            body = resp.json()
            entry["count_returned"] = len(body.get("results", [])) if isinstance(body, dict) else None
            log_entries.append(entry)
            return body

        if resp.status_code == 401:
            entry["error_message"] = "token expired/rejected, re-authenticating"
            log_entries.append(entry)
            print("  401 — forcing token refresh")
            get_access_token(force_refresh=True)
            continue

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            wait = min(2 ** attempt, 60)
            entry["error_message"] = f"status {resp.status_code}, retrying in {wait}s"
            log_entries.append(entry)
            print(f"  status {resp.status_code}, retrying in {wait}s")
            time.sleep(wait)
            continue

        entry["error_message"] = f"unrecoverable: {resp.text[:200]}"
        log_entries.append(entry)
        print(f"  unrecoverable status {resp.status_code}: {resp.text[:200]}")
        return None

    print(f"  GAVE UP after {MAX_RETRIES} attempts: {url}")
    return None


def resolve_next_url(next_val):
    if not next_val:
        return None
    return f"{ROOT_DOMAIN}{next_val}" if next_val.startswith("/") else next_val


# -----------------------------------------------------------------------------
# Object: steeprock_kol_universe — flat, full pull
# -----------------------------------------------------------------------------
def pull_kol_universe(run_id, log_entries, created_by):
    object_name = "steeprock_kol_universe"
    print(f"\n--- {object_name} ---")
    buffer, batch_files, gaps = [], [], []
    batch_num, pages_in_buffer = 0, 0
    api_reported_total = None

    url = PEOPLE_URL
    params = {"offset": 0, "limit": PEOPLE_LIMIT, "fields": FULL_PEOPLE_FIELDS}

    while url:
        body = robust_get(url, params, run_id, object_name, log_entries)

        if body is None:
            gaps.append({
                "url": url, "failed_at": datetime.now(timezone.utc).isoformat(),
                "rows_landed_before_gap": sum(b["record_count"] for b in batch_files) + len(buffer),
            })
            print(f"  GAP recorded, stopping this object: {url}")
            break

        if api_reported_total is None and "total" in body:
            api_reported_total = body.get("total")
            print(f"  API reports total={api_reported_total} for this object")

        page_data = body.get("results", [])
        if not page_data:
            print("  empty page, stopping")
            break

        buffer.extend(page_data)
        pages_in_buffer += 1

        if pages_in_buffer >= FLUSH_EVERY_PAGES or not body.get("next"):
            saved = flush_batch(buffer, object_name, run_id, batch_num, created_by)
            batch_files.append({"path": saved, "record_count": len(buffer)})
            total_so_far = sum(b["record_count"] for b in batch_files)
            print(f"  flushed batch {batch_num} ({len(buffer)} rows) -> {saved}  [total so far: {total_so_far}]")
            buffer, pages_in_buffer = [], 0
            batch_num += 1
            flush_log(log_entries)

        url = resolve_next_url(body.get("next"))
        params = None

    if buffer:
        saved = flush_batch(buffer, object_name, run_id, batch_num, created_by)
        batch_files.append({"path": saved, "record_count": len(buffer)})
        print(f"  final partial flush -> {saved}")
        flush_log(log_entries)

    gap_file = None
    if gaps:
        error_dir = os.path.join(SR_PATH, API_DOMAIN, "error")
        os.makedirs(error_dir, exist_ok=True)
        gap_file = os.path.join(error_dir, f"_gaps_{object_name}_{run_id}.json")
        atomic_write_json(gap_file, {"object": object_name, "run_id": run_id, "gaps": gaps})
        print(f"  {len(gaps)} gap(s) written to {gap_file}")

    total_landed = sum(b["record_count"] for b in batch_files)

    completeness_status = "UNKNOWN — API did not return a total field"
    if api_reported_total is not None:
        if total_landed == api_reported_total:
            completeness_status = f"MATCH — landed {total_landed} == API total {api_reported_total}"
        else:
            completeness_status = (
                f"MISMATCH — landed {total_landed} != API total {api_reported_total} "
                f"(diff {total_landed - api_reported_total:+d})"
            )
            print(f"  *** {completeness_status} — check gap file / re-run this object ***")

    print(f"  DONE {object_name}: {total_landed} rows landed. Completeness: {completeness_status}")
    return {"object": object_name, "total_landed": total_landed,
            "api_reported_total": api_reported_total,
            "completeness_status": completeness_status,
            "batch_files": batch_files, "gaps": gaps, "gap_file": gap_file}


# -----------------------------------------------------------------------------
# Shared nested-pagination helpers
# -----------------------------------------------------------------------------
def fetch_all_paginated_nested(initial_url, run_id, object_name, log_entries):
    all_results = []
    pages_fetched = 0
    url = resolve_next_url(initial_url) if initial_url.startswith("/") else initial_url
    while url:
        body = robust_get(url, None, run_id, object_name, log_entries)
        if body is None:
            return all_results, False, pages_fetched
        page_data = body.get("results", [])
        pages_fetched += 1
        if not page_data:
            break
        all_results.extend(page_data)
        url = resolve_next_url(body.get("next"))
    return all_results, True, pages_fetched


def process_person_object(person, run_id, object_name, log_entries):
    person_complete = True
    for field in NESTED_SUBRESOURCE_FIELDS:
        sub = person.get(field)
        if not isinstance(sub, dict) or "uri" not in sub:
            continue
        if sub.get("next"):
            results, is_complete, pages = fetch_all_paginated_nested(
                sub["uri"], run_id, object_name, log_entries
            )
            sub["results"] = results
            sub["_pagination_complete"] = is_complete
            sub["_pages_fetched"] = pages + 1
            if not is_complete:
                person_complete = False
        else:
            sub["results"] = sub.get("results", [])
            sub["_pagination_complete"] = True
            sub["_pages_fetched"] = 1
    return person_complete


# -----------------------------------------------------------------------------
# Generic full-universe pull with per-person nested walk — shared by
# steeprock_kol_nested_universe and steeprock_profiles_universe
# -----------------------------------------------------------------------------
def pull_full_universe_nested(object_name, fields, page_limit, run_id, log_entries,
                               created_by, test_max_people=None, include_ingest_audit=True):
    print(f"\n--- {object_name} ---")
    if test_max_people is not None:
        print(f"  TEST MODE: capped at {test_max_people} people.")

    buffer, batch_files, gaps = [], [], []
    batch_num, pages_in_buffer = 0, 0
    api_reported_total = None
    people_processed = 0
    incomplete_people = 0

    url = PEOPLE_URL
    params = {"offset": 0, "limit": page_limit, "fields": fields}

    while url:
        if test_max_people is not None and people_processed >= test_max_people:
            print(f"  test cap ({test_max_people}) reached, stopping early")
            break

        body = robust_get(url, params, run_id, object_name, log_entries)

        if body is None:
            gaps.append({
                "url": url, "failed_at": datetime.now(timezone.utc).isoformat(),
                "rows_landed_before_gap": sum(b["record_count"] for b in batch_files) + len(buffer),
            })
            print(f"  GAP recorded, stopping this object: {url}")
            break

        if api_reported_total is None and "total" in body:
            api_reported_total = body.get("total")
            print(f"  API reports total={api_reported_total} for this object")

        page_data = body.get("results", [])
        if not page_data:
            print("  empty page, stopping")
            break

        for person in page_data:
            if test_max_people is not None and people_processed >= test_max_people:
                break
            if not process_person_object(person, run_id, object_name, log_entries):
                incomplete_people += 1
            buffer.append(person)
            people_processed += 1

        pages_in_buffer += 1

        if pages_in_buffer >= FLUSH_EVERY_PAGES or not body.get("next"):
            saved = flush_batch(buffer, object_name, run_id, batch_num, created_by,
                                 include_ingest_audit=include_ingest_audit)
            batch_files.append({"path": saved, "record_count": len(buffer)})
            print(f"  flushed batch {batch_num} ({len(buffer)} people) -> {saved}  [total so far: {people_processed}]")
            buffer, pages_in_buffer = [], 0
            batch_num += 1
            flush_log(log_entries)

        url = resolve_next_url(body.get("next"))
        params = None

    if buffer:
        saved = flush_batch(buffer, object_name, run_id, batch_num, created_by,
                             include_ingest_audit=include_ingest_audit)
        batch_files.append({"path": saved, "record_count": len(buffer)})
        print(f"  final partial flush -> {saved}")
        flush_log(log_entries)

    gap_file = None
    if gaps:
        error_dir = os.path.join(SR_PATH, API_DOMAIN, "error")
        os.makedirs(error_dir, exist_ok=True)
        gap_file = os.path.join(error_dir, f"_gaps_{object_name}_{run_id}.json")
        atomic_write_json(gap_file, {"object": object_name, "run_id": run_id, "gaps": gaps})
        print(f"  {len(gaps)} gap(s) written to {gap_file}")

    total_landed = sum(b["record_count"] for b in batch_files)

    if test_max_people is not None:
        completeness_status = f"TEST RUN — capped at {test_max_people}, not a full-universe completeness check"
    elif api_reported_total is not None:
        if total_landed == api_reported_total:
            completeness_status = f"MATCH — landed {total_landed} == API total {api_reported_total}"
        else:
            completeness_status = (
                f"MISMATCH — landed {total_landed} != API total {api_reported_total} "
                f"(diff {total_landed - api_reported_total:+d})"
            )
            print(f"  *** {completeness_status} — check gap file / re-run this object ***")
    else:
        completeness_status = "UNKNOWN — API did not return a total field"

    print(f"  DONE {object_name}: {total_landed} rows landed. Completeness: {completeness_status}")
    if incomplete_people:
        print(f"  *** {incomplete_people} of {total_landed} people have incomplete nested data — "
              f"check landed JSON for _pagination_complete=False. ***")
    return {"object": object_name, "total_landed": total_landed,
            "api_reported_total": api_reported_total,
            "completeness_status": completeness_status,
            "incomplete_nested_people": incomplete_people,
            "batch_files": batch_files, "gaps": gaps, "gap_file": gap_file}


# -----------------------------------------------------------------------------
# Object: steeprock_kol_nested_universe — full universe, per explicit request
# (NPI-scoped legacy version kept below, not called by default)
# -----------------------------------------------------------------------------
def pull_kol_nested_universe(run_id, log_entries, created_by, test_max_people=None):
    return pull_full_universe_nested(
        "steeprock_kol_nested_universe", NESTED_BASE_FIELDS, PEOPLE_LIMIT,
        run_id, log_entries, created_by,
        test_max_people=test_max_people, include_ingest_audit=False,
    )


# -----------------------------------------------------------------------------
# Object: steeprock_profiles_universe — full universe, full profile fields
# -----------------------------------------------------------------------------
def pull_profiles_universe(run_id, log_entries, created_by, test_max_people=None):
    return pull_full_universe_nested(
        "steeprock_profiles_universe", PROFILES_UNIVERSE_FIELDS, PROFILES_LIMIT,
        run_id, log_entries, created_by, test_max_people=test_max_people,
    )


# -----------------------------------------------------------------------------
# LEGACY / not in the dispatch table below. NPI-scoped version, kept for
# reference in case the KOL-only scope is wanted again instead of the
# current full-universe version.
# -----------------------------------------------------------------------------
def get_kol_npi_list():
    try:
        npi_df = spark.sql(f"""
            SELECT npi FROM {NESTED_SOURCE_TABLE}
            WHERE lower(apni_kol__c) LIKE '%yes__c%'
              AND npi IS NOT NULL AND lower(trim(npi)) != 'null'
        """)
        npi_list = [row["npi"] for row in npi_df.collect()]
        total_kol_flagged = spark.sql(f"""
            SELECT count(*) AS c FROM {NESTED_SOURCE_TABLE}
            WHERE lower(apni_kol__c) LIKE '%yes__c%'
        """).collect()[0]["c"]
        dropped = total_kol_flagged - len(npi_list)
        if dropped:
            print(f"  NOTE: {dropped} KOL-flagged row(s) had a NULL/'null' npi and were excluded.")
        return npi_list
    except Exception as e:
        raise RuntimeError(f"Could not read KOL-flagged NPI list from {NESTED_SOURCE_TABLE}: {e}")


# -----------------------------------------------------------------------------
# Dispatch — maps a table_name from metadata_sourcefile_to_table_mapping
# to the function that knows how to pull it. Unlike Veeva, there's no
# generic "SELECT * FROM {table_name}" here — each object needs its own
# field list and, for two of them, its own nested-walk logic.
# -----------------------------------------------------------------------------
STEEPROCK_OLA_DISPATCH = {
    "steeprock_kol_universe": lambda run_id, log_entries, created_by:
        pull_kol_universe(run_id, log_entries, created_by),
    "steeprock_kol_nested_universe": lambda run_id, log_entries, created_by:
        pull_kol_nested_universe(run_id, log_entries, created_by, test_max_people=TEST_MAX_PEOPLE),
    "steeprock_profiles_universe": lambda run_id, log_entries, created_by:
        pull_profiles_universe(run_id, log_entries, created_by, test_max_people=TEST_MAX_PEOPLE),
}

# COMMAND ----------

# DBTITLE 1,STEEPROCK_OLMS_FUNCTIONS
# =============================================================================
# SteepRock OLMS — raw JSON landing, adapted to plug into pull_from_api().
# All 5 objects share one generic pull path (flat, single-endpoint, paginated)
# — no nested walk, no NPI-scoping dependency, unlike OLA.
# =============================================================================

import requests
import os
import json
import time
import base64
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
ROOT_DOMAIN = "https://api.apnimed.steeprockinc.com"

SECRET_SCOPE = "steeprock_API_creds_databricks_scope"
USERNAME_KEY = "username"
PASSWORD_KEY = "password"

# NOT hardcoded — set per-run via set_steeprock_olms_path(), sourced from
# metadata_source_details.db_volume where source_name='steeprock_olms'.
SR_PATH = None
API_DOMAIN = "OLMS"


def set_steeprock_olms_path(db_volume):
    global SR_PATH
    SR_PATH = db_volume


# object_name (final table name) -> endpoint URL. Hardcoded, same reasoning
# as OLA's dispatch table — there's no metadata API to discover these from,
# these are the confirmed, already-verified object names and endpoints.
OBJECT_REGISTRY = {
    "steeprock_olms_insight_user": {
        "url": f"{ROOT_DOMAIN}/v2/entities/xuser",
    },
    "steeprock_olms_insight_question": {
        "url": f"{ROOT_DOMAIN}/v2/entities/insightquestion",
    },
    "steeprock_olms_insight": {
        "url": f"{ROOT_DOMAIN}/v2/entities/insightresponse",
    },
    "steeprock_olms_insight_person": {
        "url": f"{ROOT_DOMAIN}/v2/entities/person?include=firstname,lastname,mdm,title,tlltier",
    },
    "steeprock_olms_tier_values": {
        "url": f"{ROOT_DOMAIN}/v2/entities/tlltier/",
    },
}

FLUSH_EVERY_PAGES = 25
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 5

LOG_TABLE = "apnimed_dw_dev.dw_bronze.steeprock_olms_ingestion_log"
LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("object", StringType(), True),
    StructField("url", StringType(), True),
    StructField("requested_at", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("count_returned", IntegerType(), True),
    StructField("error_message", StringType(), True),
])


# -----------------------------------------------------------------------------
# Auth — Basic Auth via secret scope (no token, no expiry -> nothing to
# refresh mid-run). Pulled from dbutils.secrets, not hardcoded.
# -----------------------------------------------------------------------------
def get_auth_headers():
    username = dbutils.secrets.get(scope=SECRET_SCOPE, key=USERNAME_KEY)
    password = dbutils.secrets.get(scope=SECRET_SCOPE, key=PASSWORD_KEY)
    auth_base64 = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# -----------------------------------------------------------------------------
# Atomic write / logging / batching helpers — same pattern as Veeva/OLA
# -----------------------------------------------------------------------------
def atomic_write_json(file_path, data, max_attempts=3):
    tmp_path = f"{file_path}.tmp"
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            os.replace(tmp_path, file_path)
            return
        except Exception as e:
            last_error = e
            print(f"  write attempt {attempt}/{max_attempts} failed: {e}")
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"Failed to write {file_path} after {max_attempts} attempts: {last_error}")


def make_log_entry(run_id, object_name, url):
    return {
        "run_id": run_id, "object": object_name, "url": url,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "status_code": None, "count_returned": None, "error_message": None,
    }


def flush_log(log_entries):
    if not log_entries:
        return
    df = spark.createDataFrame(log_entries, schema=LOG_SCHEMA)
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(LOG_TABLE)
    log_entries.clear()


def flush_batch(buffer, object_name, run_id, batch_num, created_by):
    run_date = datetime.now(timezone.utc).strftime("%m%d%Y")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_folder = os.path.join(SR_PATH, API_DOMAIN, run_date, object_name)
    os.makedirs(object_folder, exist_ok=True)

    file_name = f"batch{batch_num:05d}_{run_id}_{ts}.json"
    file_path = os.path.join(object_folder, file_name)

    row_count = len(buffer)
    landed_at = datetime.now(timezone.utc).isoformat()

    enriched = []
    for row in buffer:
        row = dict(row)
        row["load_timestamp"] = landed_at
        row["_ingest_file_nm"] = file_name
        row["_ingest_row_cnt"] = row_count
        row["_ingest_created_at"] = landed_at
        row["_ingest_updated_at"] = landed_at
        row["_ingest_created_by"] = created_by
        row["_ingest_is_active"] = True
        row["_ingest_run_id"] = run_id
        row["_ingest_source_file"] = file_path
        enriched.append(row)

    atomic_write_json(file_path, enriched)
    return file_path


def write_object_manifest(object_name, result, run_id):
    manifest_dir = os.path.join(SR_PATH, API_DOMAIN, "_manifests", run_id)
    os.makedirs(manifest_dir, exist_ok=True)
    path = os.path.join(manifest_dir, f"{object_name}.json")
    atomic_write_json(path, {
        "run_id": run_id, "object": object_name, "result": result,
        "verification_status": "NOT YET VERIFIED",
        "committed_at": datetime.now(timezone.utc).isoformat(),
    })
    return path


# -----------------------------------------------------------------------------
# Robust GET — shared retry/backoff wrapper. 401/403/404/etc are permanent
# here (unlike OLA) — Basic Auth has no token to refresh, so a 401 means the
# credentials are actually wrong, not that a session expired mid-run.
# -----------------------------------------------------------------------------
def robust_get(url, headers, run_id, object_name, log_entries):
    for attempt in range(1, MAX_RETRIES + 1):
        entry = make_log_entry(run_id, object_name, url)
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            wait = min(2 ** attempt, 60)
            entry["status_code"] = None
            entry["error_message"] = f"request exception: {e}, retrying in {wait}s"
            log_entries.append(entry)
            print(f"  request failed ({e}), retrying in {wait}s")
            time.sleep(wait)
            continue

        entry["status_code"] = resp.status_code

        if resp.status_code == 200:
            body = resp.json()
            entry["count_returned"] = len(body.get("results", [])) if isinstance(body, dict) else None
            log_entries.append(entry)
            return body

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            wait = min(2 ** attempt, 60)
            entry["error_message"] = f"status {resp.status_code}, retrying in {wait}s"
            log_entries.append(entry)
            print(f"  status {resp.status_code}, retrying in {wait}s")
            time.sleep(wait)
            continue

        entry["error_message"] = f"unrecoverable: {resp.text[:200]}"
        log_entries.append(entry)
        print(f"  unrecoverable status {resp.status_code}: {resp.text[:200]}")
        return None

    print(f"  GAVE UP after {MAX_RETRIES} attempts: {url}")
    return None


def resolve_next_url(next_val):
    if not next_val:
        return None
    return f"{ROOT_DOMAIN}{next_val}" if next_val.startswith("/") else next_val


# -----------------------------------------------------------------------------
# Generic object pull — flat, single-endpoint, paginated. All 5 OLMS
# objects use this same shape.
# -----------------------------------------------------------------------------
def pull_object(object_name, start_url, headers, run_id, log_entries, created_by):
    print(f"\n--- {object_name} ---")
    buffer, batch_files, gaps = [], [], []
    batch_num, pages_in_buffer = 0, 0
    api_reported_total = None

    url = start_url
    while url:
        body = robust_get(url, headers, run_id, object_name, log_entries)

        if body is None:
            gaps.append({
                "url": url, "failed_at": datetime.now(timezone.utc).isoformat(),
                "rows_landed_before_gap": sum(b["record_count"] for b in batch_files) + len(buffer),
            })
            print(f"  GAP recorded, stopping this object: {url}")
            break

        if api_reported_total is None and isinstance(body, dict) and "total" in body:
            api_reported_total = body.get("total")
            print(f"  API reports total={api_reported_total} for this object")

        page_data = body.get("results", [])
        if not page_data:
            print("  empty page, stopping")
            break

        buffer.extend(page_data)
        pages_in_buffer += 1
        print(f"  page fetched: {len(page_data)} rows, running total: {len(buffer)}")

        if pages_in_buffer >= FLUSH_EVERY_PAGES or not body.get("next"):
            saved = flush_batch(buffer, object_name, run_id, batch_num, created_by)
            batch_files.append({"path": saved, "record_count": len(buffer)})
            print(f"  flushed -> {saved}")
            buffer, pages_in_buffer = [], 0
            batch_num += 1
            flush_log(log_entries)

        url = resolve_next_url(body.get("next"))

    if buffer:
        saved = flush_batch(buffer, object_name, run_id, batch_num, created_by)
        batch_files.append({"path": saved, "record_count": len(buffer)})
        print(f"  final partial flush -> {saved}")
        flush_log(log_entries)

    gap_file = None
    if gaps:
        error_dir = os.path.join(SR_PATH, API_DOMAIN, "error")
        os.makedirs(error_dir, exist_ok=True)
        gap_file = os.path.join(error_dir, f"_gaps_{object_name}_{run_id}.json")
        atomic_write_json(gap_file, {"object": object_name, "run_id": run_id, "gaps": gaps})
        print(f"  {len(gaps)} gap(s) written to {gap_file}")

    total_landed = sum(b["record_count"] for b in batch_files)

    completeness_status = "UNKNOWN — API did not return a total field"
    if api_reported_total is not None:
        if total_landed == api_reported_total:
            completeness_status = f"MATCH — landed {total_landed} == API total {api_reported_total}"
        else:
            completeness_status = (
                f"MISMATCH — landed {total_landed} != API total {api_reported_total} "
                f"(diff {total_landed - api_reported_total:+d})"
            )
            print(f"  *** {completeness_status} — check gap file / re-run this object ***")

    print(f"  DONE {object_name}: {total_landed} rows landed. Completeness: {completeness_status}")
    return {"object": object_name, "total_landed": total_landed,
            "api_reported_total": api_reported_total,
            "completeness_status": completeness_status,
            "batch_files": batch_files, "gaps": gaps, "gap_file": gap_file}

# COMMAND ----------

# DBTITLE 1,VEEVA FUNCTION DEFINITIONS
# =============================================================================
# Veeva VaultCRM — API helpers, called from pull_from_api() in the next cell
# =============================================================================
import os
import json
import time
import requests
from datetime import datetime, timezone


## these root domain, ets stuff might change every 6 months . need to resee that. https://sb-apnimed-apnimed-full.veevavault.com/api/v24.3/call2discussion__v
VEEVA_ROOT_DOMAIN = "https://sb-apnimed-apnimed-full.veevavault.com"
VEEVA_BASE_URL = f"{VEEVA_ROOT_DOMAIN}/api/v24.3"
VEEVA_AUTH_URL = f"{VEEVA_BASE_URL}/auth"
VEEVA_QUERY_URL = f"{VEEVA_BASE_URL}/query"
VEEVA_SECRET_SCOPE = "apnimed-dev-secret-scope"


VEEVA_REQUEST_DELAY_SECONDS = 0.3
VEEVA_REQUEST_TIMEOUT_SECONDS = 30
VEEVA_MAX_REAUTH_ATTEMPTS = 3
VEEVA_MAX_RETRIES = 5
VEEVA_MAX_RETRY_WAIT_SECONDS = 60


# names in VEEVA_PICKLIST_OBJECTS route to pull_veeva_picklist instead of
# pull_veeva_object — picklists aren't VQL-queryable and carry no
# modified_date__v, so neither the /query endpoint nor the watermark
# mechanism applies to them.
VEEVA_PICKLIST_OBJECTS = {
    "insight_question__c",
    "call2_discussion_discussion_topics__c",
    "address_state__v",
}


def pull_veeva_picklist(session_id, picklist_name, table_name, dest_folder, run_id, created_by):
    os.makedirs(dest_folder, exist_ok=True)
    url = f"{VEEVA_BASE_URL}/objects/picklists/{picklist_name}"

    body = None
    for attempt in range(1, VEEVA_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=veeva_auth_headers(session_id), timeout=VEEVA_REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            wait = min(2 ** attempt, VEEVA_MAX_RETRY_WAIT_SECONDS)
            print(f"  {picklist_name}: request failed ({e}), retrying in {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            body = resp.json()
            if body.get("responseStatus") != "SUCCESS":
                errors = body.get("errors", [])
                reason = errors[0].get("message") if errors else body
                raise RuntimeError(f"{picklist_name}: picklist fetch rejected: {reason}")
            break

        if resp.status_code == 401:
            session_id = get_veeva_session_id()
            continue

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            wait = min(2 ** attempt, VEEVA_MAX_RETRY_WAIT_SECONDS)
            time.sleep(wait)
            continue

        raise RuntimeError(f"{picklist_name}: unrecoverable status {resp.status_code}: {resp.text[:200]}")

    if body is None:
        raise RuntimeError(f"{picklist_name}: gave up after {VEEVA_MAX_RETRIES} attempts")

    # NOTE: key guessed below — confirm against a real response before
    # trusting this on anything beyond the first manual test run.
    entries = body.get("picklistValues", body.get("values", body.get("data", [])))
    print(f"  {picklist_name}: response top-level keys: {list(body.keys())}, {len(entries)} entries")

    landed_at = datetime.now(timezone.utc).isoformat()
    out_path = f"{dest_folder}/{picklist_name}.json"

    enriched = []
    for entry in entries:
        entry = dict(entry)
        entry["created_at"] = landed_at
        entry["updated_at"] = landed_at
        entry["created_by"] = created_by
        entry["is_active"] = True
        entry["run_id"] = run_id
        entry["source_file"] = out_path
        entry["object_api_name"] = picklist_name
        enriched.append(entry)

    atomic_write_json(out_path, enriched)
    return len(entries), session_id

# session auth — the raw session ID goes straight in the Authorization
# header, NOT "Bearer <token>" like SteepRock's OAuth flow
def get_veeva_session_id():
    resp = requests.post(
        VEEVA_AUTH_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        data={
            "username": dbutils.secrets.get(scope = "apnimed-dev-secret-scope", key = "veeva-api-id"),
            "password": dbutils.secrets.get(scope = "apnimed-dev-secret-scope", key = "veeva-api-password"),
        },
        timeout=VEEVA_REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    body = resp.json()
    if "sessionId" not in body:
        raise RuntimeError(f"No sessionId in auth response: {body}")
    return body["sessionId"]







def veeva_auth_headers(session_id):
    return {"Authorization": session_id, "Accept": "application/json"}





# NOTE: SELECT * here, not the hand-vetted field lists from
# veeva_vaultcrm_landing_pull.py — the mapping table only carries
# object_name -> table_name today, nowhere to hold a vetted SELECT list.
def build_vql_query(object_api_name):
    return f"SELECT * FROM {object_api_name}"


# write to a .tmp file, then rename into place — rename is atomic on the
# same filesystem, so a reader can never see a half-written page. If the
# process dies mid-write, only the .tmp file is corrupted; the real
# filename either doesn't exist yet or holds the previous complete write.
def atomic_write_json(file_path, data):
    tmp_path = f"{file_path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f)
    os.replace(tmp_path, file_path)



VEEVA_WATERMARK_SUBFOLDER = "_veeva_watermarks"


def get_watermark_path(db_volume, table_name):
    return f"{db_volume}/{VEEVA_WATERMARK_SUBFOLDER}/{table_name}.json"


def read_stored_watermark(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f).get("last_modified_watermark")


def write_stored_watermark(path, latest_modified, run_id):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    atomic_write_json(path, {
        "last_modified_watermark": latest_modified,
        "updated_run_id": run_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })


# cheap precheck — returns (latest_modified_date_or_None, session_id).
# Raises RuntimeError only for a genuinely unrecoverable/rejected query
# (e.g. object has no modified_date__v) — caller decides how to react.
def probe_latest_modified(session_id, object_api_name):
    query = f"SELECT id, modified_date__v FROM {object_api_name} ORDER BY modified_date__v DESC LIMIT 1"

    for attempt in range(1, VEEVA_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                VEEVA_QUERY_URL, headers=veeva_auth_headers(session_id),
                data={"q": query}, timeout=VEEVA_REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            wait = min(2 ** attempt, VEEVA_MAX_RETRY_WAIT_SECONDS)
            print(f"    {object_api_name} precheck: request failed ({e}), retrying in {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            session_id = get_veeva_session_id()
            continue

        if resp.status_code == 200:
            body = resp.json()
            if body.get("responseStatus") == "SUCCESS":
                rows = body.get("data", [])
                if not rows:
                    return None, session_id  # object is empty, nothing to compare
                return rows[0].get("modified_date__v"), session_id
            errors = body.get("errors", [])
            reason = errors[0].get("message") if errors else body
            raise RuntimeError(f"precheck query rejected: {reason}")

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            wait = min(2 ** attempt, VEEVA_MAX_RETRY_WAIT_SECONDS)
            time.sleep(wait)
            continue

        raise RuntimeError(f"precheck unrecoverable status {resp.status_code}: {resp.text[:200]}")

    raise RuntimeError(f"precheck exhausted {VEEVA_MAX_RETRIES} retries")


def pull_veeva_object(session_id, object_api_name, table_name, dest_folder, run_id, created_by):
    os.makedirs(dest_folder, exist_ok=True)

    url, method, payload = VEEVA_QUERY_URL, "POST", {"q": build_vql_query(object_api_name)}
    page_num, total_rows, reauth_attempts = 0, 0, 0

    while url:
        body = None
        gap = None

        for attempt in range(1, VEEVA_MAX_RETRIES + 1):
            try:
                resp = (
                    requests.post(url, headers=veeva_auth_headers(session_id), data=payload,
                                  timeout=VEEVA_REQUEST_TIMEOUT_SECONDS)
                    if method == "POST" else
                    requests.get(url, headers=veeva_auth_headers(session_id),
                                timeout=VEEVA_REQUEST_TIMEOUT_SECONDS)
                )
            except requests.RequestException as e:
                # timeout, connection reset, DNS blip — transient, back off and retry
                wait = min(2 ** attempt, VEEVA_MAX_RETRY_WAIT_SECONDS)
                print(f"  {object_api_name} page {page_num + 1}: request failed ({e}), "
                      f"retrying in {wait}s (attempt {attempt}/{VEEVA_MAX_RETRIES})")
                time.sleep(wait)
                continue

            if resp.status_code == 200:
                body = resp.json()
                break

            if resp.status_code == 401:
                reauth_attempts += 1
                if reauth_attempts > VEEVA_MAX_REAUTH_ATTEMPTS:
                    gap = {"reason": f"401 after {VEEVA_MAX_REAUTH_ATTEMPTS} re-auth attempts — "
                                      f"check credentials, not a transient session expiry"}
                    break
                session_id = get_veeva_session_id()
                continue  # retry the same page immediately with the new session

            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                # rate limited or server-side — transient, back off and retry
                wait = min(2 ** attempt, VEEVA_MAX_RETRY_WAIT_SECONDS)
                print(f"  {object_api_name} page {page_num + 1}: status {resp.status_code}, "
                      f"retrying in {wait}s (attempt {attempt}/{VEEVA_MAX_RETRIES})")
                time.sleep(wait)
                continue

            # 400/403/404/etc — retrying won't fix this, stop immediately
            gap = {"reason": f"unrecoverable status {resp.status_code}: {resp.text[:200]}"}
            break

        if gap is None and body is None:
            # exhausted every retry attempt without ever getting a 200
            gap = {"reason": f"gave up after {VEEVA_MAX_RETRIES} attempts (transient errors/timeouts)"}

        if gap:
            # record exactly where this object stopped — whatever landed in
            # pages 1..page_num is still on disk and still good; this file
            # is what stops that partial data from being mistaken for complete
            gap.update({
                "object": object_api_name,
                "url": url,
                "pages_landed": page_num,
                "rows_landed": total_rows,
                "run_id": run_id,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            })
            gap_path = (f"{dest_folder}/_gap_{object_api_name}_"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json")
            atomic_write_json(gap_path, gap)
            raise RuntimeError(
                f"{object_api_name}: stopped after {page_num} page(s)/{total_rows} row(s) — "
                f"{gap['reason']}. Gap recorded at {gap_path}"
            )

        page_num += 1
        rows = body.get("data", [])
        total_rows += len(rows)

        out_path = f"{dest_folder}/{object_api_name}_page{page_num:04d}.json"
        landed_at = datetime.now(timezone.utc).isoformat()

        # metadata columns only — created_at/updated_at, created_by, is_active,
        # run_id, source_file, plus page_num/object_api_name for queryability.
        # updated_at == created_at at this layer since landing writes each
        # row exactly once; it'll diverge once a downstream MERGE touches it.
        enriched_rows = []
        for row in rows:
            row = dict(row)
            row["created_at"] = landed_at
            row["updated_at"] = landed_at
            row["created_by"] = created_by
            row["is_active"] = True
            row["run_id"] = run_id
            row["source_file"] = out_path
            row["page_num"] = page_num
            row["object_api_name"] = object_api_name
            enriched_rows.append(row)

        atomic_write_json(out_path, enriched_rows)

        next_page = body.get("responseDetails", {}).get("next_page")
        url = f"{VEEVA_ROOT_DOMAIN}{next_page}" if next_page else None
        method, payload = "POST", None  # continuation pages are POST, no body

        time.sleep(VEEVA_REQUEST_DELAY_SECONDS)

    return page_num, total_rows, session_id

# COMMAND ----------

import paramiko
import os
import re
import stat
import uuid
import hashlib
from datetime import datetime, timezone
from pyspark.sql import functions as F

catalog = "apnimed_dw_dev"
metadata_schema = "metadata"
source_table = f"{catalog}.{metadata_schema}.metadata_source_details"
mapping_table = f"{catalog}.{metadata_schema}.metadata_sourcefile_to_table_mapping"
download_log_table = f"{catalog}.{metadata_schema}.metadata_file_download_log"
fingerprint_table = f"{catalog}.{metadata_schema}.metadata_source_item_fingerprint"
SFTP_SECRET_SCOPE = "sftp_scope"


def esc(s):
    return str(s).replace("'", "''") if s is not None else ""


def generate_run_id():
    return str(uuid.uuid4())


# cadence check 
def is_due_for_ingestion(last_ingested_at, cadence_days):
    if cadence_days is None:
        return True
    if last_ingested_at is None:
        return True
    days_since_last_run = (datetime.now(timezone.utc) - last_ingested_at.replace(tzinfo=timezone.utc)).days
    return days_since_last_run >= cadence_days

# get folder_file_pattern and table name mapping
def get_active_mappings(source_name):
    rows = (
        spark.table(mapping_table)
        .filter((F.col("source_name") == source_name) & (F.col("is_active") == True))
        .select("folder_file_pattern", "table_name")
        .collect()
    )
    return [(re.compile(r["folder_file_pattern"]), r["table_name"]) for r in rows]

# find table name from pattern
def resolve_table_name(item_name, compiled_mappings):
    for pattern, table_name in compiled_mappings:
        if pattern.match(item_name):
            return table_name
    return None

# constructe destination folder locaiton 
def derive_dest_folder(table_name, db_volume):
    return f"{db_volume}/{table_name}"


# add download started entry
def log_download_started(source_name, table_name, file_name, remote_path, local_path, run_id):
    started_at = datetime.now(timezone.utc)
    spark.sql(f"""
        INSERT INTO {download_log_table}
        (source_name, table_name, file_name, remote_path, local_path, status, started_at, run_id)
        VALUES (
            '{esc(source_name)}', '{esc(table_name)}', '{esc(file_name)}',
            '{esc(remote_path)}', '{esc(local_path)}', 'STARTED',
            TIMESTAMP'{started_at.isoformat()}', '{esc(run_id)}'
        )
    """)
    return started_at

# add download complete entry
def log_download_complete(source_name, table_name, file_name, run_id, started_at,
                           status, file_size_bytes=None, error_message=None):
    completed_at = datetime.now(timezone.utc)
    size_clause = f"{file_size_bytes}" if file_size_bytes is not None else "NULL"
    error_clause = f"'{esc(error_message)}'" if error_message else "NULL"
    spark.sql(f"""
        UPDATE {download_log_table}
        SET status = '{status}',
            completed_at = TIMESTAMP'{completed_at.isoformat()}',
            file_size_bytes = {size_clause},
            error_message = {error_clause}
        WHERE source_name = '{esc(source_name)}'
          AND table_name = '{esc(table_name)}'
          AND file_name = '{esc(file_name)}'
          AND run_id = '{esc(run_id)}'
          AND started_at = TIMESTAMP'{started_at.isoformat()}'
    """)


# SFTP/ADLS/size-aware APIs: fingerprint before download (skip if unchanged). Size-blind APIs: download first, fingerprint after, compare next run. (one per table per source)
def compute_fingerprint(items):
    joined = "|".join(f"{name}:{size}" for name, size in sorted(items))
    return hashlib.md5(joined.encode("utf-8")).hexdigest()

# get latest fingerprint of the table
def get_stored_fingerprint(source_name, table_name):
    rows = (
        spark.table(fingerprint_table)
        .filter((F.col("source_name") == source_name) & (F.col("table_name") == table_name))
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["fingerprint"] if rows else None

# update or insert fingerprint
def upsert_fingerprint(source_name, table_name, fingerprint):
    exists = (
        spark.table(fingerprint_table)
        .filter((F.col("source_name") == source_name) & (F.col("table_name") == table_name))
        .count() > 0
    )
    if exists:
        spark.sql(f"""
            UPDATE {fingerprint_table}
            SET fingerprint = '{esc(fingerprint)}',
                last_pulled_at = current_timestamp(),
                updated_at = current_timestamp()
            WHERE source_name = '{esc(source_name)}' AND table_name = '{esc(table_name)}'
        """)
    else:
        spark.sql(f"""
            INSERT INTO {fingerprint_table} (source_name, table_name, fingerprint, last_pulled_at)
            VALUES ('{esc(source_name)}', '{esc(table_name)}', '{esc(fingerprint)}', current_timestamp())
        """)

# create sftp connection
def connect_to_sftp(source):
    sftp_password = source["sftp_password"]
    transport = paramiko.Transport((source["sftp_host"], int(source["sftp_port"] or 22)))

    if "/" in sftp_password:
        private_key = paramiko.RSAKey.from_private_key_file(sftp_password)
        transport.connect(username=source["sftp_username"], pkey=private_key)
    else:
        password = dbutils.secrets.get(scope=SFTP_SECRET_SCOPE, key=sftp_password)
        transport.connect(username=source["sftp_username"], password=password)

    transport.set_keepalive(30)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport



# pull matching files from sftp
def pull_from_sftp(source, compiled_mappings, run_id):
    sftp, transport = connect_to_sftp(source)
    source_name = source["source_name"]
    db_volume = source["db_volume"]
    remote_dir = source["remote_dir"]

    try:
        entries = sftp.listdir_attr(remote_dir)
        table_items = {}
        unmapped = []

        for entry in entries:
            table_name = resolve_table_name(entry.filename, compiled_mappings)
            if table_name is None:
                unmapped.append(entry.filename)
                continue

            dest_folder = derive_dest_folder(table_name, db_volume)
            is_dir = stat.S_ISDIR(entry.st_mode)

            if is_dir:
                remote_subpath = f"{remote_dir}/{entry.filename}"
                for sub_entry in sftp.listdir_attr(remote_subpath):
                    if stat.S_ISREG(sub_entry.st_mode):
                        remote_file = f"{remote_subpath}/{sub_entry.filename}"
                        local_file = f"{dest_folder}/{sub_entry.filename}"
                        table_items.setdefault(table_name, []).append(
                            (remote_file, local_file, sub_entry.filename, sub_entry.st_size)
                        )
            else:
                remote_file = f"{remote_dir}/{entry.filename}"
                local_file = f"{dest_folder}/{entry.filename}"
                table_items.setdefault(table_name, []).append(
                    (remote_file, local_file, entry.filename, entry.st_size)
                )

        if unmapped:
            print(f"  UNMAPPED (skipped, not pulled): {unmapped}")

        pulled = []

        for table_name, items in table_items.items():
            new_fingerprint = compute_fingerprint([(name, size) for _, _, name, size in items])
            old_fingerprint = get_stored_fingerprint(source_name, table_name)

            if new_fingerprint == old_fingerprint:
                print(f"  {table_name}: unchanged since last pull — skipping ({len(items)} file(s))")
                continue

            dest_folder = derive_dest_folder(table_name, db_volume)
            os.makedirs(dest_folder, exist_ok=True)

            for remote_file, local_file, file_name, size in items:
                started_at = log_download_started(source_name, table_name, file_name, remote_file, local_file, run_id)
                try:
                    sftp.get(remote_file, local_file)
                    log_download_complete(source_name, table_name, file_name, run_id, started_at, "SUCCESS", file_size_bytes=size)
                    pulled.append((file_name, table_name))
                except Exception as e:
                    log_download_complete(source_name, table_name, file_name, run_id, started_at, "FAILED", error_message=str(e).splitlines()[0])
                    raise

            upsert_fingerprint(source_name, table_name, new_fingerprint)

        return pulled

    finally:
        sftp.close()
        transport.close()


# collect (path,filename,size) tuples for all files under the given path (recursive)
def list_adls_files_recursive(path):
    results = []
    stack = [path]
    while stack:
        current = stack.pop()
        for entry in dbutils.fs.ls(current):
            if entry.isDir():
                stack.append(entry.path)
            else:
                results.append((entry.path, entry.name, entry.size))
    return results


def pull_from_adls(source, compiled_mappings, run_id):
    source_name = source["source_name"]
    db_volume = source["db_volume"]
    adls_dir = source["adls_dir"]

    entries = dbutils.fs.ls(adls_dir)
    table_items = {}
    unmapped = []

    for entry in entries:
        item_name = entry.name.rstrip("/")
        table_name = resolve_table_name(item_name, compiled_mappings)
        if table_name is None:
            unmapped.append(item_name)
            continue

        dest_folder = derive_dest_folder(table_name, db_volume)

        if entry.isDir():
            for file_path, file_name, size in list_adls_files_recursive(entry.path):
                local_file = f"{dest_folder}/{file_name}"
                table_items.setdefault(table_name, []).append((file_path, local_file, file_name, size))
        else:
            local_file = f"{dest_folder}/{item_name}"
            table_items.setdefault(table_name, []).append((entry.path, local_file, item_name, entry.size))

    if unmapped:
        print(f"  UNMAPPED (skipped, not pulled): {unmapped}")

    pulled = []

    for table_name, items in table_items.items():
        new_fingerprint = compute_fingerprint([(name, size) for _, _, name, size in items])
        old_fingerprint = get_stored_fingerprint(source_name, table_name)

        if new_fingerprint == old_fingerprint:
            print(f"  {table_name}: unchanged since last pull — skipping ({len(items)} file(s))")
            continue

        dest_folder = derive_dest_folder(table_name, db_volume)
        dbutils.fs.mkdirs(dest_folder)

        for remote_path, local_file, file_name, size in items:
            started_at = log_download_started(source_name, table_name, file_name, remote_path, local_file, run_id)
            try:
                dbutils.fs.cp(remote_path, local_file)
                log_download_complete(source_name, table_name, file_name, run_id, started_at, "SUCCESS", file_size_bytes=size)
                pulled.append((file_name, table_name))
            except Exception as e:
                log_download_complete(source_name, table_name, file_name, run_id, started_at, "FAILED", error_message=str(e).splitlines()[0])
                raise

        upsert_fingerprint(source_name, table_name, new_fingerprint)

    return pulled


# pull matching files from api :- Shreeraj's API code will go here (delete this comment after task is done)
def pull_from_api(source, compiled_mappings, run_id):
    source_name = source["source_name"]
    objects_in_scope = [table_name for _, table_name in compiled_mappings]

    if source_name == "veeva_crm":
        print(f"  Veeva objects in scope ({len(objects_in_scope)}): {objects_in_scope}")

        session_id = get_veeva_session_id()
        created_by = spark.sql("SELECT current_user()").collect()[0][0]
        pulled = []
        failures = []

        for _, table_name in compiled_mappings:
            object_api_name = table_name
            dest_folder = derive_dest_folder(table_name, source["db_volume"])
            watermark_path = get_watermark_path(source["db_volume"], table_name)
            stored_watermark = read_stored_watermark(watermark_path)

            try:
                latest_modified, session_id = probe_latest_modified(session_id, object_api_name)
                precheck_ok = True
            except RuntimeError as e:
                print(f"    {object_api_name}: precheck failed ({e}) — falling back to full pull")
                latest_modified, precheck_ok = None, False

            # skip only when the precheck actually ran, the object has data,
            # AND nothing's changed since the stored watermark
            if precheck_ok and latest_modified is not None and stored_watermark is not None \
                    and latest_modified <= stored_watermark:
                print(f"    {object_api_name}: unchanged since {stored_watermark} — skipping")
                continue

            if precheck_ok and latest_modified is None:
                print(f"    {object_api_name}: object is empty — skipping")
                continue

            started_at = log_download_started(source_name, table_name, object_api_name, "API", dest_folder, run_id)
            try:
                pages, rows, session_id = pull_veeva_object(
                    session_id, object_api_name, table_name, dest_folder, run_id, created_by
                )
                log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "SUCCESS")
                print(f"    {object_api_name}: {pages} page(s), {rows} row(s)")
                pulled.append((object_api_name, table_name))

                # only advance the watermark on a fully successful pull —
                # if precheck failed and fell back to full pull, latest_modified
                # is None here too, so re-derive it from what was just landed
                if latest_modified is None:
                    latest_modified, session_id = probe_latest_modified(session_id, object_api_name)
                if latest_modified is not None:
                    write_stored_watermark(watermark_path, latest_modified, run_id)

            except Exception as e:
                error_message = str(e).splitlines()[0]
                log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "FAILED", error_message=error_message)
                print(f"    {object_api_name}: FAILED — {error_message}")
                failures.append((object_api_name, table_name, error_message))
                continue

        if failures:
            failed_summary = ", ".join(f"{obj}" for obj, _, _ in failures)
            raise RuntimeError(
                f"{len(failures)}/{len(compiled_mappings)} veeva_crm object(s) failed after retries: "
                f"{failed_summary}. {len(pulled)} object(s) landed successfully — see download log for per-object detail."
            )

        return pulled

    ##### STEEPROCK OLA AND OLMS THROUGH HERE, controlled by metadata_sourcefile_to_table_mapping. source_name ######################
    elif source_name == "steeprock_olms":
        print(f"  SteepRock OLMS objects in scope ({len(objects_in_scope)}): {objects_in_scope}")
        raise NotImplementedError("steeprock_olms API pull not yet implemented")

    elif source_name == "steeprock_ola":
        print(f"  SteepRock OLA objects in scope ({len(objects_in_scope)}): {objects_in_scope}")
        raise NotImplementedError("steeprock_ola API pull not yet implemented")

    else:
        raise ValueError(f"No API implementation defined for source: {source_name}")


def mark_source_ingested(source_id):
    spark.sql(f"""
        UPDATE {source_table}
        SET last_ingested_at = current_timestamp()
        WHERE source_id = {source_id}
    """)



# Main Function
def process_source(source):
    source_name = source["source_name"]
    cadence_days = source["cadence_days"]
    last_ingested_at = source["last_ingested_at"]

    if not is_due_for_ingestion(last_ingested_at, cadence_days):
        return ("SKIPPED_CADENCE", source_name, f"Not due yet (cadence={cadence_days} days, last run={last_ingested_at})")

    compiled_mappings = get_active_mappings(source_name)
    if not compiled_mappings:
        return ("SKIPPED_NO_MAPPING", source_name, "No active rows in metadata_sourcefile_to_table_mapping — nothing to pull")

    transmission_method = (source["transmission_method"] or "").strip().upper()
    run_id = generate_run_id()

    try:
        if transmission_method == "SFTP":
            pulled = pull_from_sftp(source, compiled_mappings, run_id)
        elif transmission_method == "ADLS":
            pulled = pull_from_adls(source, compiled_mappings, run_id)
        elif transmission_method == "API":
            pulled = pull_from_api(source, compiled_mappings, run_id)
        else:
            return ("FAILED", source_name, f"Unrecognized transmission_method: '{transmission_method}'")

        mark_source_ingested(source["source_id"])
        tables_pulled = sorted(set(t for _, t in pulled))
        return ("SUCCESS", source_name, f"run_id={run_id} | {len(pulled)} item(s) into {len(tables_pulled)} table(s): {tables_pulled}")

    except NotImplementedError as e:
        return ("NOT_IMPLEMENTED", source_name, str(e))
    except Exception as e:
        return ("FAILED", source_name, f"run_id={run_id} | {str(e).splitlines()[0]}")


active_sources = (
    spark.table(source_table)
    .filter(F.col("is_active") == True)
    .collect()
)

# calling the main function =
results = []
for row in active_sources:
    source = row.asDict()
    status, source_name, detail = process_source(source)
    print(f"{status:<20} | {source_name:<20} | {detail}")
    results.append((source_name, status, detail))

succeeded = [r for r in results if r[1] == "SUCCESS"]
skipped_cadence = [r for r in results if r[1] == "SKIPPED_CADENCE"]
skipped_no_mapping = [r for r in results if r[1] == "SKIPPED_NO_MAPPING"]
not_implemented = [r for r in results if r[1] == "NOT_IMPLEMENTED"]
failed = [r for r in results if r[1] == "FAILED"]

print("\n" + "=" * 70)
print("INGESTION RUN SUMMARY")
print("=" * 70)
print(f"Succeeded              : {len(succeeded)}")
print(f"Skipped (cadence)      : {len(skipped_cadence)}")
print(f"Skipped (no mapping)   : {len(skipped_no_mapping)}")
print(f"Not implemented        : {len(not_implemented)}")
print(f"Failed                   : {len(failed)}")

if failed:
    print("\nFailed — needs attention:")
    for source_name, _, detail in failed:
        print(f"  - {source_name}: {detail}")

# COMMAND ----------

pip install paramiko

# COMMAND ----------

# DBTITLE 1,Final
import paramiko
import os
import re
import stat
import time
import uuid
import hashlib
import zipfile
import gzip
import shutil
import tempfile
from datetime import datetime, timezone
from pyspark.sql import functions as F

catalog = "apnimed_dw_dev"
metadata_schema = "metadata"
source_table = f"{catalog}.{metadata_schema}.metadata_source_details"
mapping_table = f"{catalog}.{metadata_schema}.metadata_sourcefile_to_table_mapping"
download_log_table = f"{catalog}.{metadata_schema}.metadata_file_download_log"
fingerprint_table = f"{catalog}.{metadata_schema}.metadata_source_item_fingerprint"
SFTP_SECRET_SCOPE = "apnimed-dev-secret-scope"

MAX_DOWNLOAD_RETRIES = 5
RETRY_WAIT_SECONDS = 15
CHUNK_SIZE = 4 * 1024 * 1024


def esc(s):
    return str(s).replace("'", "''") if s is not None else ""


def generate_run_id():
    return str(uuid.uuid4())

# check ingestion due date
def is_due_for_ingestion(last_ingested_at, cadence_days):
    if cadence_days is None:
        print("Cadence is missing")
        return False
    if last_ingested_at is None:
        return True
    days_since_last_run = (datetime.now(timezone.utc) - last_ingested_at.replace(tzinfo=timezone.utc)).days
    return days_since_last_run >= cadence_days

# collect regix object along with their table name (returns compiled_mappings)
def get_active_mappings(source_name):
    rows = (
        spark.table(mapping_table)
        .filter((F.col("source_name") == source_name) & (F.col("is_active") == True))
        .select("folder_file_pattern", "table_name")
        .collect()
    )
    return [(re.compile(r["folder_file_pattern"]), r["table_name"]) for r in rows]

# give it pattern name of a table it will match from the list of mappings and return corresponding table name 
def resolve_table_name(item_name, compiled_mappings):
    for pattern, table_name in compiled_mappings:
        if pattern.match(item_name):
            return table_name
    return None

# returns path of databricks volume where to paste the source files
def derive_dest_folder(table_name, db_volume, date_str):
    return f"{db_volume}/{date_str}/{table_name}"

# extract the file zip or gzip files and return the list of path of files extracted 
def extract_compressed_file(local_compressed_path, extract_dir, compression_type):
    os.makedirs(extract_dir, exist_ok=True)
    compression_type = (compression_type or "").strip().lower()

    if compression_type == "zip":
        with zipfile.ZipFile(local_compressed_path, "r") as zf:
            zf.extractall(extract_dir)
        extracted_files = []
        for root, _, files in os.walk(extract_dir):
            for fname in files:
                extracted_files.append(os.path.join(root, fname))
        return extracted_files

    elif compression_type == "gzip":
        base_name = os.path.basename(local_compressed_path)
        output_name = base_name[:-3] if base_name.lower().endswith(".gz") else f"{base_name}.decompressed"
        output_path = os.path.join(extract_dir, output_name)
        with gzip.open(local_compressed_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        return [output_path]

    else:
        raise ValueError(f"Unsupported compression_type: '{compression_type}' (expected 'zip' or 'gzip')")

# update the log table once the download has started
def log_download_started(source_name, table_name, file_name, remote_path, local_path, run_id):
    started_at = datetime.now(timezone.utc)
    spark.sql(f"""
        INSERT INTO {download_log_table}
        (source_name, table_name, file_name, remote_path, local_path, status, started_at, run_id)
        VALUES (
            '{esc(source_name)}', '{esc(table_name)}', '{esc(file_name)}',
            '{esc(remote_path)}', '{esc(local_path)}', 'STARTED',
            TIMESTAMP'{started_at.isoformat()}', '{esc(run_id)}'
        )
    """)
    return started_at

# update the log table one the download has been completed
def log_download_complete(source_name, table_name, file_name, run_id, started_at,
                           status, file_size_bytes=None, error_message=None):
    completed_at = datetime.now(timezone.utc)
    size_clause = f"{file_size_bytes}" if file_size_bytes is not None else "NULL"
    error_clause = f"'{esc(error_message)}'" if error_message else "NULL"
    spark.sql(f"""
        UPDATE {download_log_table}
        SET status = '{status}',
            completed_at = TIMESTAMP'{completed_at.isoformat()}',
            file_size_bytes = {size_clause},
            error_message = {error_clause}
        WHERE source_name = '{esc(source_name)}'
          AND table_name = '{esc(table_name)}'
          AND file_name = '{esc(file_name)}'
          AND run_id = '{esc(run_id)}'
          AND started_at = TIMESTAMP'{started_at.isoformat()}'
    """)

# calculate the fingerprint to skip re-download of same files 
def compute_fingerprint(items):
    joined = "|".join(f"{name}:{size}" for name, size in sorted(items))
    return hashlib.md5(joined.encode("utf-8")).hexdigest()

# get the latest fingerprint for table of given source
def get_stored_fingerprint(source_name, table_name):
    rows = (
        spark.table(fingerprint_table)
        .filter((F.col("source_name") == source_name) & (F.col("table_name") == table_name))
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["fingerprint"] if rows else None

# create or update fingerprint, to keep the latest fingerprint for each table of each source
def upsert_fingerprint(source_name, table_name, fingerprint):
    exists = (
        spark.table(fingerprint_table)
        .filter((F.col("source_name") == source_name) & (F.col("table_name") == table_name))
        .count() > 0
    )
    if exists:
        spark.sql(f"""
            UPDATE {fingerprint_table}
            SET fingerprint = '{esc(fingerprint)}',
                last_pulled_at = current_timestamp(),
                updated_at = current_timestamp()
            WHERE source_name = '{esc(source_name)}' AND table_name = '{esc(table_name)}'
        """)
    else:
        spark.sql(f"""
            INSERT INTO {fingerprint_table} (source_name, table_name, fingerprint, last_pulled_at)
            VALUES ('{esc(source_name)}', '{esc(table_name)}', '{esc(fingerprint)}', current_timestamp())
        """)

# make connection to sftp
def connect_to_sftp(source):
    sftp_host = dbutils.secrets.get(scope=SFTP_SECRET_SCOPE, key=source["sftp_host"])
    sftp_port = dbutils.secrets.get(scope=SFTP_SECRET_SCOPE, key=source["sftp_port"])
    sftp_username = dbutils.secrets.get(scope=SFTP_SECRET_SCOPE, key=source["sftp_username"])
    sftp_password_key = source["sftp_password"]

    transport = paramiko.Transport((sftp_host, int(sftp_port)))

    if "/" in sftp_password_key:
        private_key = paramiko.RSAKey.from_private_key_file(sftp_password_key)
        transport.connect(username=sftp_username, pkey=private_key)
    else:
        password = dbutils.secrets.get(scope=SFTP_SECRET_SCOPE, key=sftp_password_key)
        transport.connect(username=sftp_username, password=password)

    transport.set_keepalive(30)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


# download logic for sftp -> It has a retry logic, it picks up from the last byte downlaoded and resumes downlaod
def download_with_retry(source, remote_file, local_path, expected_size=None):
    last_error = None

    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        sftp = None
        transport = None
        try:
            existing_bytes = os.path.getsize(local_path) if os.path.exists(local_path) else 0

            if existing_bytes > 0:
                print(f"    Attempt {attempt}/{MAX_DOWNLOAD_RETRIES}: resuming '{os.path.basename(local_path)}' from {existing_bytes:,} bytes")
            else:
                print(f"    Attempt {attempt}/{MAX_DOWNLOAD_RETRIES}: starting '{os.path.basename(local_path)}' fresh")

            sftp, transport = connect_to_sftp(source)

            with sftp.open(remote_file, "rb") as remote_f:
                if existing_bytes > 0:
                    remote_f.seek(existing_bytes)

                with open(local_path, "ab" if existing_bytes > 0 else "wb") as local_f:
                    transferred = existing_bytes
                    while True:
                        chunk = remote_f.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        local_f.write(chunk)
                        transferred += len(chunk)

            final_size = os.path.getsize(local_path)

            if expected_size is not None and final_size != expected_size:
                print(f"    Size mismatch after attempt {attempt}: got {final_size:,}, expected {expected_size:,} — retrying")
                last_error = f"Size mismatch: {final_size} != {expected_size}"
                continue

            print(f"    Complete: {local_path} ({final_size:,} bytes)")
            return

        except Exception as e:
            last_error = str(e)
            print(f"    Attempt {attempt}/{MAX_DOWNLOAD_RETRIES} failed: {e}")

            if "garbage" in str(e).lower() and os.path.exists(local_path):
                print(f"    Corrupt partial file detected — discarding for a clean retry")
                os.remove(local_path)

            if attempt < MAX_DOWNLOAD_RETRIES:
                print(f"    Waiting {RETRY_WAIT_SECONDS}s before retry...")
                time.sleep(RETRY_WAIT_SECONDS)

        finally:
            if sftp:
                try:
                    sftp.close()
                except Exception:
                    pass
            if transport:
                try:
                    transport.close()
                except Exception:
                    pass

    if os.path.exists(local_path):
        os.remove(local_path)

    raise RuntimeError(f"Failed to download '{remote_file}' after {MAX_DOWNLOAD_RETRIES} attempts: {last_error}")


# Downloads a single (non-container ie uncompressed) item from SFTP.
def download_sftp_item(source, remote_file, remote_size, dest_folder, file_compressed, compression_type,
                        source_name, table_name, run_id):
    file_name = os.path.basename(remote_file) # eg:- "/inbound/LAD_568345_LM35500_DXCD_M20260827/part1.parquet"
    # I was here last time 

    if not file_compressed:
        local_file = f"{dest_folder}/{file_name}"
        started_at = log_download_started(source_name, table_name, file_name, remote_file, local_file, run_id)
        try:
            download_with_retry(source, remote_file, local_file, expected_size=remote_size)
            size = os.path.getsize(local_file)
            log_download_complete(source_name, table_name, file_name, run_id, started_at, "SUCCESS", file_size_bytes=size)
            return [(file_name, size)]
        except Exception as e:
            log_download_complete(source_name, table_name, file_name, run_id, started_at, "FAILED", error_message=str(e).splitlines()[0])
            raise

    # this part handles when each file of table comes as zip eg:- ^hce_detl_mast\.zip$
    temp_dir = tempfile.mkdtemp(prefix="ingest_")
    temp_compressed_path = os.path.join(temp_dir, file_name)

    started_at = log_download_started(source_name, table_name, file_name, remote_file, temp_compressed_path, run_id)
    try:
        download_with_retry(source, remote_file, temp_compressed_path, expected_size=remote_size)

        extract_dir = os.path.join(temp_dir, "extracted")
        extracted_paths = extract_compressed_file(temp_compressed_path, extract_dir, compression_type)

        os.makedirs(dest_folder, exist_ok=True)
        landed = []
        for extracted_path in extracted_paths:
            extracted_name = os.path.basename(extracted_path)
            final_path = f"{dest_folder}/{extracted_name}"
            shutil.copy2(extracted_path, final_path)
            landed.append((extracted_name, os.path.getsize(final_path)))

        log_download_complete(source_name, table_name, file_name, run_id, started_at, "SUCCESS",
                               file_size_bytes=os.path.getsize(temp_compressed_path))
        return landed

    except Exception as e:
        log_download_complete(source_name, table_name, file_name, run_id, started_at, "FAILED",
                               error_message=str(e).splitlines()[0])
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# Handles a compressed item that doesn't match any pattern on its own
def handle_container_archive(source, remote_file, remote_size, db_volume, date_str, compression_type,
                              compiled_mappings, source_name, run_id):
    file_name = os.path.basename(remote_file)
    temp_dir = tempfile.mkdtemp(prefix="ingest_")
    temp_compressed_path = os.path.join(temp_dir, file_name)

    started_at = log_download_started(source_name, "__container__", file_name, remote_file, temp_compressed_path, run_id)
    pulled = []
    still_unmapped = []

    try:
        download_with_retry(source, remote_file, temp_compressed_path, expected_size=remote_size)

        extract_dir = os.path.join(temp_dir, "extracted")
        extracted_paths = extract_compressed_file(temp_compressed_path, extract_dir, compression_type)

        resolved_by_table = {}
        for extracted_path in extracted_paths:
            extracted_name = os.path.basename(extracted_path)
            table_name = resolve_table_name(extracted_name, compiled_mappings)
            # for things like hce_detl_mast/part1.txt inside zip
            if table_name is None:
                rel_path = os.path.relpath(extracted_path, extract_dir)
                parent_folder = rel_path.split(os.sep)[0] if os.sep in rel_path else None
                if parent_folder:
                    table_name = resolve_table_name(parent_folder, compiled_mappings)
            if table_name is None:
                still_unmapped.append(extracted_name)
                continue
            resolved_by_table.setdefault(table_name, []).append(extracted_path)

        if still_unmapped:
            print(f"  UNMAPPED after extraction (skipped, not landed): {still_unmapped}")

        for table_name, paths in resolved_by_table.items():
            dest_folder = derive_dest_folder(table_name, db_volume, date_str)
            os.makedirs(dest_folder, exist_ok=True)

            items_for_fingerprint = []
            for extracted_path in paths:
                extracted_name = os.path.basename(extracted_path)
                final_path = f"{dest_folder}/{extracted_name}"
                shutil.copy2(extracted_path, final_path)
                size = os.path.getsize(final_path)
                items_for_fingerprint.append((extracted_name, size))
                pulled.append((extracted_name, table_name))

            new_fingerprint = compute_fingerprint(items_for_fingerprint)
            upsert_fingerprint(source_name, table_name, new_fingerprint)

        log_download_complete(source_name, "__container__", file_name, run_id, started_at, "SUCCESS",
                               file_size_bytes=os.path.getsize(temp_compressed_path))
        return pulled

    except Exception as e:
        log_download_complete(source_name, "__container__", file_name, run_id, started_at, "FAILED",
                               error_message=str(e).splitlines()[0])
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def pull_from_sftp(source, compiled_mappings, run_id, date_str):
    sftp, transport = connect_to_sftp(source)
    source_name = source["source_name"]
    db_volume = source["db_volume"]
    remote_dir = source["remote_dir"]
    file_compressed = bool(source.get("file_compressed"))
    compression_type = source.get("compression_type")

    try:
        entries = sftp.listdir_attr(remote_dir)
        table_items = {}
        container_candidates = []
        unmapped = []

        for entry in entries:
            table_name = resolve_table_name(entry.filename, compiled_mappings)

            if table_name is None:
                if file_compressed:
                    container_candidates.append(entry)
                else:
                    unmapped.append(entry.filename)
                continue

            is_dir = stat.S_ISDIR(entry.st_mode)

            if is_dir:
                remote_subpath = f"{remote_dir}/{entry.filename}"
                for sub_entry in sftp.listdir_attr(remote_subpath):
                    if stat.S_ISREG(sub_entry.st_mode):
                        remote_file = f"{remote_subpath}/{sub_entry.filename}"
                        table_items.setdefault(table_name, []).append(
                            (remote_file, sub_entry.filename, sub_entry.st_size)
                        )
            else:
                remote_file = f"{remote_dir}/{entry.filename}"
                table_items.setdefault(table_name, []).append(
                    (remote_file, entry.filename, entry.st_size)
                )

    finally:
        sftp.close()
        transport.close()

    pulled = []
    skipped_unchanged = []
    failed_tables = []

    for table_name, items in table_items.items():
        new_fingerprint = compute_fingerprint([(name, size) for _, name, size in items])
        old_fingerprint = get_stored_fingerprint(source_name, table_name)

        if new_fingerprint == old_fingerprint:
            print(f"  {table_name}: unchanged since last pull — skipping ({len(items)} file(s))")
            skipped_unchanged.append(table_name)
            continue

        dest_folder = derive_dest_folder(table_name, db_volume, date_str)
        os.makedirs(dest_folder, exist_ok=True)

        try:
            for remote_file, file_name, size in items:
                landed = download_sftp_item(
                    source, remote_file, size, dest_folder, file_compressed, compression_type,
                    source_name, table_name, run_id
                )
                for landed_name, landed_size in landed:
                    pulled.append((landed_name, table_name))
            upsert_fingerprint(source_name, table_name, new_fingerprint)
        except Exception as e:
            print(f"  Download failed for table '{table_name}': {e}")
            failed_tables.append(table_name)

    for entry in container_candidates:
        remote_file = f"{remote_dir}/{entry.filename}"
        try:
            landed = handle_container_archive(
                source, remote_file, entry.st_size, db_volume, date_str, compression_type,
                compiled_mappings, source_name, run_id
            )
            pulled.extend(landed)
            if not landed:
                unmapped.append(entry.filename)
        except Exception as e:
            print(f"  Container extraction failed for '{entry.filename}': {e}")
            failed_tables.append(entry.filename)

    if unmapped:
        print(f"  UNMAPPED (skipped, not pulled): {unmapped}")

    return {
        "pulled": pulled,
        "skipped_unchanged": skipped_unchanged,
        "failed_tables": failed_tables,
    }


def list_adls_files_recursive(path):
    results = []
    stack = [path]
    while stack:
        current = stack.pop()
        for entry in dbutils.fs.ls(current):
            if entry.isDir():
                stack.append(entry.path)
            else:
                results.append((entry.path, entry.name, entry.size))
    return results


def download_adls_item(remote_path, dest_folder, file_compressed, compression_type,
                        source_name, table_name, run_id):
    file_name = os.path.basename(remote_path)

    if not file_compressed:
        local_file = f"{dest_folder}/{file_name}"
        started_at = log_download_started(source_name, table_name, file_name, remote_path, local_file, run_id)
        try:
            dbutils.fs.cp(remote_path, local_file)
            size = dbutils.fs.ls(local_file)[0].size
            log_download_complete(source_name, table_name, file_name, run_id, started_at, "SUCCESS", file_size_bytes=size)
            return [(file_name, size)]
        except Exception as e:
            log_download_complete(source_name, table_name, file_name, run_id, started_at, "FAILED", error_message=str(e).splitlines()[0])
            raise

    temp_dir = tempfile.mkdtemp(prefix="ingest_")
    temp_compressed_path = os.path.join(temp_dir, file_name)

    started_at = log_download_started(source_name, table_name, file_name, remote_path, temp_compressed_path, run_id)
    try:
        dbutils.fs.cp(remote_path, f"file:{temp_compressed_path}")

        extract_dir = os.path.join(temp_dir, "extracted")
        extracted_paths = extract_compressed_file(temp_compressed_path, extract_dir, compression_type)

        dbutils.fs.mkdirs(dest_folder)
        landed = []
        for extracted_path in extracted_paths:
            extracted_name = os.path.basename(extracted_path)
            final_path = f"{dest_folder}/{extracted_name}"
            dbutils.fs.cp(f"file:{extracted_path}", final_path)
            landed_size = dbutils.fs.ls(final_path)[0].size
            landed.append((extracted_name, landed_size))

        log_download_complete(source_name, table_name, file_name, run_id, started_at, "SUCCESS",
                               file_size_bytes=os.path.getsize(temp_compressed_path))
        return landed

    except Exception as e:
        log_download_complete(source_name, table_name, file_name, run_id, started_at, "FAILED",
                               error_message=str(e).splitlines()[0])
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def pull_from_adls(source, compiled_mappings, run_id, date_str):
    source_name = source["source_name"]
    db_volume = source["db_volume"]
    adls_dir = source["adls_dir"]
    file_compressed = bool(source.get("file_compressed"))
    compression_type = source.get("compression_type")

    entries = dbutils.fs.ls(adls_dir)
    table_items = {}
    unmapped = []

    for entry in entries:
        item_name = entry.name.rstrip("/")
        table_name = resolve_table_name(item_name, compiled_mappings)
        if table_name is None:
            unmapped.append(item_name)
            continue

        if entry.isDir():
            for file_path, file_name, size in list_adls_files_recursive(entry.path):
                table_items.setdefault(table_name, []).append((file_path, file_name, size))
        else:
            table_items.setdefault(table_name, []).append((entry.path, item_name, entry.size))

    if unmapped:
        print(f"  UNMAPPED (skipped, not pulled): {unmapped}")

    pulled = []
    skipped_unchanged = []
    failed_tables = []

    for table_name, items in table_items.items():
        new_fingerprint = compute_fingerprint([(name, size) for _, name, size in items])
        old_fingerprint = get_stored_fingerprint(source_name, table_name)

        if new_fingerprint == old_fingerprint:
            print(f"  {table_name}: unchanged since last pull — skipping ({len(items)} file(s))")
            skipped_unchanged.append(table_name)
            continue

        dest_folder = derive_dest_folder(table_name, db_volume, date_str)
        dbutils.fs.mkdirs(dest_folder)

        try:
            for remote_path, file_name, size in items:
                landed = download_adls_item(
                    remote_path, dest_folder, file_compressed, compression_type,
                    source_name, table_name, run_id
                )
                for landed_name, landed_size in landed:
                    pulled.append((landed_name, table_name))
            upsert_fingerprint(source_name, table_name, new_fingerprint)
        except Exception as e:
            print(f"  Download failed for table '{table_name}': {e}")
            failed_tables.append(table_name)

    return {
        "pulled": pulled,
        "skipped_unchanged": skipped_unchanged,
        "failed_tables": failed_tables,
    }


def pull_from_api(source, compiled_mappings, run_id):
    source_name = source["source_name"]
    objects_in_scope = [table_name for _, table_name in compiled_mappings]
#################################################################
    if source_name == "veeva_crm":
        print(f"  Veeva objects in scope ({len(objects_in_scope)}): {objects_in_scope}")

        session_id = get_veeva_session_id()
        created_by = spark.sql("SELECT current_user()").collect()[0][0]
        pulled = []
        failures = []

        for _, table_name in compiled_mappings:
            object_api_name = table_name
            dest_folder = derive_dest_folder(table_name, source["db_volume"])
            started_at = log_download_started(source_name, table_name, object_api_name, "API", dest_folder, run_id)

            try:
                if object_api_name in VEEVA_PICKLIST_OBJECTS:
                    # no modified_date__v, no watermark — always a full fetch
                    row_count, session_id = pull_veeva_picklist(
                        session_id, object_api_name, table_name, dest_folder, run_id, created_by
                    )
                    log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "SUCCESS")
                    print(f"    {object_api_name} (picklist): {row_count} entries")
                    pulled.append((object_api_name, table_name))
                    continue

                # ---- existing watermark-gated object pull, unchanged ----
                watermark_path = get_watermark_path(source["db_volume"], table_name)
                stored_watermark = read_stored_watermark(watermark_path)

                try:
                    latest_modified, session_id = probe_latest_modified(session_id, object_api_name)
                    precheck_ok = True
                except RuntimeError as e:
                    print(f"    {object_api_name}: precheck failed ({e}) — falling back to full pull")
                    latest_modified, precheck_ok = None, False

                if precheck_ok and latest_modified is not None and stored_watermark is not None \
                        and latest_modified <= stored_watermark:
                    print(f"    {object_api_name}: unchanged since {stored_watermark} — skipping")
                    log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "SUCCESS")
                    continue

                if precheck_ok and latest_modified is None:
                    print(f"    {object_api_name}: object is empty — skipping")
                    log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "SUCCESS")
                    continue

                pages, rows, session_id = pull_veeva_object(
                    session_id, object_api_name, table_name, dest_folder, run_id, created_by
                )
                log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "SUCCESS")
                print(f"    {object_api_name}: {pages} page(s), {rows} row(s)")
                pulled.append((object_api_name, table_name))

                if latest_modified is None:
                    latest_modified, session_id = probe_latest_modified(session_id, object_api_name)
                if latest_modified is not None:
                    write_stored_watermark(watermark_path, latest_modified, run_id)

            except Exception as e:
                error_message = str(e).splitlines()[0]
                log_download_complete(source_name, table_name, object_api_name, run_id, started_at, "FAILED", error_message=error_message)
                print(f"    {object_api_name}: FAILED — {error_message}")
                failures.append((object_api_name, table_name, error_message))
                continue

        if failures:
            failed_summary = ", ".join(f"{obj}" for obj, _, _ in failures)
            raise RuntimeError(
                f"{len(failures)}/{len(compiled_mappings)} veeva_crm object(s) failed after retries: "
                f"{failed_summary}. {len(pulled)} object(s) landed successfully — see download log for per-object detail."
            )

        return pulled
#####################################
    elif source_name == "steeprock_ola":
        print(f"  SteepRock OLA objects in scope ({len(objects_in_scope)}): {objects_in_scope}")

        set_steeprock_ola_path(source["db_volume"])
        created_by = spark.sql("SELECT current_user()").collect()[0][0]
        run_date = datetime.now(timezone.utc).strftime("%m%d%Y")
        log_entries = []
        pulled = []
        failures = []

        for _, table_name in compiled_mappings:
            pull_fn = STEEPROCK_OLA_DISPATCH.get(table_name)
            if pull_fn is None:
                msg = f"no pull function defined for table_name '{table_name}'"
                print(f"    {table_name}: {msg} — skipping")
                failures.append((table_name, table_name, msg))
                continue

            # informational only — the reference script's own flush_batch
            # writes the real path; this mirrors it for the download log
            dest_folder = f"{source['db_volume']}/{API_DOMAIN}/{run_date}/{table_name}"
            started_at = log_download_started(source_name, table_name, table_name, "API", dest_folder, run_id)

            try:
                result = pull_fn(run_id, log_entries, created_by)
                write_object_manifest(result["object"], result, run_id)

                if result.get("gaps"):
                    # partial landing, not a clean success — surface it
                    # honestly rather than reporting SUCCESS on incomplete data
                    log_download_complete(
                        source_name, table_name, table_name, run_id, started_at,
                        "SUCCESS_WITH_GAPS",
                        error_message=f"{len(result['gaps'])} gap(s) — {result.get('completeness_status')}",
                    )
                    print(f"    {table_name}: landed with gaps — {result.get('completeness_status')}")
                    failures.append((table_name, table_name, result.get("completeness_status")))
                    continue

                log_download_complete(source_name, table_name, table_name, run_id, started_at, "SUCCESS")
                print(f"    {table_name}: {result['total_landed']} rows landed. {result.get('completeness_status')}")
                pulled.append((table_name, table_name))

            except Exception as e:
                error_message = str(e).splitlines()[0]
                log_download_complete(source_name, table_name, table_name, run_id, started_at, "FAILED", error_message=error_message)
                print(f"    {table_name}: FAILED — {error_message}")
                failures.append((table_name, table_name, error_message))
                continue

        flush_log(log_entries)

        if failures:
            failed_summary = ", ".join(f"{obj}" for obj, _, _ in failures)
            raise RuntimeError(
                f"{len(failures)}/{len(compiled_mappings)} steeprock_ola object(s) failed or landed with gaps: "
                f"{failed_summary}. {len(pulled)} object(s) landed cleanly — see download log, "
                f"{LOG_TABLE}, and gap files under {source['db_volume']}/{API_DOMAIN}/error/ for detail."
            )

        return pulled
###############################################
    elif source_name == "steeprock_olms":
        print(f"  SteepRock OLMS objects in scope ({len(objects_in_scope)}): {objects_in_scope}")

        set_steeprock_olms_path(source["db_volume"])
        headers = get_auth_headers()  # Basic Auth, no expiry — fetched once, reused across every object
        created_by = spark.sql("SELECT current_user()").collect()[0][0]
        run_date = datetime.now(timezone.utc).strftime("%m%d%Y")
        log_entries = []
        pulled = []
        failures = []

        for _, table_name in compiled_mappings:
            cfg = OBJECT_REGISTRY.get(table_name)
            if cfg is None:
                msg = f"no endpoint registered for table_name '{table_name}'"
                print(f"    {table_name}: {msg} — skipping")
                failures.append((table_name, table_name, msg))
                continue

            dest_folder = f"{source['db_volume']}/{API_DOMAIN}/{run_date}/{table_name}"
            started_at = log_download_started(source_name, table_name, table_name, "API", dest_folder, run_id)

            try:
                result = pull_object(table_name, cfg["url"], headers, run_id, log_entries, created_by)
                write_object_manifest(result["object"], result, run_id)

                if result.get("gaps"):
                    log_download_complete(
                        source_name, table_name, table_name, run_id, started_at,
                        "SUCCESS_WITH_GAPS",
                        error_message=f"{len(result['gaps'])} gap(s) — {result.get('completeness_status')}",
                    )
                    print(f"    {table_name}: landed with gaps — {result.get('completeness_status')}")
                    failures.append((table_name, table_name, result.get("completeness_status")))
                    continue

                log_download_complete(source_name, table_name, table_name, run_id, started_at, "SUCCESS")
                print(f"    {table_name}: {result['total_landed']} rows landed. {result.get('completeness_status')}")
                pulled.append((table_name, table_name))

            except Exception as e:
                error_message = str(e).splitlines()[0]
                log_download_complete(source_name, table_name, table_name, run_id, started_at, "FAILED", error_message=error_message)
                print(f"    {table_name}: FAILED — {error_message}")
                failures.append((table_name, table_name, error_message))
                continue

        flush_log(log_entries)

        if failures:
            failed_summary = ", ".join(f"{obj}" for obj, _, _ in failures)
            raise RuntimeError(
                f"{len(failures)}/{len(compiled_mappings)} steeprock_olms object(s) failed or landed with gaps: "
                f"{failed_summary}. {len(pulled)} object(s) landed cleanly — see download log, "
                f"{LOG_TABLE}, and gap files under {source['db_volume']}/{API_DOMAIN}/error/ for detail."
            )

        return pulled
    else:
        raise ValueError(f"No API implementation defined for source: {source_name}")

##################################################################
def mark_source_ingested(source_id):
    spark.sql(f"""
        UPDATE {source_table}
        SET last_ingested_at = current_timestamp()
        WHERE source_id = {source_id}
    """)


def process_source(source):
    source_name = source["source_name"]
    cadence_days = source["cadence_days"]
    last_ingested_at = source["last_ingested_at"]

    if not is_due_for_ingestion(last_ingested_at, cadence_days):
        return ("SKIPPED_CADENCE", source_name, f"Not due yet (cadence={cadence_days} days, last run={last_ingested_at})")

    compiled_mappings = get_active_mappings(source_name)
    if not compiled_mappings:
        return ("SKIPPED_NO_MAPPING", source_name, "No active rows in metadata_sourcefile_to_table_mapping — nothing to pull")

    transmission_method = (source["transmission_method"] or "").strip().upper()
    run_id = generate_run_id()
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    try:
        if transmission_method == "SFTP":
            result = pull_from_sftp(source, compiled_mappings, run_id, date_str)
        elif transmission_method == "ADLS":
            result = pull_from_adls(source, compiled_mappings, run_id, date_str)
        elif transmission_method == "API":
            result = pull_from_api(source, compiled_mappings, run_id, date_str)
        else:
            return ("FAILED", source_name, f"Unrecognized transmission_method: '{transmission_method}'")

        pulled = result["pulled"]
        skipped_unchanged = result["skipped_unchanged"]
        failed_tables = result["failed_tables"]

        if failed_tables:
            return ("FAILED", source_name, f"run_id={run_id} | {len(failed_tables)} table(s) failed: {failed_tables}")

        if not pulled and not skipped_unchanged:
            return ("FAILED", source_name, f"run_id={run_id} | 0 items pulled and 0 legitimately skipped — check logs above")

        mark_source_ingested(source["source_id"])
        tables_pulled = sorted(set(t for _, t in pulled))
        detail = f"run_id={run_id} | {len(pulled)} item(s) into {len(tables_pulled)} table(s) pulled"
        if skipped_unchanged:
            detail += f" | {len(skipped_unchanged)} table(s) unchanged, skipped"
        return ("SUCCESS", source_name, detail)

    except NotImplementedError as e:
        return ("NOT_IMPLEMENTED", source_name, str(e))
    except Exception as e:
        return ("FAILED", source_name, f"run_id={run_id} | {str(e).splitlines()[0]}")


active_sources = (
    spark.table(source_table)
    .filter(F.col("is_active") == True)
    .collect()
)

results = []
for row in active_sources:
    source = row.asDict()
    status, source_name, detail = process_source(source)
    print(f"{status:<20} | {source_name:<20} | {detail}")
    results.append((source_name, status, detail))

succeeded = [r for r in results if r[1] == "SUCCESS"]
skipped_cadence = [r for r in results if r[1] == "SKIPPED_CADENCE"]
skipped_no_mapping = [r for r in results if r[1] == "SKIPPED_NO_MAPPING"]
not_implemented = [r for r in results if r[1] == "NOT_IMPLEMENTED"]
failed = [r for r in results if r[1] == "FAILED"]

print("\n" + "=" * 70)
print("INGESTION RUN SUMMARY")
print("=" * 70)
print(f"Succeeded              : {len(succeeded)}")
print(f"Skipped (cadence)      : {len(skipped_cadence)}")
print(f"Skipped (no mapping)   : {len(skipped_no_mapping)}")
print(f"Not implemented        : {len(not_implemented)}")
print(f"Failed                   : {len(failed)}")

if failed:
    print("\nFailed — needs attention:")
    for source_name, _, detail in failed:
        print(f"  - {source_name}: {detail}")