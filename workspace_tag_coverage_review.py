# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Tag-Coverage Review (Read-Only)
# MAGIC
# MAGIC Companion to `workspace_inventory_and_tagging.py`. This notebook **only reads** — no
# MAGIC `update()` / `edit()` / `ALTER ... SET TAGS` calls anywhere. Safe to run on any
# MAGIC workspace, including production.
# MAGIC
# MAGIC What it produces:
# MAGIC 1. **Executive summary** — one line per asset type with tag-coverage %
# MAGIC 2. **Per-asset inventory** with tags inline (jobs, clusters, warehouses, dashboards)
# MAGIC 3. **Missing-required-tags** detail for whatever tag keys you mark as required
# MAGIC 4. **Tag value distribution** — top values per tag key, to surface typos/drift
# MAGIC 5. **Unity Catalog tag coverage** — table-level via `system.information_schema`
# MAGIC
# MAGIC Tweak the `REQUIRED_TAGS` and `TARGET_CATALOGS` config below to match your governance standard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

# Tag keys you expect every taggable asset to carry. Used for the coverage report.
REQUIRED_TAGS = ["team", "env", "cost_center"]

# UC catalogs to include in the table-tag coverage scan.
# Default ["*"] scans every catalog visible to you (Databricks-internal catalogs are excluded).
# Use a specific list to narrow the report, e.g. ["main", "prod"].
TARGET_CATALOGS = ["*"]

# Where to save the inventory snapshot (optional — leave None to skip persistence).
SNAPSHOT_TABLE = None   # e.g. "main.governance.workspace_inventory_snapshot"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import pandas as pd
from collections import Counter
from datetime import datetime, timezone

w = WorkspaceClient()
ME = w.current_user.me().user_name
HOST = w.config.host
SNAPSHOT_AT = datetime.now(timezone.utc).isoformat(timespec="seconds")

print(f"Reviewing workspace: {HOST}")
print(f"Run by:              {ME}")
print(f"Snapshot at:         {SNAPSHOT_AT}")
print(f"Required tag keys:   {REQUIRED_TAGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pull inventory (read-only)

# COMMAND ----------

# --- Jobs ---
jobs_rows = []
for j in w.jobs.list(expand_tasks=False):
    jobs_rows.append({
        "asset_type":  "job",
        "id":          str(j.job_id),
        "name":        j.settings.name,
        "creator":     j.creator_user_name,
        "tags":        dict(j.settings.tags or {}),
    })

# --- Clusters ---
cluster_rows = []
for c in w.clusters.list():
    cluster_rows.append({
        "asset_type":  "cluster",
        "id":          c.cluster_id,
        "name":        c.cluster_name,
        "creator":     c.creator_user_name,
        "tags":        dict(c.custom_tags or {}),
    })

# --- SQL warehouses ---
warehouse_rows = []
for wh in w.warehouses.list():
    tag_pairs = (wh.tags.custom_tags if wh.tags else []) or []
    warehouse_rows.append({
        "asset_type":  "warehouse",
        "id":          wh.id,
        "name":        wh.name,
        "creator":     wh.creator_name,
        "tags":        {t.key: t.value for t in tag_pairs},
    })

# --- Lakeview dashboards (no tag support today, but counted for visibility) ---
# Use REST directly — w.lakeview.list() only exists on databricks-sdk >= ~0.30.
dashboard_rows = []
try:
    page_token = None
    while True:
        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query=params) or {}
        for d in resp.get("dashboards", []):
            dashboard_rows.append({
                "asset_type":  "dashboard",
                "id":          d.get("dashboard_id"),
                "name":        d.get("display_name"),
                "creator":     None,
                "tags":        {},
            })
        page_token = resp.get("next_page_token")
        if not page_token:
            break
except Exception as e:
    print(f"  (skipping Lakeview dashboards — {type(e).__name__}: {e})")

all_rows = jobs_rows + cluster_rows + warehouse_rows + dashboard_rows
inventory_pdf = pd.DataFrame(all_rows)
print(f"Inventory: {len(jobs_rows)} jobs, {len(cluster_rows)} clusters, "
      f"{len(warehouse_rows)} warehouses, {len(dashboard_rows)} dashboards")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executive summary — coverage % per asset type

# COMMAND ----------

def coverage_for(rows, taggable=True):
    if not rows:
        return {"total": 0, "tagged": 0, "missing_required": 0, "coverage_pct": None, "required_pct": None}
    total = len(rows)
    tagged = sum(1 for r in rows if r["tags"])
    has_required = sum(1 for r in rows if all(k in r["tags"] for k in REQUIRED_TAGS))
    return {
        "total":             total,
        "tagged":            tagged,
        "coverage_pct":      round(100 * tagged / total, 1) if taggable else None,
        "has_required":      has_required,
        "required_pct":      round(100 * has_required / total, 1) if taggable else None,
    }

summary = pd.DataFrame([
    {"asset_type": "job",       **coverage_for(jobs_rows)},
    {"asset_type": "cluster",   **coverage_for(cluster_rows)},
    {"asset_type": "warehouse", **coverage_for(warehouse_rows)},
    {"asset_type": "dashboard", **coverage_for(dashboard_rows, taggable=False)},
])
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Full inventory with tags inline

# COMMAND ----------

inventory_display = inventory_pdf.copy()
inventory_display["tags"] = inventory_display["tags"].apply(
    lambda d: ", ".join(f"{k}={v}" for k, v in sorted(d.items())) if d else ""
)
display(inventory_display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Assets missing required tags
# MAGIC
# MAGIC One row per (asset, missing_key). Sorted by asset for easy reading.

# COMMAND ----------

missing_rows = []
for r in all_rows:
    if r["asset_type"] == "dashboard":
        continue  # tag support N/A
    for k in REQUIRED_TAGS:
        if k not in r["tags"]:
            missing_rows.append({
                "asset_type":  r["asset_type"],
                "id":          r["id"],
                "name":        r["name"],
                "missing_tag": k,
            })

if missing_rows:
    missing_pdf = pd.DataFrame(missing_rows).sort_values(["asset_type", "name", "missing_tag"])
    display(missing_pdf)
    print(f"\n{len(missing_pdf)} (asset, missing_key) violations across {missing_pdf[['asset_type','id']].drop_duplicates().shape[0]} assets")
else:
    print("All taggable assets carry every required tag. ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tag-value distribution
# MAGIC
# MAGIC Top values per tag key — quick way to spot typos (`prod` vs `Production` vs `PRD`) or
# MAGIC orphaned values used by only one asset.

# COMMAND ----------

key_to_values = {}
for r in all_rows:
    for k, v in r["tags"].items():
        key_to_values.setdefault(k, []).append(v)

dist_rows = []
for k, values in sorted(key_to_values.items()):
    for v, n in Counter(values).most_common():
        dist_rows.append({"tag_key": k, "tag_value": v, "asset_count": n})

if dist_rows:
    display(pd.DataFrame(dist_rows))
else:
    print("No tags found on any asset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Unity Catalog table-tag coverage
# MAGIC
# MAGIC Pulls tags via `system.information_schema.table_tags` and joins to `tables` to compute
# MAGIC coverage. Requires SELECT on `system.information_schema` (granted to all account users
# MAGIC by default, but verify if the next cell errors).

# COMMAND ----------

if TARGET_CATALOGS == ["*"]:
    catalog_filter = "WHERE table_catalog NOT LIKE '\\_\\_databricks\\_internal\\_%' ESCAPE '\\\\'"
elif TARGET_CATALOGS:
    quoted = ", ".join(f"'{c}'" for c in TARGET_CATALOGS)
    catalog_filter = f"WHERE table_catalog IN ({quoted})"
else:
    catalog_filter = ""

table_tag_coverage = spark.sql(f"""
    WITH all_tables AS (
        SELECT table_catalog, table_schema, table_name
        FROM system.information_schema.tables
        {catalog_filter}
    ),
    tagged AS (
        SELECT DISTINCT catalog_name AS table_catalog,
                        schema_name  AS table_schema,
                        table_name
        FROM system.information_schema.table_tags
    )
    SELECT
        t.table_catalog,
        t.table_schema,
        COUNT(*)                                       AS total_tables,
        COUNT(tagged.table_name)                       AS tagged_tables,
        ROUND(100.0 * COUNT(tagged.table_name) / COUNT(*), 1) AS coverage_pct
    FROM all_tables t
    LEFT JOIN tagged
        ON t.table_catalog = tagged.table_catalog
       AND t.table_schema  = tagged.table_schema
       AND t.table_name    = tagged.table_name
    GROUP BY t.table_catalog, t.table_schema
    ORDER BY coverage_pct ASC, t.table_catalog, t.table_schema
""")
display(table_tag_coverage)

if table_tag_coverage.count() == 0:
    print()
    print(f"No tables found in scope: TARGET_CATALOGS={TARGET_CATALOGS}")
    print("Run `SHOW CATALOGS` to see what's available, then update TARGET_CATALOGS in Section 0.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. UC tables missing required tags (detail)

# COMMAND ----------

# Build the required-tag list as a comparable string for the query
required_list_sql = ", ".join(f"'{k}'" for k in REQUIRED_TAGS) or "''"

uc_missing = spark.sql(f"""
    WITH tbl AS (
        SELECT table_catalog, table_schema, table_name
        FROM system.information_schema.tables
        {catalog_filter}
    ),
    present AS (
        SELECT
            catalog_name AS table_catalog,
            schema_name  AS table_schema,
            table_name,
            collect_set(tag_name) AS present_tags
        FROM system.information_schema.table_tags
        GROUP BY catalog_name, schema_name, table_name
    )
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        array_except(array({required_list_sql}), coalesce(p.present_tags, array())) AS missing_required_tags
    FROM tbl t
    LEFT JOIN present p
        ON t.table_catalog = p.table_catalog
       AND t.table_schema  = p.table_schema
       AND t.table_name    = p.table_name
    WHERE size(array_except(array({required_list_sql}), coalesce(p.present_tags, array()))) > 0
    ORDER BY t.table_catalog, t.table_schema, t.table_name
""")
display(uc_missing)

if uc_missing.count() == 0:
    # Distinguish "no tables in scope" from "all tables compliant"
    tables_in_scope = spark.sql(
        f"SELECT COUNT(*) FROM system.information_schema.tables {catalog_filter}"
    ).collect()[0][0]
    print()
    if tables_in_scope == 0:
        print(f"No tables found in scope: TARGET_CATALOGS={TARGET_CATALOGS}")
    else:
        print(f"All {tables_in_scope} tables in scope already carry every required tag: {REQUIRED_TAGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. (Optional) Persist snapshot to Delta
# MAGIC
# MAGIC Skipped unless `SNAPSHOT_TABLE` is set in section 0. Useful for trend dashboards.

# COMMAND ----------

if SNAPSHOT_TABLE:
    snapshot_pdf = inventory_pdf.copy()
    snapshot_pdf["tags"]        = snapshot_pdf["tags"].apply(lambda d: ", ".join(f"{k}={v}" for k, v in sorted(d.items())))
    snapshot_pdf["snapshot_at"] = SNAPSHOT_AT
    snapshot_pdf["workspace"]   = HOST

    (spark.createDataFrame(snapshot_pdf)
        .write.mode("append")
        .saveAsTable(SNAPSHOT_TABLE))
    print(f"Appended {len(snapshot_pdf)} rows to {SNAPSHOT_TABLE}")
else:
    print("SNAPSHOT_TABLE not set — skipping persistence.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this notebook does NOT do
# MAGIC
# MAGIC By design, this notebook is **read-only**:
# MAGIC
# MAGIC | Operation                 | Status |
# MAGIC |---------------------------|:------:|
# MAGIC | `jobs.update(...)`        | ❌ not called |
# MAGIC | `clusters.edit(...)`      | ❌ not called |
# MAGIC | `warehouses.edit(...)`    | ❌ not called |
# MAGIC | `ALTER ... SET TAGS`      | ❌ not called |
# MAGIC | Any DDL on workspace assets | ❌ not called |
# MAGIC
# MAGIC To **apply** tags based on this review, use the companion notebook `workspace_inventory_and_tagging.py`,
# MAGIC which has the write-path patterns (single-asset + bulk + dry-run).
