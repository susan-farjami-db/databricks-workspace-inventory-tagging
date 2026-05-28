# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Inventory & Tagging from a Notebook
# MAGIC
# MAGIC This notebook shows how to do everything the Databricks CLI does for workspace assets — but inline from a notebook or job — using the **Databricks Python SDK** (`databricks-sdk`, pre-installed on DBR).
# MAGIC
# MAGIC It covers:
# MAGIC 1. **Listing** notebooks, jobs, AI/BI (Lakeview) dashboards, clusters, SQL warehouses, and Unity Catalog objects
# MAGIC 2. **Reading** existing tags on each asset type
# MAGIC 3. **Applying / updating** tags (single asset and bulk)
# MAGIC 4. Exporting a full **inventory DataFrame** you can save to a Delta table
# MAGIC
# MAGIC Authentication is automatic when run inside Databricks — no config needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as jobs_svc
from databricks.sdk.service import sql as sql_svc
from databricks.sdk.service import compute as compute_svc

w = WorkspaceClient()
print(f"Connected as: {w.current_user.me().user_name}")
print(f"Workspace:    {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List notebooks, files, and folders
# MAGIC
# MAGIC `workspace.list()` returns one level at a time. To walk subfolders, recurse manually
# MAGIC on entries with `object_type == DIRECTORY`.

# COMMAND ----------

from databricks.sdk.service.workspace import ObjectType

def walk_workspace(root: str, descend: bool = True):
    """Yield every ObjectInfo under `root`. Set descend=False for one level only."""
    for obj in w.workspace.list(root):
        yield obj
        if descend and obj.object_type == ObjectType.DIRECTORY:
            yield from walk_workspace(obj.path, descend=True)

# Pick any path you want to scan — your home folder, a shared folder, etc.
ROOT_PATH = f"/Users/{w.current_user.me().user_name}"

# Set descend=False for a top-level listing only (much faster on large trees)
assets = list(walk_workspace(ROOT_PATH, descend=True))
print(f"Found {len(assets)} objects under {ROOT_PATH}\n")

# object_type is one of: NOTEBOOK, FILE, DIRECTORY, REPO, LIBRARY, DASHBOARD
for a in assets[:10]:
    ot = a.object_type.value if a.object_type else "?"
    print(f"  {ot:10s}  {a.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. List jobs and their tags
# MAGIC
# MAGIC Jobs have first-class tags via `settings.tags` (a `dict[str, str]`).

# COMMAND ----------

jobs_inventory = []
# w.jobs.list() returns a paginated iterator — no need to set a limit
for j in w.jobs.list(expand_tasks=False):
    jobs_inventory.append({
        "job_id":    j.job_id,
        "name":      j.settings.name,
        "creator":   j.creator_user_name,
        "tags":      dict(j.settings.tags or {}),
    })

print(f"Found {len(jobs_inventory)} jobs")
for row in jobs_inventory[:5]:
    print(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. List AI/BI (Lakeview) dashboards

# COMMAND ----------

# Use the REST API directly — w.lakeview.list() requires databricks-sdk >= ~0.30,
# but the underlying endpoint is available in every workspace.
dashboards = []
page_token = None
while True:
    params = {"page_size": 100}
    if page_token:
        params["page_token"] = page_token
    resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query=params) or {}
    for d in resp.get("dashboards", []):
        dashboards.append({
            "dashboard_id":  d.get("dashboard_id"),
            "name":          d.get("display_name"),
            "path":          d.get("path"),
            "warehouse_id":  d.get("warehouse_id"),
        })
    page_token = resp.get("next_page_token")
    if not page_token:
        break

print(f"Found {len(dashboards)} Lakeview dashboards")
for row in dashboards[:5]:
    print(row)

# Note: Lakeview dashboards don't currently support tags directly.
# Teams typically tag the underlying warehouse or organize by folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. List clusters and their tags
# MAGIC
# MAGIC Clusters expose `custom_tags` (`dict[str, str]`).

# COMMAND ----------

clusters_inventory = []
for c in w.clusters.list():
    clusters_inventory.append({
        "cluster_id":   c.cluster_id,
        "name":         c.cluster_name,
        "state":        c.state.value if c.state else None,
        "creator":      c.creator_user_name,
        "custom_tags":  dict(c.custom_tags or {}),
    })

print(f"Found {len(clusters_inventory)} clusters")
for row in clusters_inventory[:5]:
    print(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. List SQL warehouses and their tags
# MAGIC
# MAGIC Warehouses expose `tags.custom_tags` as a list of key/value pairs.

# COMMAND ----------

warehouses_inventory = []
for wh in w.warehouses.list():
    tag_pairs = (wh.tags.custom_tags if wh.tags else []) or []
    warehouses_inventory.append({
        "warehouse_id":  wh.id,
        "name":          wh.name,
        "size":          wh.cluster_size,
        "state":         wh.state.value if wh.state else None,
        "tags":          {t.key: t.value for t in tag_pairs},
    })

print(f"Found {len(warehouses_inventory)} SQL warehouses")
for row in warehouses_inventory[:5]:
    print(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. List Unity Catalog objects and their tags
# MAGIC
# MAGIC UC catalogs, schemas, tables, columns, volumes, and models all support tags.
# MAGIC The easiest way to read tags in bulk is the `system.information_schema` views.

# COMMAND ----------

# All table-level tags across the metastore.
# Use "*" for every catalog you can see (Databricks-internal catalogs are excluded),
# or change to a specific catalog like "main" or "serverless_stable_2sfjed_catalog".
TARGET_CATALOG = "*"

if TARGET_CATALOG == "*":
    catalog_filter = "WHERE catalog_name NOT LIKE '\\_\\_databricks\\_internal\\_%' ESCAPE '\\\\'"
else:
    catalog_filter = f"WHERE catalog_name = '{TARGET_CATALOG}'"

table_tags_df = spark.sql(f"""
    SELECT catalog_name, schema_name, table_name, tag_name, tag_value
    FROM system.information_schema.table_tags
    {catalog_filter}
    ORDER BY catalog_name, schema_name, table_name
""")
display(table_tags_df)

if table_tags_df.count() == 0:
    print()
    print(f"No tagged tables found in scope: TARGET_CATALOG={TARGET_CATALOG!r}")
    print("If you expected tags to exist, run `SHOW CATALOGS` and update TARGET_CATALOG.")
    print("If you're just starting out, Section 7d below shows how to apply table tags via SQL.")

# Other useful views:
#   system.information_schema.catalog_tags
#   system.information_schema.schema_tags
#   system.information_schema.column_tags
#   system.information_schema.volume_tags

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Apply / update tags
# MAGIC
# MAGIC The patterns below are **idempotent** — re-running won't create duplicates.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a. Tag a job
# MAGIC
# MAGIC `jobs.update()` merges with existing settings. Use it to patch just the tags.

# COMMAND ----------

# EXAMPLE — replace JOB_ID with a real job id from section 2
JOB_ID = 0  # <-- set me

if JOB_ID:
    job = w.jobs.get(job_id=JOB_ID)
    new_tags = dict(job.settings.tags or {})
    new_tags.update({
        "team":        "data-engineering",
        "cost_center": "1234",
        "env":         "prod",
    })

    w.jobs.update(
        job_id=JOB_ID,
        new_settings=jobs_svc.JobSettings(tags=new_tags),
    )
    print(f"Updated tags on job {JOB_ID}: {new_tags}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. Tag a cluster

# COMMAND ----------

CLUSTER_ID = ""  # <-- set me

if CLUSTER_ID:
    c = w.clusters.get(cluster_id=CLUSTER_ID)
    new_tags = dict(c.custom_tags or {})
    new_tags.update({"team": "data-engineering", "env": "prod"})

    # clusters.edit() requires the full spec; reuse the existing one
    w.clusters.edit(
        cluster_id=CLUSTER_ID,
        cluster_name=c.cluster_name,
        spark_version=c.spark_version,
        node_type_id=c.node_type_id,
        num_workers=c.num_workers,
        custom_tags=new_tags,
    )
    print(f"Updated tags on cluster {CLUSTER_ID}: {new_tags}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c. Tag a SQL warehouse

# COMMAND ----------

WAREHOUSE_ID = ""  # <-- set me

if WAREHOUSE_ID:
    wh = w.warehouses.get(id=WAREHOUSE_ID)
    existing = {t.key: t.value for t in (wh.tags.custom_tags if wh.tags else [])}
    existing.update({"team": "analytics", "env": "prod"})

    w.warehouses.edit(
        id=WAREHOUSE_ID,
        name=wh.name,
        cluster_size=wh.cluster_size,
        tags=sql_svc.EndpointTags(
            custom_tags=[sql_svc.EndpointTagPair(key=k, value=v) for k, v in existing.items()]
        ),
    )
    print(f"Updated tags on warehouse {WAREHOUSE_ID}: {existing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7d. Tag Unity Catalog objects (SQL)
# MAGIC
# MAGIC The cleanest way to tag tables, schemas, catalogs, and columns is plain SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table tags
# MAGIC -- ALTER TABLE main.sales.orders SET TAGS ('team' = 'data-eng', 'pii' = 'false');
# MAGIC
# MAGIC -- Schema tags
# MAGIC -- ALTER SCHEMA main.sales SET TAGS ('domain' = 'commerce');
# MAGIC
# MAGIC -- Column tags
# MAGIC -- ALTER TABLE main.sales.orders ALTER COLUMN customer_email SET TAGS ('pii' = 'true');
# MAGIC
# MAGIC -- Volume tags
# MAGIC -- ALTER VOLUME main.sales.raw_drops SET TAGS ('zone' = 'bronze');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Bulk tagging example
# MAGIC
# MAGIC Apply a standard tag set to every job whose name matches a pattern.

# COMMAND ----------

PATTERN = "etl_"  # tag every job whose name starts with "etl_"
STANDARD_TAGS = {"team": "data-engineering", "managed_by": "platform"}
DRY_RUN = True   # flip to False to actually write

updated = 0
for j in w.jobs.list():
    if not j.settings.name or not j.settings.name.startswith(PATTERN):
        continue

    merged = dict(j.settings.tags or {})
    if all(merged.get(k) == v for k, v in STANDARD_TAGS.items()):
        continue  # already tagged — skip

    merged.update(STANDARD_TAGS)
    print(f"{'[DRY] ' if DRY_RUN else ''}Tagging job {j.job_id} ({j.settings.name}) -> {merged}")

    if not DRY_RUN:
        w.jobs.update(job_id=j.job_id, new_settings=jobs_svc.JobSettings(tags=merged))
    updated += 1

print(f"\n{'Would update' if DRY_RUN else 'Updated'} {updated} job(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Export a full inventory to Delta
# MAGIC
# MAGIC Combine everything above into a single inventory DataFrame and persist it.
# MAGIC Useful for governance dashboards, tag-coverage reports, or chargeback.

# COMMAND ----------

import pandas as pd

inventory_rows = (
    [{"asset_type": "job",       "id": str(r["job_id"]),       "name": r["name"], "tags": r["tags"]}      for r in jobs_inventory]
  + [{"asset_type": "cluster",   "id": r["cluster_id"],        "name": r["name"], "tags": r["custom_tags"]} for r in clusters_inventory]
  + [{"asset_type": "warehouse", "id": r["warehouse_id"],      "name": r["name"], "tags": r["tags"]}      for r in warehouses_inventory]
  + [{"asset_type": "dashboard", "id": r["dashboard_id"],      "name": r["name"], "tags": {}}             for r in dashboards]
)

inventory_pdf = pd.DataFrame(inventory_rows)
inventory_pdf["tags"] = inventory_pdf["tags"].apply(lambda d: ", ".join(f"{k}={v}" for k, v in d.items()))
display(inventory_pdf)

# To persist:
# spark.createDataFrame(inventory_pdf).write.mode("overwrite").saveAsTable("main.governance.workspace_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: what supports tags today
# MAGIC
# MAGIC | Asset                                          | Tag support | How                                       |
# MAGIC |------------------------------------------------|:-----------:|-------------------------------------------|
# MAGIC | Jobs                                           | ✅          | `settings.tags`                           |
# MAGIC | Clusters                                       | ✅          | `custom_tags`                             |
# MAGIC | SQL warehouses                                 | ✅          | `tags.custom_tags`                        |
# MAGIC | UC catalogs / schemas / tables / columns / volumes / models | ✅          | `ALTER ... SET TAGS (...)`                |
# MAGIC | Pipelines (Lakeflow / DLT)                     | ✅          | `tags` on pipeline spec                   |
# MAGIC | Model Serving endpoints                        | ✅          | `tags`                                    |
# MAGIC | Notebooks / workspace files                    | ❌          | Organize by folder; tag the job that runs them |
# MAGIC | AI/BI (Lakeview) dashboards                    | ❌          | Tag the warehouse / underlying tables     |
# MAGIC
# MAGIC Every taggable asset is reachable from the SDK or from SQL — there is no CLI-only path.
