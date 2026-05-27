# Workspace Inventory & Tagging — Sample Notebooks

Two Databricks notebooks for managing workspace assets and tags **from a notebook or job** instead of the CLI, using the Databricks Python SDK (`databricks-sdk`, pre-installed on DBR).

| Notebook | What it does | Read-only? |
|---|---|:--:|
| `workspace_inventory_and_tagging.py` | List notebooks, jobs, dashboards, clusters, warehouses, UC objects; read **and apply** tags (single-asset + bulk patterns with `DRY_RUN` toggle) | No |
| `workspace_tag_coverage_review.py`   | Pull the same inventory and produce a tag-coverage governance report — required-tag violations, tag-value drift, UC table coverage | **Yes** |

## How to import

In your Databricks workspace:

1. **Workspace → Import** (top-right of the file browser)
2. Choose **File**, drop in either `.py` file
3. Databricks recognizes the `# Databricks notebook source` header and imports it as a notebook

## What gets tagged where

| Asset                                                                         | Tag support today | API                              |
|-------------------------------------------------------------------------------|:-----------------:|----------------------------------|
| Jobs                                                                          | ✅                | `WorkspaceClient.jobs.update`    |
| Clusters                                                                      | ✅                | `WorkspaceClient.clusters.edit`  |
| SQL warehouses                                                                | ✅                | `WorkspaceClient.warehouses.edit`|
| UC catalogs / schemas / tables / columns / volumes / models                   | ✅                | `ALTER ... SET TAGS (...)`       |
| Lakeflow / DLT pipelines                                                      | ✅                | pipeline spec `tags`             |
| Model Serving endpoints                                                       | ✅                | endpoint `tags`                  |
| Notebooks / workspace files                                                   | ❌                | tag the enclosing job instead    |
| AI/BI (Lakeview) dashboards                                                   | ❌                | tag the warehouse / source tables|

## Auth

Both notebooks rely on the default Databricks SDK auth chain. When run **inside a Databricks workspace**, that's automatic — no token, no config. Outside the workspace, set a profile via `~/.databrickscfg` or the standard env vars.

## Recommended order

1. Run **`workspace_tag_coverage_review.py`** first (read-only) to see current state.
2. Decide on a `REQUIRED_TAGS` standard.
3. Use **`workspace_inventory_and_tagging.py`** to apply tags. Keep `DRY_RUN = True` for the first pass to preview changes.
4. Re-run the review notebook to confirm coverage improved.

## Pitfalls hit during testing

- `w.jobs.list(limit=N)` is capped at 100 by the API. The SDK returns a paginated iterator already — don't pass `limit`.
- Two `~/.databrickscfg` profiles pointing at the same workspace host break the SDK's CLI-auth subprocess (it can't disambiguate). Either delete the duplicate or use direct token auth.
- `system.information_schema.table_tags` requires SELECT on `system.information_schema` (granted to all account users by default — verify if it errors).

