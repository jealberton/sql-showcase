# SQL Showcase

A collection of SQL projects with some query patterns, stored procedures, and data engineering solutions in BigQuery.

> **Note:** All scripts in this repository have been anonymized. Internal project IDs, environment names, and schema references have been replaced with generic placeholders. Business logic and SQL patterns are fully preserved.

---

## Tech Stack

- **BigQuery** — cloud data warehouse (Google Cloud)
- **SQL / BigQuery Stored Procedures** — orchestration and processing logic
- **Window Functions, CTEs, EXECUTE IMMEDIATE** — advanced SQL patterns

---

## Projects

### 📊 Dashboard Cost Analysis
**Folder:** `dashboard-cost-analysis/`

**Business Context:** This project was part of a cloud cost optimization initiative. Over 1TB of data was analyzed to map processing costs to individual dashboards across Tableau and Looker Studio. The analysis enabled targeted optimization actions that reduced processing costs of the most resource-intensive dashboards by **up to 70% per month**.

**What it does:**
- Builds a stored procedure that calculates per-dashboard processing costs in BigQuery
- Joins job execution history with dashboard metadata (Tableau workbooks + Looker Studio dashboards)
- Applies proportional cost attribution based on access counts when multiple dashboards share the same source table
- Produces a monthly aggregate with estimated processing cost in BRL
- Includes full logging and error handling via a log execution procedure

**Key SQL patterns:**
- `EXECUTE IMMEDIATE` for dynamic SQL execution with parameterized project references
- `ROW_NUMBER()` deduplication before aggregation to avoid double-counting bytes processed
- Window functions (`SUM OVER`, `MAX OVER`) for proportional cost distribution across dashboards
- `STRING_AGG` for consolidating multiple table names per dashboard
- `UNION ALL` to combine Tableau and Looker Studio metrics in a single output
- Parameterized stored procedure with full error handling and transaction logging

**Files:**
| File | Description |
|---|---|
| `prc_dashboard_costs.sql` | BigQuery stored procedure for monthly dashboard cost calculation |
