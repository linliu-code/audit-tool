# Hudi Inefficiency Audit

Identifies Hudi-implementation inefficiencies in a Spark application's jobs by
analyzing Spark UI metrics + optional Hudi commit metadata.

**This is NOT a generic Spark perf tool.** It identifies cases where Hudi is
doing more work than the operation logically requires — referring back to a
catalog of known issues (filed PRs, internal hypotheses).

See `DESIGN.md` for requirements, design context, and known-issue catalog.

## Quick start

```bash
# Against a running Spark application
python audit.py --spark-ui http://localhost:4040 --app-id app-20260522-1234

# With Hudi commit metadata enrichment (recommended)
python audit.py --spark-ui http://localhost:4040 --app-id app-20260522-1234 \
                --hudi-path /path/to/hudi/table

# Audit a single job
python audit.py --spark-ui http://localhost:4040 --app-id app-... \
                --job-id 47

# JSON output for tooling integration
python audit.py --spark-ui http://localhost:4040 --app-id app-... --json

# CI integration — fail on high-severity findings
python audit.py --spark-ui ... --app-id ... --fail-on-high
```

## What it detects (v1)

Stage-level rules:
- `count_star_full_scan` — apache/hudi#18769 (count(*) bug)
- `mdt_over_parallelization` — MDT shuffle parallelism too high for workload
- `mor_count_skipping_footer_fast_path` — R-7 missed opt
- `global_index_full_shuffle` — wrong index for the operation
- `marker_handler_dominates` — W-6 marker batch interval issue
- `shuffle_spill` — reduce-side memory pressure
- `task_skew` — long-tail task distribution
- `fetch_wait_dominates` — network-bound shuffle

Job-level (cross-stage) rules:
- `delete_should_be_drop_partition` — partition-aligned DELETE not routed
- `merge_into_smj_small_source` — AQE BHJ conversion not firing
- `bulkinsert_sort_none_many_partitions` — W-6 OOM risk

## Sample output

```
==============================================================================
Hudi Inefficiency Audit  —  app: app-20260522-104715-0001
Hudi version: 1.1.1
==============================================================================

SUMMARY: 4 finding(s) across 2 job(s) — 2 high, 2 medium, 0 low

------------------------------------------------------------------------------
JOB 12  (24.9s)  — MERGE INTO hudi_orders
------------------------------------------------------------------------------

  🔴 [HIGH]  global_index_full_shuffle  @ stage 48
     Known issue:  GLOBAL_SIMPLE index causes full-table shuffle for small upsert
     Affects:      all (config choice)
     Evidence:
        shuffle_write_bytes: 412,300,000
        input_bytes: 12,500,000
        shuffle_amplification: 33.0
     Recommendation:
        GLOBAL_SIMPLE / GLOBAL_BLOOM index moves the entire target side.
        If your upsert respects partition boundaries, switch to SIMPLE or BLOOM (local).
        For true global lookup, use RECORD_INDEX.

  🟡 [MEDIUM]  merge_into_smj_small_source  @ stage 49
     ...
```

## Extending

To add a new detection rule:

1. Add a function in `rules.py` matching the `StageContext -> Optional[Finding]`
   signature (or `List[StageContext] -> List[Finding]` for job-level).
2. Append it to `STAGE_RULES` or `JOB_RULES` at the bottom of `rules.py`.
3. If it links to a Hudi issue, add an entry to `known_issues.KNOWN_ISSUES`.
4. Update `DESIGN.md`'s "v1 / v2 / v3" section.
5. Drop a test fixture into `tests/` if validating against real Spark UI data.

See `DESIGN.md` §3 for the rule schema.

## Files

| File | Purpose |
|---|---|
| `DESIGN.md` | Requirements, design decisions, context for future revisits |
| `audit.py` | CLI entry point |
| `client.py` | SparkUIClient + HudiMetaReader |
| `stage_context.py` | StageContext data class + builder |
| `rules.py` | Detection rule library |
| `known_issues.py` | Cross-reference catalog (PRs, hypotheses, investigation IDs) |
| `report.py` | Text + JSON formatters |
| `README.md` | This file |

## Status

v1 — initial build, June 2026. Tested on synthetic Spark UI fixtures; not yet
validated against live production Spark applications. Run against a known-bad
job (e.g., the F-DML-TUNING tuned MERGE INTO) to confirm rule firing before
relying on it.
