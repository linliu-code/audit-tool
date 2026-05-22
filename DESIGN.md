# Hudi Inefficiency Audit — Design Document

**Status:** v1 — June 2026 (initial build)
**Owner:** Lin Liu
**Lives at:** `/Users/linliu/hudi-perf/tools/hudi_inefficiency_audit/`

This doc captures requirements, design context, and intended evolution for the
Hudi inefficiency audit tool. It is the single source of truth for "why this
tool exists and how it should work" — refer to this when extending.

---

## 1. Why this tool exists — the problem it solves

Standard Spark performance tooling (Sparklens, Dr. Elephant, Delight) tells you
about generic Spark inefficiencies: over-parallelization, skew, spill,
shuffle volume. These tell you **HOW** Spark is running.

They do NOT tell you whether **Hudi** is doing the right thing. Examples of
Hudi-specific inefficiencies that generic tools miss:

- `count(*)` reading full files instead of footer (apache/hudi#18769)
- DELETE with partition-only predicate going through row-tagging instead of
  metadata-only `delete_partition`
- MOR count(*) skipping the footer fast-path even when no log files exist
  (R-7 missed opt)
- MDT writes over-parallelized for the workload size (the W-mdt-1 finding)
- bulkinsert.sort.mode=NONE with many partitions heading toward W-6 OOM
- bytes→string promotion crashing data-skipping (apache/hudi#18810 — 1.1.0 regression)
- reconcile.schema=true silently blocking documented type promotions (apache/hudi#18806)

These are not Spark tuning issues. They are Hudi implementation issues. The
tool's job is to find them automatically from Spark job artifacts.

The framing shift:

| Generic Spark tool tells you | This tool tells you |
|---|---|
| "Stage 47 has skewed tasks" | "Stage 47 is a Hudi `tagLocation` doing GLOBAL_SIMPLE on a 1MB upsert — switch to local SIMPLE" |
| "Shuffle write is 50× input" | "Stage 49 is a Hudi MDT RLI write over-parallelized for record count" |
| "GC time is high" | "The W-6 marker batch interval is making this commit phase wait" |

The output names the **specific Hudi behavior** and links to the **known-issue
catalog** with recommendations.

---

## 2. Requirements

### Functional

- **R1.** Take as input: Spark UI base URL (or event log path) + Spark app ID + optional Hudi table path.
- **R2.** For each completed job in the app, identify which Hudi operation it represents (upsert, insert, merge, etc.) — heuristic by stage names + commit metadata if table path provided.
- **R3.** For each stage in each job, run a configurable set of detection rules.
- **R4.** Each rule that fires produces: (rule name, stage IDs, evidence, linked-issue ref, recommendation).
- **R5.** Output a structured report: per-job summary + per-stage findings, with cross-reference to known issues.
- **R6.** Easy extensibility: adding a new detection rule is one Python function + entry in the rule registry.

### Non-functional

- **R7.** No external dependencies beyond Python stdlib + `requests`.
- **R8.** Should run in < 30 seconds against a typical Spark app history.
- **R9.** Read-only — never modifies Spark UI state or Hudi table state.
- **R10.** Output format: plain text default + structured JSON option (`--json`).

### Out of scope (for v1)

- Tracing executor-level metrics (per-task profiling, async-profiler integration). Add in v2.
- Real-time monitoring (only post-hoc analysis).
- Automatic fix application — output is recommendations only.
- Visualization (no charts; text/JSON only).
- Distributed-tracing-level attribution (e.g., per-RPC timing). Spark UI granularity is sufficient.

---

## 3. Design

### Architecture

```
                ┌────────────────────────────────────────────┐
                │             Inputs                          │
                │   - Spark UI URL or event log path          │
                │   - Application ID                          │
                │   - Optional: Hudi table path               │
                │   - Optional: filter to specific job ID(s)  │
                └─────────────────┬──────────────────────────┘
                                  │
                ┌─────────────────▼──────────────────────────┐
                │   SparkUIClient                             │
                │   - REST API or event log JSON parser       │
                │   - Returns: applications, jobs, stages,    │
                │     stage details, SQL plans                │
                └─────────────────┬──────────────────────────┘
                                  │
                ┌─────────────────▼──────────────────────────┐
                │   HudiMetaReader (optional, if path given)  │
                │   - Reads .commit / .deltacommit metadata   │
                │   - Returns: operation type per commit,     │
                │     file counts, MDT partition list         │
                └─────────────────┬──────────────────────────┘
                                  │
                ┌─────────────────▼──────────────────────────┐
                │   StageContext builder                      │
                │   - Per stage, assemble: metrics, name      │
                │     pattern, Hudi-attributable phase,       │
                │     SQL plan node (if available)            │
                └─────────────────┬──────────────────────────┘
                                  │
                ┌─────────────────▼──────────────────────────┐
                │   RuleEngine                                │
                │   - Iterates registered rules               │
                │   - Each rule: takes StageContext, returns  │
                │     Finding | None                          │
                │   - Findings: rule name, evidence dict,     │
                │     linked-issue, recommendation            │
                └─────────────────┬──────────────────────────┘
                                  │
                ┌─────────────────▼──────────────────────────┐
                │   ReportFormatter                           │
                │   - Plain text for humans                   │
                │   - JSON for tooling                        │
                └────────────────────────────────────────────┘
```

### File layout

- `audit.py` — CLI entry point, argument parsing, orchestration
- `client.py` — SparkUIClient + HudiMetaReader
- `stage_context.py` — StageContext data class + builder
- `rules.py` — Rule definitions (one function per detection rule)
- `known_issues.py` — Known issue catalog (W-6, #18770, #18810, etc.)
- `report.py` — Text + JSON formatters
- `tests/` — Unit tests for rules (use fixture JSON from real Spark UI)

### Data flow per job

For each job:
1. Pull stages + their metrics from Spark UI
2. Read SQL plan if available (helps identify join types, MERGE INTO source size)
3. Optionally enrich with Hudi commit metadata (operation type)
4. For each stage:
   a. Build StageContext
   b. Run all rules
   c. Collect findings
5. Compute per-job summary (total findings, severity buckets)

### Rule schema

Each rule is a Python function with signature:

```python
def rule_<name>(ctx: StageContext) -> Optional[Finding]:
    if not <condition>:
        return None
    return Finding(
        rule_id="<name>",
        severity="high" | "medium" | "low",
        evidence={...},
        linked_issue="apache/hudi#<number>" or "W-6" or None,
        recommendation="...",
    )
```

Rules are pure functions of one stage's context. Cross-stage rules use a
higher-level `JobContext` instead (e.g., "stage 47 + stage 49 together
indicate X").

### Known-issue catalog

A YAML/JSON file listing every Hudi inefficiency we've cataloged from the
problem-map work, with:
- ID (`W-6-marker-batch`, `R-2-count-star`, etc.)
- Title
- Description
- Filed-PR URL if any
- Detection signature (which rules detect it)
- Workaround / recommendation
- Severity

The rule engine cross-references findings against this catalog when emitting
reports.

---

## 4. Context for future revisits

### Sources this tool is built on

- The 27-surface taxonomy from `/Users/linliu/hudi-perf/PROBLEM_MAP.md`
- The shuffle-stage decomposition matrix (S1–S19) developed in chat
- The DML coverage matrix (10 DMLs, which step is exercised)
- The default-vs-tuned probe (F-DML-TUNING) showing defaults are often
  efficient at small/medium scale
- The CDC schema-evolution probes (F-S5S6-#1 through #7) that catalogued
  multiple Hudi regressions and missed opts

### Filed upstream PRs (referenced by the known-issue catalog)

- apache/hudi#18769 — `count(*)` reading file content instead of footers (issue)
- apache/hudi#18770 — fix PR for #18769
- apache/hudi#18792 — col-stats NaN/truncation correctness (filed for #18754 + #18755)
- apache/hudi#18794 — `outputTimestampType` ignored (#18752)
- apache/hudi#18806 — `reconcile.schema=true` blocks documented type promotion (repro)
- apache/hudi#18807 — MDT col_stats auto-extend on ADD COLUMN (codification)
- apache/hudi#18810 — `bytes → string` promotion crashes data-skipping (1.1.0 regression)

### Internal hypotheses pending filing

- **H-w6-#1**: change `markerBatchIntervalMs` default 50→10
- **H-w6-#2**: change `bulkinsert.sort.mode` default NONE→PARTITION_SORT
- **H-r7-#1**: widen `isCount` gate to allow MOR file slices with no logs

### Methodology lessons baked into the tool

- "Single-iteration triage signals in 1.5–2× range are unreliable" — surfaced
  as W-5 / R-9 / W-9 false positives. The tool should require ≥ 3 iterations
  for any "speedup claim" rule.
- "Tuning recommendations are scale-dependent" — surfaced by F-DML-TUNING.
  Rules should include a `min_scale` parameter where applicable.
- "MDT is more expensive than people expect" — surfaced by P-MDT-FULL-1.
  Rules should attribute MDT overhead explicitly.

### Re-entry checklist for future contributors

To work on this tool:
1. Read this DESIGN.md top-to-bottom.
2. Run `python audit.py --help` to see current CLI.
3. Look at `rules.py` to see the rule schema. Add a new rule by adding a
   function and registering it in the `RULES` list.
4. To validate against a known scenario: use the fixture JSONs in `tests/`.
5. To add a new known issue: append to `known_issues.py` and create or update
   the rule that detects it.

---

## 5. Evolution roadmap

### v1 (this release) — 6-8 detection rules

Focused on the highest-impact inefficiencies we've cataloged:

1. `count_star_full_scan` — apache/hudi#18769 (count(*) bug)
2. `delete_should_be_drop_partition` — partition-aligned DELETE not routed
3. `mdt_over_parallelization` — MDT shuffle tasks much larger than data warrants
4. `bulkinsert_sort_none_many_partitions` — W-6 OOM risk
5. `marker_batch_interval_dominates` — H-w6-#1 territory
6. `merge_into_smj_small_source` — AQE BHJ conversion not firing
7. `global_simple_index_small_upsert` — wrong index for the operation
8. `mor_count_skipping_footer_fast_path` — R-7 missed opt

### v2 (future) — broader coverage + visualization

- Add 10+ more rules from continued problem-map work
- Add `--watch` mode for streaming jobs (sample UI periodically)
- Add HTML/Markdown report formats
- Add per-task drill-down for stages flagged with skew
- Integrate event-log parsing for offline analysis

### v3 (future) — production deployment

- Run as a sidecar against staging clusters
- Slack/PagerDuty alerts on high-severity findings
- Trend tracking — same workload, did it regress?

---

## 6. Known limitations

- Rules are heuristic. False positives possible. Recommendations should be
  validated empirically before applying.
- Hudi version detection is implicit (via Spark conf). If absent, rules that
  depend on version (e.g., "1.1.0+ has bytes→string regression") may miss.
- SQL plan parsing depends on Spark version compatibility. We follow Spark
  3.4+ API; older versions may need fallback.
- Stage-name heuristics for Hudi-attribution are best-effort. Hudi name
  conventions may change across versions; rules need maintenance.

---

## 7. Audit log (record changes here when extending)

| Date | Change | Why |
|---|---|---|
| 2026-05-22 | v1 initial build | Materializes the diagnostic framework developed in chat. |
| 2026-05-22 | First smoke test against live Hudi MERGE INTO | Tool connected, parsed metrics, ran rules end-to-end. 24 findings, all `task_skew` from JIT-warmup artifacts (10× p99/median ratios with absolute durations <250ms each). |

### Lessons from first smoke test (immediate v1.1 work)

1. **`task_skew` rule is too aggressive** — fires on JIT-warmup artifacts (iter1 ≫ steady) at small scale. Need to add a minimum-absolute-duration gate (e.g., only fire if p99 > 2s OR median > 200ms). The X-3 finding from the problem map is the canonical pattern this rule misclassifies.

2. **Hudi `hudi_phase` detection misses many stages** — many show as `unknown` because Hudi 1.1.1's actual stage names don't match my heuristic patterns. Examples seen: `foreach at HoodieSparkEngineContext.java:175`, `collect at SparkRDDWriteClient.java:442`. Need to expand pattern matching with class-name fallbacks.

3. **Many small Hudi-internal sub-jobs per user DML** — one MERGE INTO produced 33 Spark jobs. Per-job findings are granular but noisy. Future: group sub-jobs under the umbrella DML by commit-instant correlation.

4. **No high-severity findings on this run** — expected, since the MERGE INTO ran with defaults at moderate scale (and we confirmed earlier defaults are efficient there). Next: validate the tool against the F-DML-TUNING tuned run, which SHOULD produce high-sev findings (over-parallelization).

### Pending v1.1 changes (queued)

- [ ] Tighten `task_skew` with absolute-duration gate
- [ ] Expand `hudi_phase` patterns to cover real 1.1.1 stage names (foreach/collect/etc.)
- [ ] Validate against F-DML-TUNING tuned run — should produce `mdt_over_parallelization` findings
- [ ] Add fixture-based unit tests with real captured Spark UI JSON
- [ ] Handle the `spark.jars` regex parsing for version detection (current impl misses some bundle naming conventions)
