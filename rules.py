"""Detection rules for Hudi-implementation inefficiencies.

Each rule is a pure function: StageContext -> Optional[Finding]. Add a new
rule by writing a function and appending it to the RULES list at the bottom.

For cross-stage detections (where you need to look at multiple stages of a
job together), use a JobRule function that takes List[StageContext].
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from stage_context import StageContext


@dataclass
class Finding:
    rule_id: str
    severity: str  # "high", "medium", "low"
    stage_id: Optional[int]
    evidence: Dict
    linked_issue: Optional[str]  # Key in known_issues.KNOWN_ISSUES
    recommendation: str
    job_id: Optional[int] = None  # set by orchestrator
    rule_kind: str = "stage"  # "stage" or "job"


# ──────────────────────── Stage-level rules ──────────────────────────────


def rule_count_star_full_scan(ctx: StageContext) -> Optional[Finding]:
    """count(*) reading file content instead of parquet footers (#18769).

    Signature: stage that's part of an aggregation but reading >> footer-size
    bytes per file. We can't always identify count(*) from stage info alone;
    use the SQL plan if available, otherwise fall back to "stage reads
    surprisingly many bytes given it's just aggregating."
    """
    # Heuristic: stage doing aggregation but reading > 100KB per file
    if ctx.input_bytes < 1024 * 1024:
        return None
    # If we have a SQL node and it's a count aggregation, raise confidence
    if ctx.sql_node and "count" in str(ctx.sql_node).lower():
        # Compare bytes-per-record — count(*) should not read full rows
        records = max(ctx.shuffle_read_records, 1)
        bytes_per_record = ctx.input_bytes / records
        if bytes_per_record > 1000:  # full row reads ~hundreds of bytes; footer reads <100
            return Finding(
                rule_id="count_star_full_scan",
                severity="high",
                stage_id=ctx.stage_id,
                evidence={
                    "input_bytes": ctx.input_bytes,
                    "shuffle_read_records": records,
                    "bytes_per_record": int(bytes_per_record),
                    "note": "count(*) aggregation node observed in SQL plan",
                },
                linked_issue="hudi-18769-count-star",
                recommendation=(
                    "Watch apache/hudi#18770 for merge. Workaround: query raw "
                    "parquet directly if this is performance-critical."
                ),
            )
    return None


def rule_mdt_over_parallelization(ctx: StageContext) -> Optional[Finding]:
    """MDT shuffle stage has many tiny tasks for the data volume."""
    if not ctx.hudi_phase.startswith("mdt"):
        return None
    if ctx.num_tasks < 50:
        return None
    avg_per_task = ctx.avg_input_per_task_bytes
    if avg_per_task < 100 * 1024:  # < 100 KB per task = over-parallel
        return Finding(
            rule_id="mdt_over_parallelization",
            severity="medium",
            stage_id=ctx.stage_id,
            evidence={
                "hudi_phase": ctx.hudi_phase,
                "num_tasks": ctx.num_tasks,
                "input_bytes": ctx.input_bytes,
                "shuffle_read_bytes": ctx.shuffle_read_bytes,
                "avg_per_task_bytes": int(avg_per_task),
                "note": "MDT shuffle parallelism much larger than data warrants",
            },
            linked_issue="mdt-over-parallelization",
            recommendation=(
                "Lower hoodie.metadata.*.parallelism for this MDT partition. "
                "Target ~5000-50000 records per task. Rule of thumb: "
                "raise per-task data to at least 1MB."
            ),
        )
    return None


def rule_mor_count_skipping_footer_fast_path(ctx: StageContext) -> Optional[Finding]:
    """MOR count(*) reading data when the slice has no log files (R-7)."""
    if ctx.hudi_table_config is None:
        return None
    if ctx.hudi_table_config.get("hoodie.table.type", "").upper() != "MERGE_ON_READ":
        return None
    if not ctx.sql_node or "count" not in str(ctx.sql_node).lower():
        return None
    # Heuristic: input bytes per file group is much higher than footer-only
    records = max(ctx.shuffle_read_records, 1)
    if ctx.input_bytes / records > 500:  # full-row read on count(*)
        return Finding(
            rule_id="mor_count_skipping_footer_fast_path",
            severity="medium",
            stage_id=ctx.stage_id,
            evidence={
                "table_type": "MERGE_ON_READ",
                "input_bytes": ctx.input_bytes,
                "bytes_per_record": int(ctx.input_bytes / records),
            },
            linked_issue="r7-mor-count-no-logs",
            recommendation=(
                "If file slices have no log files, count(*) should use footer "
                "fast-path. Pending fix: widen the isCount gate to allow MOR "
                "slices without log files (H-r7-#1)."
            ),
        )
    return None


def rule_global_index_full_shuffle(ctx: StageContext) -> Optional[Finding]:
    """Heuristic: tagLocation stage with very high shuffle write / input ratio
    suggests global index doing cross-partition shuffle for a small upsert."""
    if ctx.hudi_phase != "tagLocation":
        return None
    if ctx.shuffle_write_bytes < 100 * 1024 * 1024:
        # only flag at meaningful scale
        return None
    if ctx.shuffle_amplification > 10:
        return Finding(
            rule_id="global_index_full_shuffle",
            severity="high",
            stage_id=ctx.stage_id,
            evidence={
                "shuffle_write_bytes": ctx.shuffle_write_bytes,
                "input_bytes": ctx.input_bytes,
                "shuffle_amplification": round(ctx.shuffle_amplification, 1),
            },
            linked_issue="global-simple-index-small-upsert",
            recommendation=(
                "GLOBAL_SIMPLE / GLOBAL_BLOOM index moves the entire target side. "
                "If your upsert respects partition boundaries, switch to SIMPLE "
                "or BLOOM (local). For true global lookup, use RECORD_INDEX."
            ),
        )
    return None


def rule_marker_handler_dominates(ctx: StageContext) -> Optional[Finding]:
    """Stages spent in marker management indicate W-6 territory."""
    if ctx.hudi_phase != "markerHandling":
        return None
    # If a marker stage is on the critical path and > 1s, flag it
    if ctx.duration_ms > 1000:
        return Finding(
            rule_id="marker_handler_dominates",
            severity="medium",
            stage_id=ctx.stage_id,
            evidence={
                "duration_ms": ctx.duration_ms,
                "stage_name": ctx.name,
            },
            linked_issue="w6-marker-batch-interval",
            recommendation=(
                "Marker batch interval defaults to 50ms and is paid serially "
                "per file. Lower hoodie.markerBatchIntervalMs to 10ms, OR set "
                "hoodie.write.markers.type=DIRECT (use the latter for "
                "object-store backends; the former for local FS / HDFS)."
            ),
        )
    return None


def rule_shuffle_spill(ctx: StageContext) -> Optional[Finding]:
    """Any shuffle stage with disk spill is operating under memory pressure."""
    if ctx.disk_spill_bytes <= 0:
        return None
    return Finding(
        rule_id="shuffle_spill",
        severity="medium",
        stage_id=ctx.stage_id,
        evidence={
            "memory_spill_bytes": ctx.memory_spill_bytes,
            "disk_spill_bytes": ctx.disk_spill_bytes,
            "hudi_phase": ctx.hudi_phase,
        },
        linked_issue=None,
        recommendation=(
            "Reduce-side memory pressure. Options: raise spark.executor.memory, "
            "raise spark.memory.fraction, lower spark.reducer.maxSizeInFlight, "
            "or raise shuffle.partitions (smaller per-task data)."
        ),
    )


def rule_skew(ctx: StageContext) -> Optional[Finding]:
    """Long-tail task distribution within a stage."""
    if ctx.num_tasks < 10:
        return None
    if ctx.skew_ratio < 5:
        return None
    return Finding(
        rule_id="task_skew",
        severity="medium",
        stage_id=ctx.stage_id,
        evidence={
            "num_tasks": ctx.num_tasks,
            "task_duration_p99_ms": ctx.task_duration_p99_ms,
            "task_duration_median_ms": ctx.task_duration_median_ms,
            "skew_ratio": round(ctx.skew_ratio, 1),
            "hudi_phase": ctx.hudi_phase,
        },
        linked_issue=None,
        recommendation=(
            "p99 task is much slower than median — data skew. Verify "
            "spark.sql.adaptive.skewJoin.enabled=true and check if the "
            "shuffle key (partition path / record key) has skewed values."
        ),
    )


def rule_fetch_wait_dominates(ctx: StageContext) -> Optional[Finding]:
    """Stage is fetch-bound rather than compute-bound."""
    if ctx.shuffle_read_bytes < 10 * 1024 * 1024:
        return None
    if ctx.fetch_wait_fraction < 0.3:
        return None
    return Finding(
        rule_id="fetch_wait_dominates",
        severity="medium",
        stage_id=ctx.stage_id,
        evidence={
            "shuffle_read_bytes": ctx.shuffle_read_bytes,
            "fetch_wait_time_ms": ctx.fetch_wait_time_ms,
            "duration_ms": ctx.duration_ms,
            "fetch_wait_fraction": round(ctx.fetch_wait_fraction, 2),
        },
        linked_issue=None,
        recommendation=(
            "More than 30% of stage time is spent waiting for shuffle fetches. "
            "Enable external shuffle service. Consider push-based shuffle "
            "(spark.shuffle.push.enabled=true, Spark 3.2+). Raise "
            "spark.shuffle.io.numConnectionsPerPeer."
        ),
    )


# ──────────────────────── Job-level rules (cross-stage) ───────────────────


def rule_delete_should_be_drop_partition(
    job_stages: List[StageContext],
    hudi_commit: Optional[Dict] = None,
) -> List[Finding]:
    """Job that is a DELETE with partition-only predicate that didn't use
    delete_partition. Detected by:
      - Hudi commit metadata shows operation=delete (not delete_partition)
      - The DELETE predicate referenced only partition columns
      - Stage trace includes tagLocation + per-row writes

    Without parsing the SQL we can only flag the first two; for v1 we use the
    presence of tagLocation as a proxy for "per-row delete path active".
    """
    if not hudi_commit:
        return []
    op_type = hudi_commit.get("operationType", "").lower()
    if op_type != "delete":
        return []
    # Look for tagLocation + write stages (per-row delete path)
    has_taglocation = any(s.hudi_phase == "tagLocation" for s in job_stages)
    if not has_taglocation:
        return []
    return [Finding(
        rule_id="delete_should_be_drop_partition",
        severity="high",
        stage_id=None,
        rule_kind="job",
        evidence={
            "hudi_operation": op_type,
            "has_taglocation_stage": True,
            "note": (
                "DELETE operation with per-row tagging. If the WHERE predicate "
                "references only partition columns, this could have been "
                "delete_partition (100x+ faster). Cannot confirm predicate "
                "shape without SQL text."
            ),
        },
        linked_issue="delete-should-be-drop-partition",
        recommendation=(
            "Check the DELETE's WHERE clause. If it references only partition "
            "columns, rewrite as ALTER TABLE ... DROP PARTITION or CALL "
            "drop_partition(...). For automated handling, file a Hudi "
            "analyzer-rule PR."
        ),
    )]


def rule_merge_into_smj_with_small_source(
    job_stages: List[StageContext],
    sql_executions: Optional[List[Dict]] = None,
) -> List[Finding]:
    """MERGE INTO using SortMergeJoin when source side is small enough for BHJ.

    Detection: a stage whose hudi_phase is mergeSourceJoin with shuffle_write
    bytes below ~50MB suggests source could have been broadcast but wasn't.
    """
    findings = []
    for s in job_stages:
        if s.hudi_phase != "mergeSourceJoin":
            continue
        if "sortmerge" not in s.name.lower():
            continue
        if s.shuffle_write_bytes > 100 * 1024 * 1024:
            continue  # source side genuinely large, SMJ is correct
        findings.append(Finding(
            rule_id="merge_into_smj_small_source",
            severity="medium",
            stage_id=s.stage_id,
            evidence={
                "stage_name": s.name,
                "shuffle_write_bytes": s.shuffle_write_bytes,
                "note": "MERGE INTO using SortMergeJoin but source is < 100MB",
            },
            linked_issue="merge-into-smj-small-source",
            recommendation=(
                "Raise spark.sql.adaptive.autoBroadcastJoinThreshold to 50-100MB "
                "so AQE converts SMJ to BroadcastHashJoin at runtime. Verify in "
                "SQL plan that the conversion happens."
            ),
        ))
    return findings


def rule_bulkinsert_sort_none_many_partitions(
    job_stages: List[StageContext],
    hudi_table_config: Optional[Dict] = None,
) -> List[Finding]:
    """bulkinsert.sort.mode=NONE with many partitions risks W-6 OOM."""
    if hudi_table_config is None:
        return []
    sort_mode = hudi_table_config.get("hoodie.bulkinsert.sort.mode", "NONE").upper()
    if sort_mode != "NONE":
        return []
    # Look for a bulk_insert stage that touched many partitions
    for s in job_stages:
        if "bulkinsert" not in s.name.lower():
            continue
        # Approximate "many partitions" by output records / num_tasks heuristic
        # Better: read commit metadata. v1 uses a simple proxy.
        if s.num_tasks > 50:
            return [Finding(
                rule_id="bulkinsert_sort_none_many_partitions",
                severity="high",
                stage_id=s.stage_id,
                evidence={
                    "stage_name": s.name,
                    "num_tasks": s.num_tasks,
                    "current_sort_mode": "NONE",
                    "note": (
                        "bulkinsert.sort.mode=NONE keeps one parquet writer "
                        "open per partition path seen, leading to heap pressure "
                        "and OOM at high partition counts."
                    ),
                },
                linked_issue="w6-bulkinsert-sort-none",
                recommendation=(
                    "Set hoodie.bulkinsert.sort.mode=PARTITION_SORT. Closes "
                    "prior writer on partition transition. Empirically: 261s → 18s "
                    "at p=200, eliminates OOM regime."
                ),
            )]
    return []


# ──────────────────────── Rule registry ──────────────────────────────────


STAGE_RULES: List[Callable[[StageContext], Optional[Finding]]] = [
    rule_count_star_full_scan,
    rule_mdt_over_parallelization,
    rule_mor_count_skipping_footer_fast_path,
    rule_global_index_full_shuffle,
    rule_marker_handler_dominates,
    rule_shuffle_spill,
    rule_skew,
    rule_fetch_wait_dominates,
]

JOB_RULES: List[Callable] = [
    rule_delete_should_be_drop_partition,
    rule_merge_into_smj_with_small_source,
    rule_bulkinsert_sort_none_many_partitions,
]


def apply_stage_rules(ctx: StageContext) -> List[Finding]:
    findings = []
    for rule in STAGE_RULES:
        try:
            f = rule(ctx)
            if f is not None:
                findings.append(f)
        except Exception as e:
            # don't let a buggy rule crash the audit
            findings.append(Finding(
                rule_id=f"rule_error:{rule.__name__}",
                severity="low",
                stage_id=ctx.stage_id,
                evidence={"error": str(e)},
                linked_issue=None,
                recommendation="rule implementation bug — review",
            ))
    return findings


def apply_job_rules(
    job_stages: List[StageContext],
    hudi_commit: Optional[Dict] = None,
    hudi_table_config: Optional[Dict] = None,
    sql_executions: Optional[List[Dict]] = None,
) -> List[Finding]:
    findings = []
    for rule in JOB_RULES:
        try:
            # Each job rule has a different signature; route by name
            if rule.__name__ == "rule_delete_should_be_drop_partition":
                fs = rule(job_stages, hudi_commit=hudi_commit)
            elif rule.__name__ == "rule_merge_into_smj_with_small_source":
                fs = rule(job_stages, sql_executions=sql_executions)
            elif rule.__name__ == "rule_bulkinsert_sort_none_many_partitions":
                fs = rule(job_stages, hudi_table_config=hudi_table_config)
            else:
                fs = rule(job_stages)
            findings.extend(fs or [])
        except Exception as e:
            findings.append(Finding(
                rule_id=f"rule_error:{rule.__name__}",
                severity="low",
                stage_id=None,
                rule_kind="job",
                evidence={"error": str(e)},
                linked_issue=None,
                recommendation="rule implementation bug — review",
            ))
    return findings
