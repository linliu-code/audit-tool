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
    if not (ctx.sql_node and "count" in str(ctx.sql_node).lower()):
        return None
    # Compare bytes-per-record — count(*) should not read full rows
    records = max(ctx.shuffle_read_records, 1)
    bytes_per_record = ctx.input_bytes / records
    if bytes_per_record <= 1000:  # full row reads ~hundreds of bytes; footer reads <100
        return None
    # Tier by absolute read amplification cost. The bug is the same at every
    # scale, but the operational urgency tracks the bytes wasted.
    if ctx.input_bytes >= 1024 ** 3:           # >= 1 GB
        severity = "high"
    elif ctx.input_bytes >= 10 * 1024 * 1024:  # 10 MB - 1 GB
        severity = "medium"
    else:                                       # 1 - 10 MB
        severity = "low"
    return Finding(
        rule_id="count_star_full_scan",
        severity=severity,
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


def rule_mdt_over_parallelization(ctx: StageContext) -> Optional[Finding]:
    """MDT shuffle stage has many tiny tasks for the data volume."""
    if not ctx.hudi_phase.startswith("mdt"):
        return None
    if ctx.num_tasks < 50:
        return None
    avg_per_task = ctx.avg_input_per_task_bytes
    if avg_per_task >= 100 * 1024:  # > 100 KB per task = healthy
        return None
    # Tier by combined signal: severe over-parallelism is num_tasks AND
    # very-low per-task input. Modest cases get low severity so they don't
    # drown the high-impact ones.
    if ctx.num_tasks >= 1000 and avg_per_task < 10 * 1024:
        severity = "high"
    elif ctx.num_tasks >= 200 and avg_per_task < 50 * 1024:
        severity = "medium"
    else:
        severity = "low"
    return Finding(
        rule_id="mdt_over_parallelization",
        severity=severity,
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
    if ctx.shuffle_amplification <= 10:
        return None
    # Tier by absolute shuffle volume. A 200 MB amplified shuffle is the
    # same pattern as a 50 GB one but has very different operational urgency.
    if ctx.shuffle_write_bytes >= 10 * 1024 ** 3:        # >= 10 GB
        severity = "high"
    elif ctx.shuffle_write_bytes >= 1024 ** 3:           # 1 GB - 10 GB
        severity = "medium"
    else:                                                 # 100 MB - 1 GB
        severity = "low"
    return Finding(
        rule_id="global_index_full_shuffle",
        severity=severity,
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


def rule_marker_handler_dominates(ctx: StageContext) -> Optional[Finding]:
    """Stages spent in marker management indicate W-6 territory."""
    if ctx.hudi_phase != "markerHandling":
        return None
    # Require a material duration before flagging: 1s of marker work on a
    # multi-minute commit isn't worth a finding. Tier by absolute time.
    if ctx.duration_ms < 5 * 1000:
        return None
    if ctx.duration_ms >= 30 * 1000:       # >= 30 s
        severity = "high"
    else:                                   # 5 - 30 s
        severity = "medium"
    return Finding(
        rule_id="marker_handler_dominates",
        severity=severity,
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


def rule_shuffle_spill(ctx: StageContext) -> Optional[Finding]:
    """Shuffle stage with material disk spill, indicating reduce-side memory pressure.

    Reports only material spills — Spark spills tiny amounts routinely during
    normal hash-aggregation flushes (a few KB), which is not a problem and
    fires false-positive findings without a minimum threshold.

    Tiers:
      - >= 5 GB memory_spill  → severity high (OOM-territory, urgent)
      - 100 MB < x < 5 GB     → severity medium (real pressure)
      - <  100 MB             → don't fire (normal flush behavior)
    """
    MIN_DISK_SPILL = 1 * 1024 * 1024     # 1 MB — must have actually paged to disk
    MIN_MEMORY_SPILL = 100 * 1024 * 1024  # 100 MB — must be a material amount
    HIGH_MEMORY_SPILL = 5 * 1024 * 1024 * 1024  # 5 GB → high severity

    if ctx.disk_spill_bytes < MIN_DISK_SPILL:
        return None
    if ctx.memory_spill_bytes < MIN_MEMORY_SPILL:
        return None

    severity = "high" if ctx.memory_spill_bytes >= HIGH_MEMORY_SPILL else "medium"
    return Finding(
        rule_id="shuffle_spill",
        severity=severity,
        stage_id=ctx.stage_id,
        evidence={
            "memory_spill_bytes": ctx.memory_spill_bytes,
            "disk_spill_bytes": ctx.disk_spill_bytes,
            "hudi_phase": ctx.hudi_phase,
            "stage_name": ctx.name,
        },
        linked_issue=None,
        recommendation=(
            "Reduce-side memory pressure. The shuffle's per-task working set "
            "exceeded the spill threshold and paged to disk. Options: raise "
            "spark.executor.memory; raise spark.memory.fraction; lower "
            "spark.reducer.maxSizeInFlight; raise shuffle.partitions to make "
            "per-task data smaller. For multi-GB spills, the root cause is "
            "often skewed shuffle keys or a wrong-scale aggregation (e.g. "
            "global index countByKey on a large dataset for a small upsert)."
        ),
    )


def rule_skew(ctx: StageContext) -> Optional[Finding]:
    """Long-tail task distribution within a stage.

    Requires both relative skew (p99/median ratio) AND absolute duration
    above floor thresholds, to avoid firing on JIT-warmup artifacts at
    small scale (the canonical false positive: p99=300ms vs median=20ms
    is a 15x ratio but the absolute work is sub-second).
    """
    if ctx.num_tasks < 10:
        return None
    if ctx.skew_ratio < 5:
        return None
    p99 = ctx.task_duration_p99_ms
    med = ctx.task_duration_median_ms
    # Absolute-duration gate: only flag when the long tail is actually painful
    if p99 < 2_000 and med < 200:
        return None
    # Tier severity by the absolute long-tail cost
    if p99 >= 30_000:
        severity = "high"
    else:
        severity = "medium"
    return Finding(
        rule_id="task_skew",
        severity=severity,
        stage_id=ctx.stage_id,
        evidence={
            "num_tasks": ctx.num_tasks,
            "task_duration_p99_ms": p99,
            "task_duration_median_ms": med,
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
    """Stage is fetch-bound rather than compute-bound.

    Requires both a meaningful absolute wait time AND the wait fraction to
    avoid firing on tiny stages where a 100ms fetch wait happens to be 30%
    of a 300ms stage — technically matches the ratio but is operationally
    irrelevant.
    """
    if ctx.shuffle_read_bytes < 10 * 1024 * 1024:
        return None
    if ctx.fetch_wait_fraction < 0.3:
        return None
    if ctx.fetch_wait_time_ms < 5_000:
        return None
    # Tier by absolute fetch-wait time
    if ctx.fetch_wait_time_ms >= 30_000:
        severity = "high"
    else:                                # 5-30 s
        severity = "medium"
    return Finding(
        rule_id="fetch_wait_dominates",
        severity=severity,
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


def rule_table_service_plan_per_partition_metadata_fetch(ctx: StageContext) -> Optional[Finding]:
    """Table-service planning (compaction / clustering) dispatched one Spark
    task per partition, with each task calling the singular metadata-table
    listing API. Every task does ~2 S3 GETs against the metadata HFile and
    near-zero CPU work; the batched plural API would collapse N round trips
    into 1.

    Signature:
      - stage is a compaction-plan or clustering-plan flatMap (via hudi_phase)
      - numTasks is large (typically equals partition count)
      - CPU efficiency is very low (waiting on S3 GETs, not computing)
      - Spark-accounted I/O is zero (the metadata reads don't register as
        Spark inputBytes / shuffleReadBytes)
      - per-task duration is small but adds up across many tasks

    Tunables:
      - MIN_TASKS: only fire for genuinely partition-heavy tables
      - MAX_CPU_EFFICIENCY: 0.10 = stage was waiting >90% of executor time
      - MIN_WALL_MS: don't fire on incidental short stages
    """
    MIN_TASKS = 50
    MAX_CPU_EFFICIENCY = 0.10
    MIN_WALL_MS = 2_000
    if ctx.hudi_phase not in ("compactionPlan", "clusteringPlan"):
        return None
    if ctx.num_tasks < MIN_TASKS:
        return None
    if ctx.duration_ms < MIN_WALL_MS:
        return None
    if ctx.has_any_io_bytes:
        return None
    if ctx.cpu_efficiency >= MAX_CPU_EFFICIENCY:
        return None
    return Finding(
        rule_id="table_service_plan_per_partition_metadata_fetch",
        severity="high",
        stage_id=ctx.stage_id,
        evidence={
            "hudi_phase": ctx.hudi_phase,
            "num_tasks": ctx.num_tasks,
            "executor_run_time_ms": ctx.duration_ms,
            "executor_cpu_time_ms": ctx.executor_cpu_time_ns // 1_000_000,
            "cpu_efficiency": round(ctx.cpu_efficiency, 4),
            "input_bytes": ctx.input_bytes,
            "shuffle_read_bytes": ctx.shuffle_read_bytes,
            "task_p99_ms": ctx.task_duration_p99_ms,
            "task_median_ms": ctx.task_duration_median_ms,
        },
        linked_issue="table-service-plan-per-partition-metadata-fetch",
        recommendation=(
            "The compaction-plan / clustering-plan generator parallelizes one "
            "Spark task per partition, and each task does its own singular "
            "metadata-table listing call (~2 S3 GETs against the MDT files "
            "partition HFile). The batched plural API "
            "(HoodieTableMetadata.getAllFilesInPartitions) already exists and "
            "would do one range read for all partitions; it just isn't used "
            "by the plan generator. Fix: pre-load all partitions into the "
            "FileSystemView cache via the batched API on the driver, then "
            "iterate driver-side instead of via engineContext.flatMap (the "
            "warmed cache lives on the driver only, so closure-serialized "
            "executor copies would discard it)."
        ),
    )


# Stage phases that already have a specific, more-actionable rule. Skip them
# from the generic detector to avoid duplicate findings on the same stage.
_SPECIFIC_PHASES_HANDLED_ELSEWHERE = ("compactionPlan", "clusteringPlan")


def rule_metadata_bound_stage(ctx: StageContext) -> Optional[Finding]:
    """Generic detector for "wait but not I/O" stages — a stage with many
    parallel tasks doing almost no CPU work and producing no Spark-tracked
    I/O bytes is almost certainly waiting on RPC / metadata-store / S3-LIST
    calls outside Spark's I/O accounting.

    This rule is the **methodology lesson** from the compaction-plan
    investigation distilled into code: that finding hinged on three signals
    co-occurring (high numTasks × low cpu_efficiency × zero Spark I/O bytes).
    Future variants of the same class of issue — workload-profile collects,
    archive scans, MDT bootstrap, glue-sync, etc. — share the signature.

    Severity is `low` by design: the rule is a candidate-for-investigation
    flag, not a confirmed issue. Specific rules (e.g.
    rule_table_service_plan_per_partition_metadata_fetch) catch known
    variants with `high` severity; this rule catches the next one before
    anyone has written a specific rule for it.

    Skips stages already covered by a more-specific rule (matched by
    hudi_phase) so the same stage doesn't produce duplicate findings.
    """
    # Skip phases that have their own dedicated rule
    if ctx.hudi_phase in _SPECIFIC_PHASES_HANDLED_ELSEWHERE:
        return None
    # Need genuine fan-out — single-task stages aren't the pattern
    if ctx.num_tasks < 50:
        return None
    # Don't fire on trivially-short stages
    if ctx.duration_ms < 2_000:
        return None
    # Stage must be CPU-starved (waiting on something external)
    if ctx.cpu_efficiency >= 0.10:
        return None
    # And must have NO Spark-tracked I/O (else it's data work, not metadata work)
    if ctx.has_any_io_bytes:
        return None
    return Finding(
        rule_id="metadata_bound_stage",
        severity="low",
        stage_id=ctx.stage_id,
        evidence={
            "num_tasks": ctx.num_tasks,
            "executor_run_time_ms": ctx.duration_ms,
            "executor_cpu_time_ms": ctx.executor_cpu_time_ns // 1_000_000,
            "cpu_efficiency": round(ctx.cpu_efficiency, 4),
            "hudi_phase": ctx.hudi_phase,
            "stage_name": ctx.name,
        },
        linked_issue="metadata-bound-stage-generic",
        recommendation=(
            "This stage spent ~all of its time waiting on RPC / metadata-store / "
            "S3-LIST calls (very low CPU, zero Spark-tracked I/O). Examine the "
            "stage's `details` call stack to identify the Hudi (or other) class "
            "responsible and check whether a batched alternative API exists. "
            "Common culprits: per-partition file listing, per-file MDT lookups, "
            "per-table catalog sync, archive scans. If you identify a specific "
            "code path that should batch its calls, file a follow-up and add a "
            "dedicated detection rule to rules.py."
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

    Severity tiers by cumulative tagLocation shuffle_write_bytes (a proxy
    for the size of the delete, and therefore the magnitude of the wasted
    work the per-row path is doing):
      - >= 1 GB    -> high   (big delete; 100x speedup is operationally real)
      - 100MB-1GB  -> medium
      - < 100 MB   -> filter (the 100x speedup is sub-second; not actionable)
    """
    if not hudi_commit:
        return []
    op_type = hudi_commit.get("operationType", "").lower()
    if op_type != "delete":
        return []
    # Look for tagLocation + write stages (per-row delete path)
    tag_stages = [s for s in job_stages if s.hudi_phase == "tagLocation"]
    if not tag_stages:
        return []
    total_tag_shuffle = sum(s.shuffle_write_bytes for s in tag_stages)
    # Floor: tiny deletes aren't worth flagging
    if total_tag_shuffle < 100 * 1024 * 1024:
        return []
    # Tier by absolute size of the per-row path's work
    if total_tag_shuffle >= 1024 ** 3:  # >= 1 GB
        severity = "high"
    else:                                # 100 MB - 1 GB
        severity = "medium"
    return [Finding(
        rule_id="delete_should_be_drop_partition",
        severity=severity,
        stage_id=None,
        rule_kind="job",
        evidence={
            "hudi_operation": op_type,
            "has_taglocation_stage": True,
            "tag_shuffle_write_bytes_total": total_tag_shuffle,
            "tag_stage_count": len(tag_stages),
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
    bytes below ~100 MB suggests source could have been broadcast but wasn't.

    Severity tiers (smaller source = stronger BHJ candidate = higher urgency):
      - < 10 MB    -> high   (well under AQE's default broadcast threshold)
      - 10 - 50 MB -> medium (likely BHJ-eligible with raised threshold)
      - 50 - 100MB -> low    (borderline; raising threshold may help)
    Floor: shuffle_write < 1 MB filtered out (empty-source SMJ is a degenerate
    case, not a useful finding).
    """
    findings = []
    MIN_SHUFFLE = 1 * 1024 * 1024
    for s in job_stages:
        if s.hudi_phase != "mergeSourceJoin":
            continue
        if "sortmerge" not in s.name.lower():
            continue
        if s.shuffle_write_bytes > 100 * 1024 * 1024:
            continue  # source side genuinely large, SMJ is correct
        if s.shuffle_write_bytes < MIN_SHUFFLE:
            continue  # degenerate: source effectively empty
        if s.shuffle_write_bytes < 10 * 1024 * 1024:
            severity = "high"
        elif s.shuffle_write_bytes < 50 * 1024 * 1024:
            severity = "medium"
        else:                                  # 50 - 100 MB
            severity = "low"
        findings.append(Finding(
            rule_id="merge_into_smj_small_source",
            severity=severity,
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
    """bulkinsert.sort.mode=NONE with many partitions risks W-6 OOM.

    Severity tiers by num_tasks (partition-count proxy). OOM risk scales
    with the number of distinct partition paths a single task may see
    open writers for at once:
      - > 500 tasks  -> high   (definite OOM territory; matches the
                                empirically-known p=200+ regime)
      - 100 - 500    -> medium (still risky)
      - 50 - 100     -> low    (bug exists; small datasets tend not to OOM)
    """
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
        if s.num_tasks <= 50:
            continue
        if s.num_tasks > 500:
            severity = "high"
        elif s.num_tasks > 100:
            severity = "medium"
        else:                              # 51 - 100
            severity = "low"
        return [Finding(
            rule_id="bulkinsert_sort_none_many_partitions",
            severity=severity,
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
    rule_table_service_plan_per_partition_metadata_fetch,
    rule_metadata_bound_stage,
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
