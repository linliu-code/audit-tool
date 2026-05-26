"""StageContext — assembles per-stage information that rules need to make
decisions.

A StageContext bundles:
  - the raw stage JSON from Spark UI
  - derived metrics (durations, ratios)
  - heuristic classification (Hudi phase: tagLocation / fileGroupShuffle / MDTwrite / etc.)
  - optional SQL plan node info
  - optional Hudi commit metadata for the surrounding job
"""
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ──────────────────────── Hudi phase classification ──────────────────────
#
# Hudi stages can be identified by either:
#   - the descriptive stage `name` set by Hudi (e.g. "tagLocation")
#   - the call-stack `details` carrying a Hudi class name when the stage
#     name is generic (e.g. "collect at HoodieSparkEngineContext.java:160")
#
# Many Hudi action-executor stages fall into the second bucket — they all
# look identical by `name` but the call-stack discriminates which action
# they're executing.

# Phase labels emitted by hudi_phase. Listed here for documentation /
# discoverability; nothing in the runtime path consults this set.
_PHASE_LABELS = {
    "tagLocation",            # write-path index lookup
    "fileGroupShuffle",       # file-group repartition
    "baseFileWrite",          # bulk_insert / upsert / insert writing parquet
    "workloadProfile",        # WorkloadProfile collection on the driver
    "markerHandling",         # marker creation / list
    "mergeSourceJoin",        # MERGE INTO's source-side join (user query)
    "commit",                 # commit finalization
    # MDT (Metadata Table) writes
    "mdtWrite",
    "mdtRliWrite",
    "mdtColStatsWrite",
    "mdtBloomWrite",
    "mdtPartitionStatsWrite",
    # Table-service planning + execution
    "compactionPlan",
    "compactionExecute",
    "clusteringPlan",
    "clusteringExecute",
    "cleanPlan",
    "cleanExecute",
    "rollback",
    "archive",
    # Bootstrap / catalog-sync paths
    "bootstrap",              # Hudi's own initial-load bootstrap
    "hiveSync",               # Hive metastore sync
    "glueSync",               # AWS Glue catalog sync
    # DS source reads (data flowing IN, not Hudi-internal work)
    "kafkaSourceFetch",
    "jdbcSourceFetch",
    "incrementalSourceFetch",
    "unknown",
}


def _phase_from_name(name: str) -> Optional[str]:
    """Match phase from the descriptive stage name. Returns None when the
    name doesn't carry a recognizable phase signal."""
    n = (name or "").lower()
    if not n:
        return None
    # tagLocation / index lookups on the write path
    if "taglocation" in n or "bloomindex" in n or "simpleindex" in n:
        return "tagLocation"
    # MDT writes — order matters; specific labels first, then generic mdt
    if "metadatapartitionstats" in n or "partition_stats" in n:
        return "mdtPartitionStatsWrite"
    if "metadatarecordindex" in n or "record_index" in n or "rli" in n:
        return "mdtRliWrite"
    if "metadatacolumnstats" in n or "column_stats" in n or "col_stats" in n:
        return "mdtColStatsWrite"
    if "metadatabloomfilter" in n or "bloom_filter" in n:
        return "mdtBloomWrite"
    if "metadatatable" in n or "metadatawriter" in n:
        return "mdtWrite"
    if "workloadprofile" in n:
        return "workloadProfile"
    if "dobulkinsert" in n or "bulkinsert" in n:
        return "baseFileWrite"
    if "doupsert" in n or "doinsert" in n:
        return "baseFileWrite"
    if "repartition" in n and "filegroup" in n.replace("_", ""):
        return "fileGroupShuffle"
    if "markerhandler" in n or "createmarker" in n:
        return "markerHandling"
    if "sortmergejoin" in n or "broadcasthashjoin" in n or "shuffledhashjoin" in n:
        return "mergeSourceJoin"
    return None


# Call-stack class-name patterns. Match the FIRST entry that has its class
# substring present in `details`. Ordered so that more-specific patterns
# precede more-general ones (e.g. PartitionAware before generic Compaction).
_DETAILS_CLASS_PATTERNS = [
    # ── Compaction ──────────────────────────────────────────────────────
    ("CompactionPlanGenerator", "compactionPlan"),
    ("ScheduleCompactionActionExecutor", "compactionPlan"),
    ("RunCompactionActionExecutor", "compactionExecute"),
    ("HoodieSparkMergeOnReadTableCompactor", "compactionExecute"),
    # ── Clustering ──────────────────────────────────────────────────────
    ("ClusteringPlanStrategy", "clusteringPlan"),
    ("ClusteringPlanActionExecutor", "clusteringPlan"),
    ("ScheduleClusteringActionExecutor", "clusteringPlan"),
    ("RunClusteringActionExecutor", "clusteringExecute"),
    ("SparkExecuteClusteringCommitActionExecutor", "clusteringExecute"),
    # ── Clean ───────────────────────────────────────────────────────────
    ("CleanPlanActionExecutor", "cleanPlan"),
    ("CleanPlanner", "cleanPlan"),
    ("HoodieCleanActionExecutor", "cleanExecute"),
    # ── Rollback ────────────────────────────────────────────────────────
    ("BaseRollbackActionExecutor", "rollback"),
    ("BaseRollbackHelper", "rollback"),
    ("RollbackActionExecutor", "rollback"),
    # ── Archive ─────────────────────────────────────────────────────────
    ("HoodieTimelineArchiver", "archive"),
    # ── Bootstrap ───────────────────────────────────────────────────────
    ("HoodieBootstrapJobOperator", "bootstrap"),
    ("BootstrapActionExecutor", "bootstrap"),
    # ── Catalog sync ────────────────────────────────────────────────────
    # Glue-specific FIRST (it would also match HiveSync* generically)
    ("AWSGlueCatalogSyncClient", "glueSync"),
    ("GlueSyncTool", "glueSync"),
    ("HiveSyncTool", "hiveSync"),
    ("HMSDDLExecutor", "hiveSync"),
    ("HiveQueryDDLExecutor", "hiveSync"),
    # ── DS source fetches ───────────────────────────────────────────────
    # These represent user-data flowing IN, not Hudi-internal work.
    ("S3EventsHoodieIncrSource", "incrementalSourceFetch"),
    ("HoodieIncrSource", "incrementalSourceFetch"),
    ("KafkaSource", "kafkaSourceFetch"),
    ("JdbcSource", "jdbcSourceFetch"),
    # ── MDT writes (call-stack form) ────────────────────────────────────
    # When MDT writes use a generic stage name, the writer class names them.
    ("HoodieBackedTableMetadataWriter", "mdtWrite"),
]


def _phase_from_details(details: str) -> Optional[str]:
    """Match phase from the stage call-stack `details`. Returns None when
    no recognizable class appears. Earlier patterns take priority."""
    if not details:
        return None
    for class_substring, label in _DETAILS_CLASS_PATTERNS:
        if class_substring in details:
            return label
    return None


@dataclass
class StageContext:
    # raw inputs
    stage: Dict
    sql_node: Optional[Dict] = None
    hudi_commit: Optional[Dict] = None
    hudi_table_config: Optional[Dict] = None
    hudi_version: Optional[str] = None
    # derived
    derived: Dict = field(default_factory=dict)

    @property
    def stage_id(self) -> int:
        return self.stage.get("stageId", -1)

    @property
    def name(self) -> str:
        return self.stage.get("name", "")

    @property
    def num_tasks(self) -> int:
        return self.stage.get("numTasks", 0)

    @property
    def duration_ms(self) -> int:
        return self.stage.get("executorRunTime", 0)

    @property
    def input_bytes(self) -> int:
        return self.stage.get("inputBytes", 0)

    @property
    def shuffle_read_bytes(self) -> int:
        return self.stage.get("shuffleReadBytes", 0)

    @property
    def shuffle_write_bytes(self) -> int:
        return self.stage.get("shuffleWriteBytes", 0)

    @property
    def shuffle_read_records(self) -> int:
        return self.stage.get("shuffleReadRecords", 0)

    @property
    def shuffle_write_records(self) -> int:
        return self.stage.get("shuffleWriteRecords", 0)

    @property
    def memory_spill_bytes(self) -> int:
        return self.stage.get("memoryBytesSpilled", 0)

    @property
    def disk_spill_bytes(self) -> int:
        return self.stage.get("diskBytesSpilled", 0)

    @property
    def fetch_wait_time_ms(self) -> int:
        return self.stage.get("shuffleFetchWaitTime", 0)

    @property
    def executor_cpu_time_ns(self) -> int:
        """Sum of CPU time across all tasks of this stage. From Spark's
        `executorCpuTime` field (nanoseconds). Use cpu_efficiency for the
        ratio against wall executor time."""
        return self.stage.get("executorCpuTime", 0)

    @property
    def cpu_efficiency(self) -> float:
        """Fraction of executor wall time that was actual CPU work.
        Returns 1.0 when there's no measurable executor time (avoids
        divide-by-zero false-positives on trivially short stages).

        Low values (<5-10%) typically indicate the stage is bottlenecked
        on external I/O — most commonly S3 GETs to listing-style metadata
        when the work itself is per-row trivial."""
        if self.duration_ms <= 0:
            return 1.0
        return (self.executor_cpu_time_ns / 1_000_000) / self.duration_ms

    @property
    def details(self) -> str:
        """Stage call-stack / details field from Spark UI. Multi-line string
        with one frame per line; rules can match on class/method substrings
        to identify which Hudi code path drove the stage."""
        return self.stage.get("details", "") or ""

    @property
    def has_any_io_bytes(self) -> bool:
        """True if the stage moved any bytes through Spark's accounted
        I/O metrics (input, output, shuffle). False here is a strong hint
        that the per-task work is purely metadata RPC / S3 GETs that don't
        register as Spark I/O."""
        return (
            self.input_bytes > 0
            or self.shuffle_read_bytes > 0
            or self.shuffle_write_bytes > 0
            or self.stage.get("outputBytes", 0) > 0
        )

    @property
    def task_duration_p99_ms(self) -> int:
        v = self._task_metric_quantile("executorRunTime", 0.99)
        return v if v is not None else self._avg_task_duration_ms()

    @property
    def task_duration_median_ms(self) -> int:
        v = self._task_metric_quantile("executorRunTime", 0.5)
        return v if v is not None else self._avg_task_duration_ms()

    def _task_metric_quantile(self, metric: str, quantile: float) -> Optional[int]:
        """Look up a quantile of a per-task metric by VALUE (not array index).

        Spark's `taskMetricsDistributions` returns a `quantiles` array (e.g.
        [0.0, 0.25, 0.5, 0.75, 1.0] by default, or [0.0, 0.5, 0.95, 0.99, 1.0]
        if the client requested those explicitly) paired with same-length
        per-metric value arrays. The previous implementation hard-coded array
        indices (assumed 9 quantiles, got 5) and silently fell back to a wrong
        value, producing 1000x+ false skew findings. This version finds the
        index whose quantile value is closest to the requested one, with a
        small tolerance.

        Returns None if distributions are not populated (most commonly: the
        stage was fetched via the /stages list endpoint which does not
        include taskMetricsDistributions). Caller can populate them by
        calling SparkUIClient.get_stage_distributions() and merging into
        self.stage under the "taskMetricsDistributions" key.
        """
        summary = self.stage.get("taskMetricsDistributions") or {}
        quantiles = summary.get("quantiles") or []
        values = summary.get(metric) or []
        if not quantiles or len(quantiles) != len(values):
            return None
        idx = min(range(len(quantiles)), key=lambda i: abs(quantiles[i] - quantile))
        # Tolerance: only accept if the requested quantile is close to an
        # available one. Keeps p99 from silently degrading to max or p75.
        if abs(quantiles[idx] - quantile) > 0.05:
            return None
        return int(values[idx])

    def _avg_task_duration_ms(self) -> int:
        """Fallback when distributions are unavailable: average per-task time.
        Returns the same value for p99 and median, so skew_ratio == 1.0 and
        the task_skew rule does not fire on stages with missing distribution
        data (correct false-negative rather than the prior 1000x false-positive)."""
        if self.num_tasks > 0:
            return self.duration_ms // self.num_tasks
        return self.duration_ms

    @property
    def hudi_phase(self) -> str:
        """Heuristic classification of this stage's Hudi-attributable phase.

        Tried in order:
          1. Pattern-match on stage `name` (cheap; works when Hudi sets a
             descriptive name like 'tagLocation' / 'metadataRecordIndex').
          2. Pattern-match on stage `details` call stack (works when Hudi
             generates a generic stage name like
             'collect at HoodieSparkEngineContext.java:160' but the call
             stack carries the action-executor class).

        Returns one of the labels listed in _PHASE_LABELS below, or
        'unknown' if neither signal matched.
        """
        from_name = _phase_from_name(self.name)
        if from_name is not None:
            return from_name
        from_details = _phase_from_details(self.details)
        if from_details is not None:
            return from_details
        return "unknown"

    @property
    def is_hudi_internal(self) -> bool:
        """Whether this stage is doing Hudi-internal work (vs user query)."""
        return self.hudi_phase not in ("unknown", "mergeSourceJoin")

    @property
    def avg_input_per_task_bytes(self) -> float:
        if self.num_tasks == 0:
            return 0
        # Prefer shuffle_read_bytes for shuffle stages, input_bytes for read stages
        bytes_in = max(self.shuffle_read_bytes, self.input_bytes)
        return bytes_in / self.num_tasks

    @property
    def skew_ratio(self) -> float:
        if self.task_duration_median_ms <= 0:
            return 1.0
        return self.task_duration_p99_ms / self.task_duration_median_ms

    @property
    def fetch_wait_fraction(self) -> float:
        if self.duration_ms <= 0:
            return 0.0
        return self.fetch_wait_time_ms / self.duration_ms

    @property
    def shuffle_amplification(self) -> float:
        """shuffle_write / input. Skip if input == 0."""
        if self.input_bytes <= 0:
            return 0.0
        return self.shuffle_write_bytes / self.input_bytes


def build_stage_contexts(
    stages: List[Dict],
    hudi_table_config: Optional[Dict] = None,
    hudi_version: Optional[str] = None,
    sql_executions: Optional[List[Dict]] = None,
    spark_client=None,
    min_tasks_for_distributions: int = 3,
    min_executor_run_time_ms_for_distributions: int = 500,
) -> List[StageContext]:
    """Build a StageContext per stage. Cross-links SQL plan nodes when possible.

    SQL plan cross-linking: each SQL execution has a stage list; we match by
    job ID. For v1 we do a simple lookup by stage ID across all SQL execs.

    If `spark_client` is provided, this also fetches per-stage
    `taskMetricsDistributions` for stages large enough to benefit
    (numTasks >= min_tasks_for_distributions and executorRunTime >=
    min_executor_run_time_ms_for_distributions), and merges them into the
    stage dict. This is required for the `task_skew` rule and any other
    quantile-based rule to produce correct findings, because the /stages list
    endpoint does NOT return distributions.
    """
    sql_node_by_stage_id: Dict[int, Dict] = {}
    if sql_executions:
        for ex in sql_executions:
            for node in ex.get("nodes", []):
                # nodes in Spark UI's /sql endpoint contain accumulator-driven
                # metrics. Stage IDs are not directly in nodes; we'd need to
                # parse the plan more deeply. v1 punt: leave empty.
                pass

    # Augment stages with taskMetricsDistributions when the caller gave us
    # a client. We mutate each stage dict in place by adding the key.
    if spark_client is not None:
        for s in stages:
            if "taskMetricsDistributions" in s:
                continue  # already populated
            if s.get("numTasks", 0) < min_tasks_for_distributions:
                continue
            if s.get("executorRunTime", 0) < min_executor_run_time_ms_for_distributions:
                continue
            stage_id = s.get("stageId")
            attempt_id = s.get("attemptId", 0)
            if stage_id is None:
                continue
            dist = spark_client.get_stage_distributions(stage_id, attempt_id)
            if dist:
                s["taskMetricsDistributions"] = dist

    return [
        StageContext(
            stage=s,
            hudi_table_config=hudi_table_config,
            hudi_version=hudi_version,
            sql_node=sql_node_by_stage_id.get(s.get("stageId", -1)),
        )
        for s in stages
    ]


def extract_hudi_version_from_table_config(config: Dict) -> Optional[str]:
    """Read the writer-version from hoodie.properties if present."""
    v = config.get("hoodie.table.initial.version")
    if v:
        return f"table_version_{v}"
    return None
