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
    def task_duration_p99_ms(self) -> int:
        """Approximate p99 task duration from the summary if available."""
        # statusTracker provides this; if absent, fall back to executor time
        summary = self.stage.get("taskMetricsDistributions") or {}
        durations = summary.get("executorRunTime") or []
        if durations and len(durations) >= 9:
            return int(durations[8])  # quantile index for p99
        return self.duration_ms

    @property
    def task_duration_median_ms(self) -> int:
        summary = self.stage.get("taskMetricsDistributions") or {}
        durations = summary.get("executorRunTime") or []
        if durations and len(durations) >= 5:
            return int(durations[4])
        return self.duration_ms // max(self.num_tasks, 1)

    @property
    def hudi_phase(self) -> str:
        """Heuristic classification of this stage's Hudi-attributable phase.
        Returns one of: 'tagLocation', 'fileGroupShuffle', 'baseFileWrite',
        'mdtWrite', 'mdtRliWrite', 'mdtColStatsWrite', 'mdtBloomWrite',
        'workloadProfile', 'commit', 'mergeSourceJoin', 'unknown'."""
        n = self.name.lower()
        if "taglocation" in n or "bloomindex" in n or "simpleindex" in n:
            return "tagLocation"
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
        if "doupsert" in n:
            return "baseFileWrite"
        if "doinsert" in n:
            return "baseFileWrite"
        if "repartition" in n and "filegroup" in n.replace("_", ""):
            return "fileGroupShuffle"
        if "markerhandler" in n or "createmarker" in n:
            return "markerHandling"
        if "sortmergejoin" in n or "broadcasthashjoin" in n or "shuffledhashjoin" in n:
            return "mergeSourceJoin"
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
) -> List[StageContext]:
    """Build a StageContext per stage. Cross-links SQL plan nodes when possible.

    SQL plan cross-linking: each SQL execution has a stage list; we match by
    job ID. For v1 we do a simple lookup by stage ID across all SQL execs.
    """
    sql_node_by_stage_id: Dict[int, Dict] = {}
    if sql_executions:
        for ex in sql_executions:
            for node in ex.get("nodes", []):
                # nodes in Spark UI's /sql endpoint contain accumulator-driven
                # metrics. Stage IDs are not directly in nodes; we'd need to
                # parse the plan more deeply. v1 punt: leave empty.
                pass

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
