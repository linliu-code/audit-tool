"""Unit tests for StageContext — currently focused on hudi_phase
classification, which the v1.1 expansion enriched from ~13 labels to ~25
covering the major Hudi action-executor paths.

For phase matching from `name`: provide a stage with a Hudi-descriptive
name (the common case when Hudi sets one).
For phase matching from `details` (call stack): provide a stage with a
generic name but the action-executor's full class in the stack.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stage_context import StageContext, _PHASE_LABELS  # noqa: E402


# ──────────────────────── helpers ─────────────────────────────────────────


def stage(*, name="", details=""):
    """Build a StageContext with the minimum fields needed for phase
    classification."""
    return StageContext(stage={
        "stageId": 1,
        "name": name,
        "numTasks": 10,
        "executorRunTime": 1000,
        "executorCpuTime": 0,
        "inputBytes": 0,
        "outputBytes": 0,
        "shuffleReadBytes": 0,
        "shuffleWriteBytes": 0,
        "shuffleReadRecords": 0,
        "memoryBytesSpilled": 0,
        "diskBytesSpilled": 0,
        "shuffleFetchWaitTime": 0,
        "details": details,
    })


# ──────────────────────── tests for name-based detection ──────────────────


class TestHudiPhaseFromName(unittest.TestCase):
    """Original name-based matchers preserved across the refactor."""

    def test_tag_location(self):
        self.assertEqual(stage(name="tagLocation step 1").hudi_phase, "tagLocation")

    def test_bloom_index(self):
        self.assertEqual(stage(name="bloomIndex lookup").hudi_phase, "tagLocation")

    def test_mdt_record_index(self):
        self.assertEqual(stage(name="metadataRecordIndex_write").hudi_phase, "mdtRliWrite")

    def test_mdt_record_index_via_rli_alias(self):
        # `rli` substring also matches the RLI label
        self.assertEqual(stage(name="rli_partition_write").hudi_phase, "mdtRliWrite")

    def test_mdt_col_stats(self):
        self.assertEqual(stage(name="metadataColumnStats write").hudi_phase, "mdtColStatsWrite")

    def test_mdt_bloom_filter(self):
        self.assertEqual(stage(name="metadataBloomFilter write").hudi_phase, "mdtBloomWrite")

    def test_mdt_partition_stats(self):
        self.assertEqual(stage(name="partition_stats").hudi_phase, "mdtPartitionStatsWrite")

    def test_mdt_generic(self):
        self.assertEqual(stage(name="metadataTable write").hudi_phase, "mdtWrite")

    def test_workload_profile(self):
        self.assertEqual(stage(name="workloadProfile collect").hudi_phase, "workloadProfile")

    def test_base_file_write_via_bulkinsert(self):
        self.assertEqual(stage(name="doBulkInsert").hudi_phase, "baseFileWrite")

    def test_base_file_write_via_upsert(self):
        self.assertEqual(stage(name="doUpsert").hudi_phase, "baseFileWrite")

    def test_file_group_shuffle(self):
        # name normalization strips underscores
        self.assertEqual(stage(name="repartition by file_group").hudi_phase, "fileGroupShuffle")

    def test_marker_handling(self):
        self.assertEqual(stage(name="markerHandler create").hudi_phase, "markerHandling")

    def test_merge_source_join(self):
        self.assertEqual(stage(name="SortMergeJoin merge_source").hudi_phase, "mergeSourceJoin")

    def test_empty_name_returns_unknown(self):
        self.assertEqual(stage(name="", details="").hudi_phase, "unknown")


# ──────────────────────── tests for call-stack-based detection ────────────


class TestHudiPhaseFromDetails(unittest.TestCase):
    """Call-stack-based matchers — the v1.1 expansion. Stage has a generic
    name (the kind Spark emits when Hudi doesn't override it) but the call
    stack carries the action-executor's class name."""

    GENERIC_NAME = "collect at HoodieSparkEngineContext.java:160"

    def _details(self, *classes):
        """Build a multi-line stack with the given classes interleaved with
        some generic frames, like a real stage `details` field."""
        frames = [
            "org.apache.spark.api.java.AbstractJavaRDDLike.collect(JavaRDDLike.scala:45)",
            "org.apache.hudi.client.common.HoodieSparkEngineContext.flatMap(...)",
        ]
        for c in classes:
            frames.append(f"org.apache.hudi.x.{c}(file.java:1)")
        return "\n".join(frames)

    # ── Compaction ──────────────────────────────────────────────────────
    def test_compaction_plan_via_base_class(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("BaseHoodieCompactionPlanGenerator"))
        self.assertEqual(s.hudi_phase, "compactionPlan")

    def test_compaction_plan_via_schedule_executor(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("ScheduleCompactionActionExecutor"))
        self.assertEqual(s.hudi_phase, "compactionPlan")

    def test_compaction_execute(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("RunCompactionActionExecutor"))
        self.assertEqual(s.hudi_phase, "compactionExecute")

    # ── Clustering ──────────────────────────────────────────────────────
    def test_clustering_plan_via_strategy(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("SparkSizeBasedClusteringPlanStrategy"))
        self.assertEqual(s.hudi_phase, "clusteringPlan")

    def test_clustering_plan_via_executor(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("ClusteringPlanActionExecutor"))
        self.assertEqual(s.hudi_phase, "clusteringPlan")

    def test_clustering_execute(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("SparkExecuteClusteringCommitActionExecutor"))
        self.assertEqual(s.hudi_phase, "clusteringExecute")

    # ── Clean ───────────────────────────────────────────────────────────
    def test_clean_plan(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("CleanPlanActionExecutor"))
        self.assertEqual(s.hudi_phase, "cleanPlan")

    def test_clean_planner_alt(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("CleanPlanner"))
        self.assertEqual(s.hudi_phase, "cleanPlan")

    def test_clean_execute(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("HoodieCleanActionExecutor"))
        self.assertEqual(s.hudi_phase, "cleanExecute")

    # ── Rollback ────────────────────────────────────────────────────────
    def test_rollback_via_base_executor(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("BaseRollbackActionExecutor"))
        self.assertEqual(s.hudi_phase, "rollback")

    def test_rollback_via_helper(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("BaseRollbackHelper"))
        self.assertEqual(s.hudi_phase, "rollback")

    # ── Archive ─────────────────────────────────────────────────────────
    def test_archive(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("HoodieTimelineArchiver"))
        self.assertEqual(s.hudi_phase, "archive")

    # ── Bootstrap ───────────────────────────────────────────────────────
    def test_bootstrap_via_job_operator(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("HoodieBootstrapJobOperator"))
        self.assertEqual(s.hudi_phase, "bootstrap")

    # ── Catalog sync ────────────────────────────────────────────────────
    def test_glue_sync(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("AWSGlueCatalogSyncClient"))
        self.assertEqual(s.hudi_phase, "glueSync")

    def test_hive_sync(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("HiveSyncTool"))
        self.assertEqual(s.hudi_phase, "hiveSync")

    def test_hive_sync_via_ddl_executor(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("HMSDDLExecutor"))
        self.assertEqual(s.hudi_phase, "hiveSync")

    # ── DS source fetches ───────────────────────────────────────────────
    def test_kafka_source(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("KafkaSource"))
        self.assertEqual(s.hudi_phase, "kafkaSourceFetch")

    def test_jdbc_source(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("JdbcSource"))
        self.assertEqual(s.hudi_phase, "jdbcSourceFetch")

    def test_incremental_source(self):
        s = stage(name=self.GENERIC_NAME, details=self._details("S3EventsHoodieIncrSource"))
        self.assertEqual(s.hudi_phase, "incrementalSourceFetch")

    # ── MDT writes via call stack ───────────────────────────────────────
    def test_mdt_write_via_metadata_writer_class(self):
        s = stage(name=self.GENERIC_NAME,
                  details=self._details("HoodieBackedTableMetadataWriter"))
        self.assertEqual(s.hudi_phase, "mdtWrite")


# ──────────────────────── precedence tests ────────────────────────────────


class TestHudiPhasePrecedence(unittest.TestCase):
    """When both signals are present, name-based detection wins (it's the
    more specific signal). Also: pattern order within details matters when
    multiple class names happen to be on the same stack."""

    def test_name_wins_over_details(self):
        # A stage explicitly named 'tagLocation' wins over a call stack
        # that might also mention a compaction-plan class (e.g. an
        # interleaved table-service path).
        s = stage(name="tagLocation step",
                  details="org.apache.hudi.x.BaseHoodieCompactionPlanGenerator(...)")
        self.assertEqual(s.hudi_phase, "tagLocation")

    def test_glue_wins_over_hive_when_both_in_stack(self):
        # AWSGlueCatalogSyncClient often inherits from HiveSyncTool in
        # Onehouse's setup; the Glue-specific pattern must win.
        s = stage(
            name="collect at HoodieSparkEngineContext.java:160",
            details=(
                "org.apache.hudi.aws.sync.AWSGlueCatalogSyncClient(...)\n"
                "org.apache.hudi.hive.HiveSyncTool(...)\n"
            ),
        )
        self.assertEqual(s.hudi_phase, "glueSync")

    def test_unknown_when_neither_matches(self):
        s = stage(name="random stage", details="org.apache.spark.scheduler.X")
        self.assertEqual(s.hudi_phase, "unknown")


class TestPhaseLabelsCatalog(unittest.TestCase):
    """The catalog set is just a documentation aid, but verify it contains
    every label that the helpers actually emit, so future additions don't
    silently drift apart."""

    def test_all_emitted_labels_are_in_catalog(self):
        from stage_context import _phase_from_name, _phase_from_details
        # Probe every known phrase: synthesize stages from each known
        # name-pattern and details-pattern, collect every distinct label
        # they emit, and verify the catalog superset.
        emitted = set()
        # Walk a handful of known matches to extract labels
        for nm in [
            "tagLocation", "metadataRecordIndex", "metadataColumnStats",
            "metadataBloomFilter", "metadataPartitionStats", "metadataTable",
            "workloadProfile", "doBulkInsert", "repartition by file_group",
            "markerHandler", "SortMergeJoin",
        ]:
            r = _phase_from_name(nm.lower())
            if r is not None:
                emitted.add(r)
        # And the details patterns
        from stage_context import _DETAILS_CLASS_PATTERNS
        for _, label in _DETAILS_CLASS_PATTERNS:
            emitted.add(label)
        # Every emitted label must be in the catalog
        missing = emitted - _PHASE_LABELS
        self.assertEqual(missing, set(), msg=f"labels emitted but not in catalog: {missing}")


if __name__ == "__main__":
    unittest.main()
