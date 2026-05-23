"""Unit tests for the detection rules.

Each rule has cases for:
  - every severity tier it can emit (fires at the right level)
  - boundary cases just under the floor (filtered, not false-positive)
  - obviously-healthy negatives (filtered)

Tests use only the Python stdlib (per DESIGN.md §2 R7) so they run with:
    python3 -m unittest discover tests
from the repo root.
"""
import os
import sys
import unittest

# Add the repo root so `import rules`, `import stage_context` work when
# tests are invoked from any working directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stage_context import StageContext  # noqa: E402
from rules import (  # noqa: E402
    rule_count_star_full_scan,
    rule_mdt_over_parallelization,
    rule_mor_count_skipping_footer_fast_path,
    rule_global_index_full_shuffle,
    rule_marker_handler_dominates,
    rule_shuffle_spill,
    rule_skew,
    rule_fetch_wait_dominates,
    rule_table_service_plan_per_partition_metadata_fetch,
)


# ──────────────────────── helpers ─────────────────────────────────────────


def make_ctx(
    *,
    stage_id=1,
    name="",
    num_tasks=100,
    executor_run_time_ms=1_000,
    executor_cpu_time_ns=0,
    input_bytes=0,
    output_bytes=0,
    shuffle_read_bytes=0,
    shuffle_write_bytes=0,
    shuffle_read_records=0,
    memory_bytes_spilled=0,
    disk_bytes_spilled=0,
    shuffle_fetch_wait_time_ms=0,
    details="",
    task_p99_ms=None,
    task_median_ms=None,
    sql_node=None,
    hudi_table_config=None,
):
    """Build a StageContext from kwargs; only set what each test needs.

    If task_p99_ms / task_median_ms are provided, synthesize a
    taskMetricsDistributions block so the StageContext's quantile-by-value
    lookup returns those exact values for p99 / median.
    """
    stage = {
        "stageId": stage_id,
        "name": name,
        "numTasks": num_tasks,
        "executorRunTime": executor_run_time_ms,
        "executorCpuTime": executor_cpu_time_ns,
        "inputBytes": input_bytes,
        "outputBytes": output_bytes,
        "shuffleReadBytes": shuffle_read_bytes,
        "shuffleWriteBytes": shuffle_write_bytes,
        "shuffleReadRecords": shuffle_read_records,
        "memoryBytesSpilled": memory_bytes_spilled,
        "diskBytesSpilled": disk_bytes_spilled,
        "shuffleFetchWaitTime": shuffle_fetch_wait_time_ms,
        "details": details,
    }
    if task_p99_ms is not None or task_median_ms is not None:
        # Spark-style quantile arrays. Use any-but-fixed set; lookup is
        # by value, not index.
        p99 = task_p99_ms if task_p99_ms is not None else 0
        med = task_median_ms if task_median_ms is not None else 0
        stage["taskMetricsDistributions"] = {
            "quantiles": [0.0, 0.5, 0.99, 1.0],
            "executorRunTime": [50.0, float(med), float(p99), float(p99) * 1.1],
        }
    ctx = StageContext(stage=stage, sql_node=sql_node, hudi_table_config=hudi_table_config)
    return ctx


def assert_fires(test, rule, ctx, expected_severity):
    """Assert the rule emits a Finding with the expected severity."""
    f = rule(ctx)
    test.assertIsNotNone(
        f,
        msg=f"{rule.__name__} expected to fire with severity={expected_severity}, "
        f"but returned None"
    )
    test.assertEqual(
        f.severity, expected_severity,
        msg=f"{rule.__name__} severity: expected={expected_severity}, got={f.severity}"
    )


def assert_filtered(test, rule, ctx, reason=""):
    """Assert the rule does NOT fire (returns None)."""
    f = rule(ctx)
    test.assertIsNone(
        f,
        msg=f"{rule.__name__} expected to be filtered out ({reason}), but fired "
        f"with severity={f.severity if f else '?'}, evidence={f.evidence if f else '?'}"
    )


# ──────────────────────── rule tests ──────────────────────────────────────


class TestCountStarFullScan(unittest.TestCase):
    SQL = {"plan": "HashAggregate count(*) on parquet table"}

    def test_high_at_1gb_plus(self):
        ctx = make_ctx(input_bytes=5 * 1024**3, shuffle_read_records=100, sql_node=self.SQL)
        assert_fires(self, rule_count_star_full_scan, ctx, "high")

    def test_medium_in_10mb_to_1gb(self):
        ctx = make_ctx(input_bytes=50 * 1024 * 1024, shuffle_read_records=100, sql_node=self.SQL)
        assert_fires(self, rule_count_star_full_scan, ctx, "medium")

    def test_low_in_1_to_10mb(self):
        ctx = make_ctx(input_bytes=5 * 1024 * 1024, shuffle_read_records=100, sql_node=self.SQL)
        assert_fires(self, rule_count_star_full_scan, ctx, "low")

    def test_filter_below_input_floor(self):
        ctx = make_ctx(input_bytes=500 * 1024, shuffle_read_records=100, sql_node=self.SQL)
        assert_filtered(self, rule_count_star_full_scan, ctx, "input < 1MB")

    def test_filter_no_sql_node(self):
        ctx = make_ctx(input_bytes=5 * 1024**3, shuffle_read_records=100, sql_node=None)
        assert_filtered(self, rule_count_star_full_scan, ctx, "no sql_node")

    def test_filter_sql_no_count(self):
        ctx = make_ctx(input_bytes=5 * 1024**3, shuffle_read_records=100,
                       sql_node={"plan": "HashAggregate sum(x)"})
        assert_filtered(self, rule_count_star_full_scan, ctx, "sql_node has no 'count'")

    def test_filter_low_bytes_per_record(self):
        # Bytes/record == 500 (below the 1000 threshold) — likely a wide-narrow read
        ctx = make_ctx(input_bytes=50 * 1024 * 1024,
                       shuffle_read_records=200_000, sql_node=self.SQL)
        assert_filtered(self, rule_count_star_full_scan, ctx, "bytes/record <= 1000")


class TestMdtOverParallelization(unittest.TestCase):
    PHASE_NAME = "metadataRecordIndex_write"  # → hudi_phase == "mdtRliWrite"

    def test_high_at_1000_tasks_small_per_task(self):
        # 2000 tasks × 5KB avg = severe
        ctx = make_ctx(name=self.PHASE_NAME, num_tasks=2000, input_bytes=2000 * 5 * 1024)
        assert_fires(self, rule_mdt_over_parallelization, ctx, "high")

    def test_medium_at_200_tasks_moderate(self):
        ctx = make_ctx(name=self.PHASE_NAME, num_tasks=500, input_bytes=500 * 30 * 1024)
        assert_fires(self, rule_mdt_over_parallelization, ctx, "medium")

    def test_low_for_small_over_parallelism(self):
        ctx = make_ctx(name=self.PHASE_NAME, num_tasks=100, input_bytes=100 * 60 * 1024)
        assert_fires(self, rule_mdt_over_parallelization, ctx, "low")

    def test_filter_healthy_per_task(self):
        ctx = make_ctx(name=self.PHASE_NAME, num_tasks=100, input_bytes=100 * 1024 * 1024)
        assert_filtered(self, rule_mdt_over_parallelization, ctx, "avg >= 100KB/task")

    def test_filter_below_task_floor(self):
        ctx = make_ctx(name=self.PHASE_NAME, num_tasks=20, input_bytes=20 * 1024)
        assert_filtered(self, rule_mdt_over_parallelization, ctx, "num_tasks < 50")

    def test_filter_non_mdt_phase(self):
        ctx = make_ctx(name="generic shuffle", num_tasks=2000, input_bytes=2000 * 5 * 1024)
        assert_filtered(self, rule_mdt_over_parallelization, ctx, "not an mdt phase")


class TestMorCountSkippingFooter(unittest.TestCase):
    MOR_CONFIG = {"hoodie.table.type": "MERGE_ON_READ"}
    COW_CONFIG = {"hoodie.table.type": "COPY_ON_WRITE"}
    SQL_COUNT = {"plan": "HashAggregate count(*)"}

    def test_fires_on_mor_count_full_read(self):
        ctx = make_ctx(input_bytes=50 * 1024 * 1024, shuffle_read_records=50,
                       hudi_table_config=self.MOR_CONFIG, sql_node=self.SQL_COUNT)
        assert_fires(self, rule_mor_count_skipping_footer_fast_path, ctx, "medium")

    def test_filter_no_table_config(self):
        ctx = make_ctx(input_bytes=50 * 1024 * 1024, shuffle_read_records=50,
                       hudi_table_config=None, sql_node=self.SQL_COUNT)
        assert_filtered(self, rule_mor_count_skipping_footer_fast_path, ctx, "no config")

    def test_filter_not_mor(self):
        ctx = make_ctx(input_bytes=50 * 1024 * 1024, shuffle_read_records=50,
                       hudi_table_config=self.COW_CONFIG, sql_node=self.SQL_COUNT)
        assert_filtered(self, rule_mor_count_skipping_footer_fast_path, ctx, "COW, not MOR")

    def test_filter_no_sql_count(self):
        ctx = make_ctx(input_bytes=50 * 1024 * 1024, shuffle_read_records=50,
                       hudi_table_config=self.MOR_CONFIG, sql_node={"plan": "Project x"})
        assert_filtered(self, rule_mor_count_skipping_footer_fast_path, ctx, "no count in plan")

    def test_filter_low_bytes_per_record(self):
        # Lots of records, bytes-per-record below threshold → indicates footer-only behavior
        ctx = make_ctx(input_bytes=50 * 1024 * 1024, shuffle_read_records=500_000,
                       hudi_table_config=self.MOR_CONFIG, sql_node=self.SQL_COUNT)
        assert_filtered(self, rule_mor_count_skipping_footer_fast_path, ctx, "bytes/rec low")


class TestGlobalIndexFullShuffle(unittest.TestCase):
    PHASE_NAME = "tagLocation step"  # → hudi_phase == "tagLocation"

    def test_high_at_10gb(self):
        ctx = make_ctx(name=self.PHASE_NAME, shuffle_write_bytes=20 * 1024**3,
                       input_bytes=100 * 1024 * 1024)
        assert_fires(self, rule_global_index_full_shuffle, ctx, "high")

    def test_medium_at_1gb(self):
        ctx = make_ctx(name=self.PHASE_NAME, shuffle_write_bytes=2 * 1024**3,
                       input_bytes=100 * 1024 * 1024)
        assert_fires(self, rule_global_index_full_shuffle, ctx, "medium")

    def test_low_at_100mb(self):
        ctx = make_ctx(name=self.PHASE_NAME, shuffle_write_bytes=500 * 1024 * 1024,
                       input_bytes=20 * 1024 * 1024)
        assert_fires(self, rule_global_index_full_shuffle, ctx, "low")

    def test_filter_below_floor(self):
        ctx = make_ctx(name=self.PHASE_NAME, shuffle_write_bytes=80 * 1024 * 1024,
                       input_bytes=20 * 1024 * 1024)
        assert_filtered(self, rule_global_index_full_shuffle, ctx, "shuffle < 100MB")

    def test_filter_low_amplification(self):
        # Healthy local index: shuffle_write ≈ input, low amplification
        ctx = make_ctx(name=self.PHASE_NAME, shuffle_write_bytes=200 * 1024 * 1024,
                       input_bytes=150 * 1024 * 1024)
        assert_filtered(self, rule_global_index_full_shuffle, ctx, "amplification <= 10")

    def test_filter_non_taglocation_phase(self):
        ctx = make_ctx(name="generic shuffle", shuffle_write_bytes=20 * 1024**3,
                       input_bytes=100 * 1024 * 1024)
        assert_filtered(self, rule_global_index_full_shuffle, ctx, "not tagLocation phase")


class TestMarkerHandlerDominates(unittest.TestCase):
    PHASE_NAME = "markerHandler something"  # → hudi_phase == "markerHandling"

    def test_high_at_30s(self):
        ctx = make_ctx(name=self.PHASE_NAME, executor_run_time_ms=60_000)
        assert_fires(self, rule_marker_handler_dominates, ctx, "high")

    def test_medium_at_5_30s(self):
        ctx = make_ctx(name=self.PHASE_NAME, executor_run_time_ms=10_000)
        assert_fires(self, rule_marker_handler_dominates, ctx, "medium")

    def test_filter_below_5s(self):
        ctx = make_ctx(name=self.PHASE_NAME, executor_run_time_ms=2_000)
        assert_filtered(self, rule_marker_handler_dominates, ctx, "duration < 5s")

    def test_filter_non_marker_phase(self):
        ctx = make_ctx(name="generic stage", executor_run_time_ms=60_000)
        assert_filtered(self, rule_marker_handler_dominates, ctx, "not marker phase")


class TestShuffleSpill(unittest.TestCase):
    def test_high_at_5gb_memory_spill(self):
        ctx = make_ctx(memory_bytes_spilled=8 * 1024**3, disk_bytes_spilled=100 * 1024 * 1024)
        assert_fires(self, rule_shuffle_spill, ctx, "high")

    def test_medium_at_100mb_memory_spill(self):
        ctx = make_ctx(memory_bytes_spilled=500 * 1024 * 1024, disk_bytes_spilled=50 * 1024 * 1024)
        assert_fires(self, rule_shuffle_spill, ctx, "medium")

    def test_filter_below_memory_floor(self):
        ctx = make_ctx(memory_bytes_spilled=50 * 1024 * 1024, disk_bytes_spilled=10 * 1024 * 1024)
        assert_filtered(self, rule_shuffle_spill, ctx, "memory_spill < 100MB")

    def test_filter_below_disk_floor(self):
        # Memory spill huge, but disk spill below floor — Spark didn't actually page
        ctx = make_ctx(memory_bytes_spilled=8 * 1024**3, disk_bytes_spilled=500 * 1024)
        assert_filtered(self, rule_shuffle_spill, ctx, "disk_spill < 1MB")

    def test_filter_no_spill(self):
        ctx = make_ctx(memory_bytes_spilled=0, disk_bytes_spilled=0)
        assert_filtered(self, rule_shuffle_spill, ctx, "no spill at all")


class TestSkew(unittest.TestCase):
    def test_high_at_p99_30s(self):
        ctx = make_ctx(num_tasks=100, task_p99_ms=60_000, task_median_ms=1_000)
        assert_fires(self, rule_skew, ctx, "high")

    def test_medium_real_skew(self):
        ctx = make_ctx(num_tasks=100, task_p99_ms=5_000, task_median_ms=300)
        assert_fires(self, rule_skew, ctx, "medium")

    def test_medium_via_median_floor(self):
        # p99 below 2s but median >= 200ms still satisfies absolute-duration gate
        ctx = make_ctx(num_tasks=100, task_p99_ms=1_500, task_median_ms=250)
        assert_fires(self, rule_skew, ctx, "medium")

    def test_filter_jit_warmup_noise(self):
        # 15x ratio but absolute durations sub-second — classic JIT warmup
        ctx = make_ctx(num_tasks=100, task_p99_ms=300, task_median_ms=20)
        assert_filtered(self, rule_skew, ctx, "p99 < 2s AND median < 200ms")

    def test_filter_few_tasks(self):
        ctx = make_ctx(num_tasks=5, task_p99_ms=60_000, task_median_ms=1_000)
        assert_filtered(self, rule_skew, ctx, "num_tasks < 10")

    def test_filter_low_skew_ratio(self):
        ctx = make_ctx(num_tasks=100, task_p99_ms=2_500, task_median_ms=1_200)
        assert_filtered(self, rule_skew, ctx, "skew_ratio < 5")


class TestFetchWaitDominates(unittest.TestCase):
    def test_high_at_30s(self):
        ctx = make_ctx(shuffle_read_bytes=100 * 1024 * 1024,
                       shuffle_fetch_wait_time_ms=60_000, executor_run_time_ms=180_000)
        assert_fires(self, rule_fetch_wait_dominates, ctx, "high")

    def test_medium_at_5_to_30s(self):
        ctx = make_ctx(shuffle_read_bytes=100 * 1024 * 1024,
                       shuffle_fetch_wait_time_ms=10_000, executor_run_time_ms=30_000)
        assert_fires(self, rule_fetch_wait_dominates, ctx, "medium")

    def test_filter_below_absolute_wait_floor(self):
        # 200ms is 33% of a 600ms stage — ratio passes but absolute wait is tiny
        ctx = make_ctx(shuffle_read_bytes=100 * 1024 * 1024,
                       shuffle_fetch_wait_time_ms=200, executor_run_time_ms=600)
        assert_filtered(self, rule_fetch_wait_dominates, ctx, "fetch_wait < 5s")

    def test_filter_low_ratio(self):
        ctx = make_ctx(shuffle_read_bytes=100 * 1024 * 1024,
                       shuffle_fetch_wait_time_ms=5_500, executor_run_time_ms=60_000)
        assert_filtered(self, rule_fetch_wait_dominates, ctx, "fetch_wait_fraction < 0.3")

    def test_filter_below_shuffle_floor(self):
        ctx = make_ctx(shuffle_read_bytes=1 * 1024 * 1024,
                       shuffle_fetch_wait_time_ms=10_000, executor_run_time_ms=30_000)
        assert_filtered(self, rule_fetch_wait_dominates, ctx, "shuffle_read < 10MB")


class TestTableServicePlanPerPartitionMetadataFetch(unittest.TestCase):
    COMPACTION_DETAILS = (
        "org.apache.spark.api.java.AbstractJavaRDDLike.collect(JavaRDDLike.scala:45)\n"
        "org.apache.hudi.client.common.HoodieSparkEngineContext.flatMap(HoodieSparkEngineContext.java:160)\n"
        "org.apache.hudi.table.action.compact.plan.generators."
        "BaseHoodieCompactionPlanGenerator.generateCompactionPlan"
        "(BaseHoodieCompactionPlanGenerator.java:122)\n"
    )

    def test_fires_on_classic_compaction_plan_pattern(self):
        ctx = make_ctx(
            details=self.COMPACTION_DETAILS,
            num_tasks=2070,
            executor_run_time_ms=263_500,
            executor_cpu_time_ns=1_100_000_000,  # 1.1s → 0.4% efficiency
        )
        assert_fires(self, rule_table_service_plan_per_partition_metadata_fetch, ctx, "high")

    def test_filter_below_task_count_floor(self):
        ctx = make_ctx(
            details=self.COMPACTION_DETAILS,
            num_tasks=10, executor_run_time_ms=263_500, executor_cpu_time_ns=1_100_000_000,
        )
        assert_filtered(
            self, rule_table_service_plan_per_partition_metadata_fetch, ctx,
            "num_tasks < 50"
        )

    def test_filter_below_wall_time_floor(self):
        ctx = make_ctx(
            details=self.COMPACTION_DETAILS,
            num_tasks=2070, executor_run_time_ms=500, executor_cpu_time_ns=1_000_000,
        )
        assert_filtered(
            self, rule_table_service_plan_per_partition_metadata_fetch, ctx,
            "duration < 2s"
        )

    def test_filter_when_io_bytes_present(self):
        ctx = make_ctx(
            details=self.COMPACTION_DETAILS,
            num_tasks=2070, executor_run_time_ms=263_500, executor_cpu_time_ns=1_100_000_000,
            input_bytes=100 * 1024 * 1024,  # has data flow — likely actual compaction, not plan
        )
        assert_filtered(
            self, rule_table_service_plan_per_partition_metadata_fetch, ctx,
            "stage has Spark-tracked I/O"
        )

    def test_filter_high_cpu_efficiency(self):
        ctx = make_ctx(
            details=self.COMPACTION_DETAILS,
            num_tasks=2070,
            executor_run_time_ms=100_000,
            executor_cpu_time_ns=80_000_000_000,  # 80s CPU on 100s wall = 80% efficiency
        )
        assert_filtered(
            self, rule_table_service_plan_per_partition_metadata_fetch, ctx,
            "cpu_efficiency >= 10%"
        )

    def test_filter_non_plan_phase(self):
        ctx = make_ctx(
            details="",  # no plan-generator class in stack
            num_tasks=2070, executor_run_time_ms=263_500, executor_cpu_time_ns=1_100_000_000,
        )
        assert_filtered(
            self, rule_table_service_plan_per_partition_metadata_fetch, ctx,
            "hudi_phase is not compactionPlan / clusteringPlan"
        )


if __name__ == "__main__":
    unittest.main()
