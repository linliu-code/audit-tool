"""Unit tests for job-level rules (rules that look at all stages of a job).

Same shape as `test_rules.py`: positive cases per severity tier, boundary
cases just under the floor, obviously-healthy negatives. Stdlib only.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stage_context import StageContext  # noqa: E402
from rules import (  # noqa: E402
    rule_delete_should_be_drop_partition,
    rule_merge_into_smj_with_small_source,
    rule_bulkinsert_sort_none_many_partitions,
)


# ──────────────────────── helpers ─────────────────────────────────────────


def mk_stage(
    *,
    stage_id=1,
    name="",
    num_tasks=10,
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
):
    """Build a single StageContext with sensible defaults."""
    return StageContext(stage={
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
    })


def assert_finding(test, findings, expected_severity, expected_rule_id):
    """Assert exactly one finding emitted with the expected severity + rule_id."""
    test.assertEqual(
        len(findings), 1,
        msg=f"expected exactly 1 finding, got {len(findings)}: "
        f"{[(f.rule_id, f.severity) for f in findings]}"
    )
    f = findings[0]
    test.assertEqual(f.rule_id, expected_rule_id)
    test.assertEqual(
        f.severity, expected_severity,
        msg=f"severity: expected={expected_severity}, got={f.severity}, "
        f"evidence={f.evidence}"
    )


def assert_no_findings(test, findings, reason=""):
    test.assertEqual(
        findings, [],
        msg=f"expected no findings ({reason}), got "
        f"{[(f.rule_id, f.severity) for f in findings]}"
    )


# ──────────────────────── rule tests ──────────────────────────────────────


class TestDeleteShouldBeDropPartition(unittest.TestCase):
    DELETE_COMMIT = {"operationType": "delete"}

    def _tag_stage(self, shuffle_write):
        # name contains "taglocation" → hudi_phase resolves to tagLocation
        return mk_stage(name="taglocation step",
                        shuffle_write_bytes=shuffle_write)

    def test_high_at_1gb_tag_shuffle(self):
        stages = [self._tag_stage(2 * 1024**3)]  # 2 GB
        findings = rule_delete_should_be_drop_partition(stages, hudi_commit=self.DELETE_COMMIT)
        assert_finding(self, findings, "high", "delete_should_be_drop_partition")

    def test_medium_in_100mb_to_1gb(self):
        stages = [self._tag_stage(500 * 1024 * 1024)]
        findings = rule_delete_should_be_drop_partition(stages, hudi_commit=self.DELETE_COMMIT)
        assert_finding(self, findings, "medium", "delete_should_be_drop_partition")

    def test_medium_via_multiple_stages_summed(self):
        # Two 60 MB tagLocation stages → sum 120 MB → medium
        stages = [self._tag_stage(60 * 1024 * 1024), self._tag_stage(60 * 1024 * 1024)]
        findings = rule_delete_should_be_drop_partition(stages, hudi_commit=self.DELETE_COMMIT)
        assert_finding(self, findings, "medium", "delete_should_be_drop_partition")

    def test_filter_below_100mb_floor(self):
        # 50 MB total — too small to warrant a finding
        stages = [self._tag_stage(50 * 1024 * 1024)]
        findings = rule_delete_should_be_drop_partition(stages, hudi_commit=self.DELETE_COMMIT)
        assert_no_findings(self, findings, "below 100 MB total tag shuffle floor")

    def test_filter_no_commit(self):
        stages = [self._tag_stage(2 * 1024**3)]
        findings = rule_delete_should_be_drop_partition(stages, hudi_commit=None)
        assert_no_findings(self, findings, "no hudi_commit")

    def test_filter_non_delete_operation(self):
        stages = [self._tag_stage(2 * 1024**3)]
        findings = rule_delete_should_be_drop_partition(stages,
                                                        hudi_commit={"operationType": "upsert"})
        assert_no_findings(self, findings, "operation != delete")

    def test_filter_no_taglocation_stage(self):
        # Has a stage but it's not tagLocation phase
        stages = [mk_stage(name="generic", shuffle_write_bytes=2 * 1024**3)]
        findings = rule_delete_should_be_drop_partition(stages, hudi_commit=self.DELETE_COMMIT)
        assert_no_findings(self, findings, "no tagLocation stage")


class TestMergeIntoSmjWithSmallSource(unittest.TestCase):
    def _smj_stage(self, shuffle_write):
        return mk_stage(name="sortMergeJoin stuff", shuffle_write_bytes=shuffle_write)

    def test_high_under_10mb(self):
        stages = [self._smj_stage(5 * 1024 * 1024)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_finding(self, findings, "high", "merge_into_smj_small_source")

    def test_medium_at_10_to_50mb(self):
        stages = [self._smj_stage(30 * 1024 * 1024)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_finding(self, findings, "medium", "merge_into_smj_small_source")

    def test_low_at_50_to_100mb(self):
        stages = [self._smj_stage(80 * 1024 * 1024)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_finding(self, findings, "low", "merge_into_smj_small_source")

    def test_filter_above_100mb(self):
        # Large enough that SMJ is the correct choice
        stages = [self._smj_stage(500 * 1024 * 1024)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_no_findings(self, findings, "source >= 100 MB; SMJ correct")

    def test_filter_empty_source(self):
        # Closes the previously-existing 0-byte hole
        stages = [self._smj_stage(0)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_no_findings(self, findings, "0-byte source — degenerate, not actionable")

    def test_filter_below_1mb_floor(self):
        stages = [self._smj_stage(500 * 1024)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_no_findings(self, findings, "shuffle < 1 MB floor")

    def test_filter_non_merge_phase(self):
        # No mergeSourceJoin phase
        stages = [mk_stage(name="generic shuffle", shuffle_write_bytes=5 * 1024 * 1024)]
        findings = rule_merge_into_smj_with_small_source(stages)
        assert_no_findings(self, findings, "not mergeSourceJoin")


class TestBulkinsertSortNoneManyPartitions(unittest.TestCase):
    SORT_NONE_CONFIG = {"hoodie.bulkinsert.sort.mode": "NONE"}
    SORT_PARTITION_CONFIG = {"hoodie.bulkinsert.sort.mode": "PARTITION_SORT"}

    def _bulk_stage(self, num_tasks):
        return mk_stage(name="bulkinsert step", num_tasks=num_tasks)

    def test_high_above_500_tasks(self):
        stages = [self._bulk_stage(1000)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages,
                                                             hudi_table_config=self.SORT_NONE_CONFIG)
        assert_finding(self, findings, "high", "bulkinsert_sort_none_many_partitions")

    def test_medium_at_100_to_500_tasks(self):
        stages = [self._bulk_stage(250)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages,
                                                             hudi_table_config=self.SORT_NONE_CONFIG)
        assert_finding(self, findings, "medium", "bulkinsert_sort_none_many_partitions")

    def test_low_at_50_to_100_tasks(self):
        stages = [self._bulk_stage(75)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages,
                                                             hudi_table_config=self.SORT_NONE_CONFIG)
        assert_finding(self, findings, "low", "bulkinsert_sort_none_many_partitions")

    def test_filter_below_50_tasks(self):
        stages = [self._bulk_stage(20)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages,
                                                             hudi_table_config=self.SORT_NONE_CONFIG)
        assert_no_findings(self, findings, "num_tasks <= 50")

    def test_filter_no_config(self):
        stages = [self._bulk_stage(1000)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages, hudi_table_config=None)
        assert_no_findings(self, findings, "no hudi_table_config")

    def test_filter_correct_sort_mode(self):
        stages = [self._bulk_stage(1000)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages,
                                                             hudi_table_config=self.SORT_PARTITION_CONFIG)
        assert_no_findings(self, findings, "sort.mode != NONE")

    def test_filter_non_bulkinsert_stage(self):
        # stage exists but isn't a bulkinsert one
        stages = [mk_stage(name="generic", num_tasks=1000)]
        findings = rule_bulkinsert_sort_none_many_partitions(stages,
                                                             hudi_table_config=self.SORT_NONE_CONFIG)
        assert_no_findings(self, findings, "no bulkinsert stage")


if __name__ == "__main__":
    unittest.main()
