"""Catalog of known Hudi inefficiencies, with cross-references to upstream
issues and internal investigation artifacts. The rule engine looks up
findings against this catalog to enrich the report.

To add a new entry: append a dict to KNOWN_ISSUES. Use a stable ID; the
rule engine joins by ID.
"""

KNOWN_ISSUES = {
    # ────── apache/hudi PRs / issues ──────────────────────────────────────
    "hudi-18769-count-star": {
        "title": "SELECT count(*) on COW reads full file content instead of parquet footers",
        "upstream": "apache/hudi#18769 (issue) / #18770 (fix PR — open)",
        "affected_versions": "all 0.15.x through master HEAD",
        "severity": "high",
        "description": (
            "count(*) on COW tables routes through the standard read path, "
            "materializing every row instead of using parquet's footer-only "
            "row count. Read amplification: 234–285× bytes vs raw parquet at "
            "the same scale. Reproduces on all Hudi versions tested."
        ),
        "recommendation": (
            "Watch apache/hudi#18770 for merge. Workaround: query raw parquet "
            "files for count if performance-critical."
        ),
    },

    "hudi-18806-reconcile-blocks-promotion": {
        "title": "reconcile.schema=true blocks documented int→long / int→double promotion",
        "upstream": "apache/hudi#18806 (repro PR — open)",
        "affected_versions": "0.15.x through master HEAD (long-standing, not a regression)",
        "severity": "medium",
        "description": (
            "When hoodie.datasource.write.reconcile.schema=true, the validation "
            "path rejects type promotions that the docs list as supported. "
            "Default mode (false) accepts them. Cannot drop a column AND promote "
            "a type in the same write."
        ),
        "recommendation": (
            "If CDC events include type promotions, leave reconcile.schema=false. "
            "Route drop-column events through SQL ALTER TABLE instead."
        ),
    },

    "hudi-18807-colstats-add-column": {
        "title": "MDT col_stats auto-extend behavior on ADD COLUMN — codification",
        "upstream": "apache/hudi#18807 (test-only PR — open)",
        "affected_versions": "current master (codifies expected behavior)",
        "severity": "low",
        "description": (
            "Default mode auto-extends col_stats to new columns. Explicit "
            "column.list mode does NOT auto-extend — silent data-skipping "
            "regression on the new column unless the list is updated."
        ),
        "recommendation": (
            "When using an explicit column.list, extend it whenever a new "
            "column is added at the source."
        ),
    },

    "hudi-18810-bytes-string-crash": {
        "title": "bytes → string promotion crashes data-skipping read with ClassCastException",
        "upstream": "apache/hudi#18810 (repro PR — open)",
        "affected_versions": "1.1.0+ (regression — works in 0.15.x through 1.0.2)",
        "severity": "high",
        "description": (
            "bytes→string is documented as supported. Empirically, reading "
            "with data-skipping enabled after this promotion throws "
            "java.lang.ClassCastException (HeapByteBuffer cannot be cast to "
            "[B). Regression introduced in 1.1.0."
        ),
        "recommendation": (
            "Disable data-skipping for affected tables until upstream fix. "
            "OR: INSERT_OVERWRITE backfill to remove the mixed-type stats."
        ),
    },

    "hudi-18792-colstats-nan-truncation": {
        "title": "Col-stats corruption from NaN / value-size truncation",
        "upstream": "apache/hudi#18792 (PR — open)",
        "affected_versions": "0.15.x through master HEAD",
        "severity": "high",
        "description": (
            "(a) NaN in FLOAT/DOUBLE columns corrupts col-stats (min/max=0.0). "
            "(b) Stats-truncated columns mis-record nullCount=valueCount. Both "
            "cause silent zero-row query results."
        ),
        "recommendation": "Land apache/hudi#18792.",
    },

    "hudi-18794-timestamp-output-type": {
        "title": "Spark write path ignores outputTimestampType (#18752)",
        "upstream": "apache/hudi#18794 (PR — open)",
        "affected_versions": "0.15.x through master HEAD",
        "severity": "medium",
        "description": (
            "Spark write path silently ignores both spark.sql.parquet.outputTimestampType "
            "and hoodie.parquet.outputtimestamptype, always emitting TIMESTAMP(MICROS) "
            "for TimestampType. Breaks downstream readers expecting MILLIS or INT96."
        ),
        "recommendation": "Land apache/hudi#18794.",
    },

    # ────── Internal hypotheses pending filing ────────────────────────────
    "w6-marker-batch-interval": {
        "title": "Timeline-server marker batch interval default 50ms paid serially",
        "upstream": "internal (H-w6-#1, pending filing)",
        "affected_versions": "default Hudi 1.x",
        "severity": "medium",
        "description": (
            "Per-partition cost is ~25ms inherent + interval_ms. For "
            "par=1 multi-partition local-FS writes, this dominates wall. "
            "Phase 3 of W-6 probe showed DIRECT markers cut slope to 25ms."
        ),
        "recommendation": (
            "Lower hoodie.markerBatchIntervalMs to 10ms, OR use DIRECT markers "
            "(hoodie.write.markers.type=DIRECT)."
        ),
    },

    "w6-bulkinsert-sort-none": {
        "title": "bulkinsert.sort.mode=NONE causes OOM at high partition counts",
        "upstream": "internal (H-w6-#2, pending filing)",
        "affected_versions": "default Hudi 1.x",
        "severity": "high",
        "description": (
            "NONE keeps N parquet writers open per Spark task (one per partition "
            "path). Heap pressure with MDT + TS-marker state → OOM at p=200. "
            "PARTITION_SORT closes prior writer on partition transition: 261s→18s."
        ),
        "recommendation": (
            "Set hoodie.bulkinsert.sort.mode=PARTITION_SORT for any partitioned "
            "bulk_insert with > 50 partitions."
        ),
    },

    "r7-mor-count-no-logs": {
        "title": "MOR count(*) doesn't use footer fast-path even for slices with no log files",
        "upstream": "internal (H-r7-#1, pending filing)",
        "affected_versions": "all (gate `!isMOR` at HoodieFileGroupReaderBasedFileFormat.scala:288)",
        "severity": "medium",
        "description": (
            "count(*) fast path is gated on !isMOR, but MOR file slices with NO "
            "log files could safely use the footer path. Current code reads 2× "
            "COW bytes for identical workload."
        ),
        "recommendation": (
            "Widen the isCount gate to allow MOR slices with no log files."
        ),
    },

    "r2-file-size-bloat": {
        "title": "Per-file metadata bloat — Hudi files carry ~440 KB extra per file",
        "upstream": "W-r2-#3 (no upstream filing; structural)",
        "affected_versions": "all 0.15.x through master HEAD",
        "severity": "low",  # not fixable in isolation
        "description": (
            "Hudi base files carry bloom filter + embedded col-stats + 5 "
            "_hoodie_* meta-fields per row. At low-row-count regimes, per-file "
            "overhead is ~440 KB. Surfaces as R-1, R-5, X-3 cost contributions."
        ),
        "recommendation": (
            "Structural — no isolated fix. Mitigations: larger files (raise "
            "max.file.size), MDT col_stats instead of per-file (Layer-3 of "
            "metadata-only-query roadmap)."
        ),
    },

    # ────── Generic Hudi missed-opts ──────────────────────────────────────
    "delete-should-be-drop-partition": {
        "title": "DELETE with partition-only predicate not routed to delete_partition",
        "upstream": "internal observation (matches design discussion 2026-05-20)",
        "affected_versions": "all 0.15.x through master HEAD",
        "severity": "high",  # huge perf delta when applicable
        "description": (
            "DELETE FROM t WHERE <partition-col-predicate> goes through per-row "
            "tagging + tombstone writes instead of the metadata-only "
            "delete_partition operation. 100×+ slower when applicable."
        ),
        "recommendation": (
            "Rewrite the DML to ALTER TABLE ... DROP PARTITION or CALL "
            "drop_partition(...). OR file a Hudi analyzer-rule PR that "
            "auto-converts when predicate references only partition columns."
        ),
    },

    "global-simple-index-small-upsert": {
        "title": "GLOBAL_SIMPLE index causes full-table shuffle for small upsert",
        "upstream": "internal observation (matches index discussion)",
        "affected_versions": "all (config choice)",
        "severity": "high",
        "description": (
            "GLOBAL_SIMPLE shuffles the existing-records side across all "
            "partitions. For a 100GB table with a 1MB upsert, moves 100GB to "
            "do a 1MB write."
        ),
        "recommendation": (
            "Switch to local SIMPLE (same-partition) if cross-partition "
            "key uniqueness isn't required. OR use RECORD_INDEX for true "
            "global lookup at lower cost."
        ),
    },

    "merge-into-smj-small-source": {
        "title": "MERGE INTO uses SortMergeJoin despite source being broadcast-eligible",
        "upstream": "Spark AQE configuration",
        "affected_versions": "all when AQE threshold < actual source size",
        "severity": "medium",
        "description": (
            "MERGE INTO source-target join defaults to SMJ, requiring full "
            "shuffle of both sides. With AQE enabled and source < threshold, "
            "should convert to BHJ — but the threshold (default 10MB) often "
            "blocks the conversion."
        ),
        "recommendation": (
            "Raise spark.sql.adaptive.autoBroadcastJoinThreshold to 50-100MB. "
            "Verify the conversion happened by checking SQL plan tab for "
            "BroadcastHashJoin instead of SortMergeJoin."
        ),
    },

    "mdt-over-parallelization": {
        "title": "MDT shuffle stages over-parallelized for the workload size",
        "upstream": "internal observation (F-DML-TUNING, 2026-05-22)",
        "affected_versions": "all when *.parallelism > workload size warrants",
        "severity": "medium",
        "description": (
            "MDT shuffle parallelism configs (record.index.parallelism etc.) "
            "default to 200 or 500. For small workloads, tasks become tiny "
            "(<100 records each), and task scheduling overhead dominates "
            "actual record processing. Empirically reproduces 1.4-1.7× "
            "slowdown when applying 'production baseline' tuning at small scale."
        ),
        "recommendation": (
            "For workloads < 1M records, lower hoodie.metadata.*.parallelism "
            "to ~50-100. Rule of thumb: target 5000-50000 records per task."
        ),
    },
}


def lookup(issue_id):
    """Return the issue dict by ID, or a placeholder if unknown."""
    return KNOWN_ISSUES.get(issue_id, {
        "title": f"(unknown issue: {issue_id})",
        "upstream": None,
        "severity": "low",
        "description": "",
        "recommendation": "",
    })
