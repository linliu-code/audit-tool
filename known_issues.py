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

    "table-service-plan-per-partition-metadata-fetch": {
        "title": (
            "Compaction / clustering plan generators dispatch one Spark task per "
            "partition, each doing a SINGULAR metadata-table fetch — should use "
            "the batched API once on the driver"
        ),
        "upstream": "internal (H-tsp-#1, pending filing)",
        "affected_versions": "all 0.15.x through master HEAD",
        "severity": "high",
        "description": (
            "BaseHoodieCompactionPlanGenerator.generateCompactionPlan uses "
            "engineContext.flatMap(partitionPaths, ...) to parallelize "
            "per-partition file listing. The lambda calls "
            "fileSystemView.getLatestFileSlicesStateless(partition), which "
            "delegates to AbstractTableFileSystemView.getAllFilesInPartition "
            "→ tableMetadata.getAllFilesInPartition(path) — the SINGULAR API.\n"
            "\n"
            "Each Spark task therefore performs an independent metadata round "
            "trip: opens its own MDT files-partition HFile reader (S3 GET for "
            "the footer/index block), seeks + reads the data block containing "
            "the partition's key (S3 GET), and deserializes the FileStatus[] "
            "payload. The post-fetch CPU work (build file groups, filter, "
            "construct CompactionOperation) is sub-millisecond per partition.\n"
            "\n"
            "Stage signature in Spark UI:\n"
            "  - call stack mentions BaseHoodieCompactionPlanGenerator\n"
            "    .generateCompactionPlan and HoodieSparkEngineContext.flatMap\n"
            "  - numTasks == number of partitions\n"
            "  - executorRunTime sum is N × ~100ms; executorCpuTime sum is\n"
            "    tiny (CPU efficiency < 1%)\n"
            "  - inputBytes / outputBytes / shuffleReadBytes all zero (the\n"
            "    metadata reads are S3 GETs, not Spark-tracked I/O)\n"
            "\n"
            "The batched plural API "
            "HoodieTableMetadata.getAllFilesInPartitions(Collection<String>) "
            "already exists at the same metadata layer "
            "(BaseTableMetadata.java line ~153; HoodieBackedTableMetadata."
            "fetchAllFilesInPartitionPaths does ONE getRecordsByKeys range "
            "scan against the MDT files-partition for all keys). It is used "
            "by BaseHoodieTableFileIndex, but not by the compaction-plan or "
            "clustering-plan generators.\n"
            "\n"
            "Impact scales with partition count. For a 2070-partition MoR "
            "table observed in production, this stage took ~33 s of wall "
            "time at < 0.5% CPU efficiency, fired every few minutes via the "
            "compaction schedule. The expected post-fix wall time is in the "
            "1-2 s range (one batched MDT read + driver-side iteration)."
        ),
        "recommendation": (
            "Pre-warm the FileSystemView's per-partition cache with a single "
            "batched call to tableMetadata.getAllFilesInPartitions(partitionPaths) "
            "BEFORE iterating, then iterate driver-side (NOT via engineContext."
            "flatMap, since closure-serialized executor copies of the view "
            "have empty caches and would re-issue per-partition metadata "
            "fetches anyway). The same fix applies to clustering-plan "
            "generation in ClusteringPlanStrategy."
        ),
        "scaling_considerations": (
            "The signature was characterized at ~2k partitions. At higher\n"
            "scales (>100k partitions, >1M total files), there are additional\n"
            "concerns that exist BOTH before and after the batched-API fix:\n"
            "\n"
            "1. MDT files-partition HFile size grows linearly with partition\n"
            "   count. At ~100k partitions × ~5 KB/record, the MDT base file\n"
            "   alone is ~500 MB. Point lookups remain O(log N) and fast, but\n"
            "   the MDT itself needs more aggressive self-compaction or its\n"
            "   own log-file chains accumulate.\n"
            "\n"
            "2. The compaction plan Avro document becomes huge. Each\n"
            "   HoodieCompactionOperation serializes to ~1 KB; 2M operations\n"
            "   produce a ~2 GB .compaction.requested file. Hudi's plan\n"
            "   writer can struggle past a few GB. This is independent of how\n"
            "   the plan is *generated* and is the more fundamental ceiling.\n"
            "\n"
            "3. FileSystemView cache footprint scales linearly. At ~2M file\n"
            "   groups × ~200 bytes of in-memory refs = ~400 MB driver heap\n"
            "   for the cache.\n"
            "\n"
            "Before-fix-specific scaling issues (get worse as N grows):\n"
            "\n"
            "4. Wall time = N × per-task-latency ÷ effective parallelism. At\n"
            "   2M partitions × ~127 ms ÷ 4 cores = ~17.6 hours just for\n"
            "   plan generation. Even at 200 cores: ~21 minutes per run.\n"
            "   This was already significant at 2k partitions; at 2M it is a\n"
            "   complete blocker for tables in that regime.\n"
            "\n"
            "5. Potential S3 throttling at very high executor parallelism.\n"
            "   At 4 cores the workload was ~12 GET/s (well below the 5500/s\n"
            "   per-prefix limit). At 1000+ cores hitting the same MDT base\n"
            "   file, peak RPS could approach the limit and trigger 503\n"
            "   SlowDown responses + S3A backoff retries. Same-key concurrent\n"
            "   reads are S3's most throttle-tolerant pattern, so this risk is\n"
            "   real only at the upper end. Symptom: bimodal task latency\n"
            "   distribution + 'Slow Down' messages in driver logs.\n"
            "\n"
            "After-fix-specific scaling issues:\n"
            "\n"
            "6. The batched response payload grows linearly. A\n"
            "   Map<String, FileStatus[]> for 200k partitions transfers tens\n"
            "   of MB from S3 and consumes a few hundred MB of driver heap.\n"
            "   For 2M partitions, the response map alone could reach 1-2 GB\n"
            "   — risk of driver OOM if -Xmx is tight. Mitigation: chunked\n"
            "   batched fetch (split partition list into groups of ~10k,\n"
            "   process each chunk's compaction operations before freeing).\n"
            "\n"
            "7. Driver-side iteration becomes the wall-time bottleneck once\n"
            "   batched MDT removes the S3 wait. At ~0.5 ms/partition the\n"
            "   driver iterates 200k in ~100 s; 2M in ~17 minutes. Still way\n"
            "   better than the unfixed code, but no longer trivial. Mitigation:\n"
            "   stream and discard intermediate FileStatus[] payloads; keep\n"
            "   only the (much smaller) CompactionOperation POJOs.\n"
            "\n"
            "8. MDT files-partition might span multiple HFile data blocks at\n"
            "   multi-GB sizes. The 'one range read' description holds for the\n"
            "   logical operation, but multiple S3 GETs run underneath.\n"
            "\n"
            "Filtering before fetch:\n"
            "\n"
            "9. Most partitions in a very large table don't need compaction\n"
            "   this cycle. If we can identify partitions with pending log\n"
            "   files cheaply (e.g. via numLogFiles fields in the MDT files-\n"
            "   partition record summary, if exposed), we can skip dead-cold\n"
            "   partitions entirely — likely an order-of-magnitude reduction\n"
            "   in the batch size. This would be a follow-up optimization.\n"
            "\n"
            "Practical guidance:\n"
            "  - < 100k partitions: batched-API fix is a pure win.\n"
            "  - 100k - 1M partitions: still wins, but watch driver heap;\n"
            "    consider chunked batching if heap is tight.\n"
            "  - > 1M partitions: the fundamental Hudi issue is the\n"
            "    compaction-plan size itself, not the planning latency.\n"
            "    Sub-plan strategies (compact partition-by-partition or in\n"
            "    waves) become more important than the singular-vs-batched\n"
            "    distinction."
        ),
        "executor_lifecycle_interaction": (
            "On clusters that use a non-Spark-builtin executor autoscaler\n"
            "(custom autoscaler external to spark.dynamicAllocation), the\n"
            "per-run wall time of these stages is often dominated by\n"
            "executor cold-start, NOT by the Hudi work. The fix's effective\n"
            "speedup is correspondingly larger in those environments.\n"
            "\n"
            "Symptoms in the Spark UI:\n"
            "  - stage.firstTaskLaunchedTime - stage.submissionTime is on\n"
            "    the order of MINUTES (5-9 min observed)\n"
            "  - stage.completionTime - stage.firstTaskLaunchedTime is the\n"
            "    actual work, typically ~30 s for thousand-partition tables\n"
            "  - sum(task.executorRunTime) is small (~200 s sum for 1000+\n"
            "    tasks running in parallel)\n"
            "  - GET /allexecutors shows a long history of short-lived\n"
            "    executors, each living roughly one compaction-plan cycle\n"
            "  - spark.dynamicAllocation.enabled = false in spark.conf,\n"
            "    contradicting the observed executor churn — that mismatch\n"
            "    indicates an external system is sizing the pool\n"
            "\n"
            "Why this interacts so strongly with our finding: the unfixed\n"
            "code path uses engineContext.flatMap, which submits a Spark\n"
            "stage and therefore REQUIRES executors. With autoscaling that\n"
            "tears executors down during idle periods between runs, every\n"
            "compaction-plan cycle pays the executor cold-start tax\n"
            "(image pull, JVM warm-up, registration with the driver).\n"
            "\n"
            "After the batched-API fix moves iteration driver-side, this\n"
            "code path no longer needs executors at all. The autoscaler\n"
            "cold-start wait disappears entirely for this work.\n"
            "\n"
            "Empirical example from a production driver (~1000-partition\n"
            "MoR table, cold-executor case):\n"
            "  submission → first-task wait: 519.8 s\n"
            "  actual stage work:             32.7 s\n"
            "  total wall:                   552.5 s\n"
            "  expected post-fix wall:       ~1-2 s (driver-side only)\n"
            "  speedup ratio:                >250x for this single run\n"
            "\n"
            "Same driver, warm-executor case from the same window:\n"
            "  submission → first-task wait:  <1 s\n"
            "  actual stage work:              ~32 s\n"
            "  total wall:                     ~33 s\n"
            "  expected post-fix wall:         ~1-2 s\n"
            "  speedup ratio:                  ~30x\n"
            "\n"
            "Practical guidance:\n"
            "  - If your cluster uses an external Spark-executor autoscaler,\n"
            "    examine GET /allexecutors history. If individual executors\n"
            "    live ~10 min and the gap between consecutive compaction-plan\n"
            "    runs exceeds that, every run pays the cold-start tax.\n"
            "  - The audit rule fires correctly in both warm and cold cases\n"
            "    (signature: zero Spark-tracked I/O, low CPU efficiency,\n"
            "    plan-generator phase). Severity remains 'high' regardless;\n"
            "    the wall-time-reclaim estimate just gets larger in cold-\n"
            "    executor regimes."
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
