#!/usr/bin/env python3
"""Hudi Inefficiency Audit — CLI entry point.

Connects to a Spark UI endpoint (or reads an event log), pulls per-stage
metrics, runs detection rules, and emits a structured report identifying
Hudi-implementation inefficiencies.

USAGE:
  python audit.py --spark-ui http://localhost:4040 --app-id app-2026...
  python audit.py --spark-ui http://localhost:4040 --app-id app-2026... \\
                  --hudi-path /path/to/hudi/table
  python audit.py --spark-ui ... --app-id ... --json
  python audit.py --spark-ui ... --app-id ... --job-id 47
"""
import argparse
import sys
from typing import Dict, List, Optional

from client import SparkUIClient, HudiMetaReader, detect_hudi_version_from_env
from stage_context import build_stage_contexts, extract_hudi_version_from_table_config
from rules import apply_stage_rules, apply_job_rules, Finding
from report import format_text, format_json


def run_audit(args) -> int:
    client = SparkUIClient(args.spark_ui, args.app_id)

    # Connectivity test
    try:
        app = client.get_app_info()
    except Exception as e:
        print(f"ERROR: could not reach Spark UI at {args.spark_ui} for app "
              f"{args.app_id}: {e}", file=sys.stderr)
        return 2

    env = client.get_environment()
    hudi_version = detect_hudi_version_from_env(env)

    hudi_table_config: Optional[Dict] = None
    hudi_meta: Optional[HudiMetaReader] = None
    if args.hudi_path:
        try:
            hudi_meta = HudiMetaReader(args.hudi_path)
            hudi_table_config = hudi_meta.get_table_config()
            if not hudi_version:
                hudi_version = extract_hudi_version_from_table_config(hudi_table_config)
        except FileNotFoundError as e:
            print(f"WARN: hudi-path is not a Hudi table: {e}", file=sys.stderr)

    # Pull jobs + stages
    all_jobs = client.get_jobs()
    if args.job_id is not None:
        all_jobs = [j for j in all_jobs if j.get("jobId") == args.job_id]

    all_stages = client.get_stages()
    stages_by_id = {s["stageId"]: s for s in all_stages}

    # SQL plans (optional)
    sql_executions = client.get_sql()

    # Build job-to-stages mapping
    job_to_stage_ids: Dict[int, List[int]] = {}
    for j in all_jobs:
        job_to_stage_ids[j["jobId"]] = j.get("stageIds", [])

    # Find Hudi commit metadata for time-correlated commits (best-effort)
    job_to_hudi_commit: Dict[int, Optional[Dict]] = {}
    if hudi_meta:
        commits = hudi_meta.list_commits()[:50]  # most recent 50
        # No clean job-to-commit mapping from Spark UI; for v1 we pass the
        # most recent commit as a hint. A future version could use stage names
        # like "doDelete at ..." to match.
        latest_commit_data = None
        for c in commits:
            if c["action"] == "commit":
                data = hudi_meta.read_commit(c["instant_time"], c["action"])
                if data:
                    latest_commit_data = data
                    break
        for jid in job_to_stage_ids:
            job_to_hudi_commit[jid] = latest_commit_data

    # Apply rules per job
    findings_by_job: Dict[int, List[Finding]] = {}
    job_info: Dict[int, Dict] = {}

    for j in all_jobs:
        jid = j["jobId"]
        job_info[jid] = {
            "name": j.get("name", ""),
            "duration_ms": _job_duration_ms(j),
            "status": j.get("status"),
            "stage_ids": job_to_stage_ids.get(jid, []),
        }

        # Build stage contexts for stages in this job
        stages_in_job = [stages_by_id[sid] for sid in job_to_stage_ids.get(jid, [])
                          if sid in stages_by_id]
        if not stages_in_job:
            continue

        ctxs = build_stage_contexts(
            stages_in_job,
            hudi_table_config=hudi_table_config,
            hudi_version=hudi_version,
            sql_executions=sql_executions,
        )

        findings: List[Finding] = []
        for ctx in ctxs:
            for f in apply_stage_rules(ctx):
                f.job_id = jid
                findings.append(f)
        for f in apply_job_rules(
            ctxs,
            hudi_commit=job_to_hudi_commit.get(jid),
            hudi_table_config=hudi_table_config,
            sql_executions=sql_executions,
        ):
            f.job_id = jid
            findings.append(f)

        findings_by_job[jid] = findings

    # Emit report
    if args.json:
        print(format_json(findings_by_job, job_info, args.app_id, hudi_version))
    else:
        print(format_text(findings_by_job, job_info, args.app_id, hudi_version))

    # Exit code: non-zero if any high-severity findings (for CI integration)
    has_high = any(f.severity == "high" for fs in findings_by_job.values() for f in fs)
    return 1 if has_high and args.fail_on_high else 0


def _job_duration_ms(j: Dict) -> Optional[int]:
    """Compute job duration from submission/completion times if present."""
    sub = j.get("submissionTime")
    comp = j.get("completionTime")
    if not sub or not comp:
        return None
    # Times are ISO strings; cheap parse
    from datetime import datetime
    try:
        t0 = datetime.strptime(sub.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z")
        t1 = datetime.strptime(comp.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z")
        return int((t1 - t0).total_seconds() * 1000)
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(
        description="Identify Hudi-implementation inefficiencies in a Spark application's jobs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--spark-ui", required=True,
                   help="Spark UI base URL, e.g. http://localhost:4040")
    p.add_argument("--app-id", required=True,
                   help="Spark application ID (from /api/v1/applications)")
    p.add_argument("--hudi-path", default=None,
                   help="Optional path to the Hudi table for commit-metadata enrichment")
    p.add_argument("--job-id", type=int, default=None,
                   help="Audit only the specified Spark job ID")
    p.add_argument("--json", action="store_true",
                   help="Emit JSON instead of human-readable text")
    p.add_argument("--fail-on-high", action="store_true",
                   help="Exit code 1 if any high-severity findings detected (for CI)")
    args = p.parse_args()
    return run_audit(args)


if __name__ == "__main__":
    sys.exit(main())
