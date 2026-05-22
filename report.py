"""Report formatters — plain text + JSON."""
import json as json_mod
from typing import Dict, List, Optional

from known_issues import lookup
from rules import Finding


def _severity_sort_key(f: Finding) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(f.severity, 3)


def format_text(
    findings_by_job: Dict[int, List[Finding]],
    job_info: Dict[int, Dict],
    app_id: str,
    hudi_version: Optional[str] = None,
) -> str:
    """Build a human-readable text report."""
    lines = []
    lines.append("=" * 78)
    lines.append(f"Hudi Inefficiency Audit  —  app: {app_id}")
    if hudi_version:
        lines.append(f"Hudi version: {hudi_version}")
    lines.append("=" * 78)
    lines.append("")

    total = sum(len(f) for f in findings_by_job.values())
    sev_counts = {"high": 0, "medium": 0, "low": 0}
    for fs in findings_by_job.values():
        for f in fs:
            sev_counts[f.severity] = sev_counts.get(f.severity, 0) + 1
    lines.append(
        f"SUMMARY: {total} finding(s) across {len(findings_by_job)} job(s) — "
        f"{sev_counts.get('high', 0)} high, {sev_counts.get('medium', 0)} medium, "
        f"{sev_counts.get('low', 0)} low"
    )
    lines.append("")

    if total == 0:
        lines.append("No Hudi inefficiencies detected by the current rule set.")
        lines.append("")
        return "\n".join(lines)

    for job_id in sorted(findings_by_job.keys()):
        fs = findings_by_job[job_id]
        if not fs:
            continue
        info = job_info.get(job_id, {})
        duration = info.get("duration_ms")
        name = info.get("name", "")
        lines.append("-" * 78)
        line = f"JOB {job_id}"
        if duration:
            line += f"  ({duration/1000:.1f}s)"
        if name:
            line += f"  — {name[:60]}"
        lines.append(line)
        lines.append("-" * 78)

        for f in sorted(fs, key=_severity_sort_key):
            sev_marker = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(f.severity, "  ")
            stage_ref = f"stage {f.stage_id}" if f.stage_id is not None else "(job-level)"
            lines.append("")
            lines.append(f"  {sev_marker} [{f.severity.upper()}]  {f.rule_id}  @ {stage_ref}")
            if f.linked_issue:
                issue = lookup(f.linked_issue)
                lines.append(f"     Known issue:  {issue.get('title', f.linked_issue)}")
                if issue.get("upstream"):
                    lines.append(f"     Upstream:     {issue['upstream']}")
                if issue.get("affected_versions"):
                    lines.append(f"     Affects:      {issue['affected_versions']}")
            lines.append("     Evidence:")
            for k, v in f.evidence.items():
                if k == "note":
                    lines.append(f"        note: {v}")
                else:
                    lines.append(f"        {k}: {v}")
            lines.append(f"     Recommendation:")
            for rec_line in f.recommendation.split(". "):
                if rec_line.strip():
                    lines.append(f"        {rec_line.strip()}.")
        lines.append("")

    return "\n".join(lines)


def format_json(
    findings_by_job: Dict[int, List[Finding]],
    job_info: Dict[int, Dict],
    app_id: str,
    hudi_version: Optional[str] = None,
) -> str:
    """Structured JSON for tooling."""
    out = {
        "app_id": app_id,
        "hudi_version": hudi_version,
        "summary": {
            "total_findings": sum(len(f) for f in findings_by_job.values()),
            "jobs_with_findings": sum(1 for fs in findings_by_job.values() if fs),
            "by_severity": {},
        },
        "jobs": [],
    }
    for fs in findings_by_job.values():
        for f in fs:
            out["summary"]["by_severity"][f.severity] = \
                out["summary"]["by_severity"].get(f.severity, 0) + 1

    for job_id, fs in sorted(findings_by_job.items()):
        if not fs:
            continue
        info = job_info.get(job_id, {})
        out["jobs"].append({
            "job_id": job_id,
            "duration_ms": info.get("duration_ms"),
            "name": info.get("name"),
            "findings": [
                {
                    "rule_id": f.rule_id,
                    "severity": f.severity,
                    "stage_id": f.stage_id,
                    "rule_kind": f.rule_kind,
                    "linked_issue": f.linked_issue,
                    "known_issue_detail": lookup(f.linked_issue) if f.linked_issue else None,
                    "evidence": f.evidence,
                    "recommendation": f.recommendation,
                }
                for f in sorted(fs, key=_severity_sort_key)
            ],
        })
    return json_mod.dumps(out, indent=2)
