"""Spark UI REST API client and Hudi commit-metadata reader.

The Spark UI client uses the documented REST API at /api/v1/applications/...
The Hudi reader walks the .hoodie/ directory for commit metadata.

Both are read-only.
"""
import json
import os
import re
import urllib.request
from typing import Any, Dict, Iterable, List, Optional


# ────────────────────── Spark UI REST API ────────────────────────────────


class SparkUIClient:
    """Minimal REST client for the Spark UI's /api/v1 endpoints."""

    def __init__(self, base_url: str, app_id: str, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.app_id = app_id
        self.timeout = timeout

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}/api/v1/applications/{self.app_id}{path}"
        with urllib.request.urlopen(url, timeout=self.timeout) as r:
            return json.loads(r.read().decode("utf-8"))

    def get_app_info(self) -> Dict:
        return self._get("")

    def get_environment(self) -> Dict:
        """Returns Spark + Hadoop conf. We use it to detect Hudi configs."""
        return self._get("/environment")

    def get_jobs(self) -> List[Dict]:
        return self._get("/jobs")

    def get_job(self, job_id: int) -> Dict:
        return self._get(f"/jobs/{job_id}")

    def get_stages(self) -> List[Dict]:
        """All stages with summary metrics."""
        return self._get("/stages")

    def get_stage_attempt(
        self,
        stage_id: int,
        attempt_id: int = 0,
        with_summaries: bool = False,
        quantiles: Optional[List[float]] = None,
    ) -> Dict:
        """Stage with full task list + accumulables.

        Spark's per-stage endpoint only populates `taskMetricsDistributions`
        when `withSummaries=true` is set, and only the per-stage endpoint —
        NOT the `/stages` list endpoint — supports it. Pass with_summaries=True
        when you need quantile distributions.

        With Spark 3.4+, you can request specific quantiles via the `quantiles`
        parameter (e.g. [0.0, 0.5, 0.95, 0.99, 1.0]); the default
        [0.0, 0.25, 0.5, 0.75, 1.0] is returned otherwise.
        """
        params = []
        if with_summaries:
            params.append("withSummaries=true")
        if quantiles is not None:
            params.append("quantiles=" + ",".join(str(q) for q in quantiles))
        suffix = ("?" + "&".join(params)) if params else ""
        return self._get(f"/stages/{stage_id}/{attempt_id}{suffix}")

    def get_stage_distributions(
        self,
        stage_id: int,
        attempt_id: int = 0,
        quantiles: Optional[List[float]] = None,
    ) -> Optional[Dict]:
        """Return just the `taskMetricsDistributions` dict for a stage, or
        None if the stage has none (e.g. it had zero tasks).

        Use this when you only need quantile distributions and not the full
        task list — saves bandwidth vs. get_stage_attempt(with_summaries=True).
        """
        if quantiles is None:
            quantiles = [0.0, 0.5, 0.95, 0.99, 1.0]
        try:
            stage = self.get_stage_attempt(
                stage_id, attempt_id, with_summaries=True, quantiles=quantiles
            )
        except Exception:
            return None
        return stage.get("taskMetricsDistributions")

    def get_executors(self) -> List[Dict]:
        return self._get("/executors")

    def get_sql(self) -> List[Dict]:
        """SQL execution list with plan + per-node metrics."""
        try:
            return self._get("/sql")
        except Exception:
            return []  # SQL endpoint may not be available


# ────────────────────── Hudi commit metadata reader ───────────────────────


class HudiMetaReader:
    """Reads commit metadata from a Hudi table's .hoodie/ directory.

    Provides per-commit:
      - operation type (upsert / insert / delete_partition / ...)
      - approximate write metrics (file count, bytes written)
      - schema digest (for detecting schema-evolution boundaries)
    """

    def __init__(self, table_path: str):
        self.table_path = table_path
        self.hoodie_dir = os.path.join(table_path, ".hoodie")
        if not os.path.isdir(self.hoodie_dir):
            raise FileNotFoundError(f"Not a Hudi table: {table_path}")

    def list_commits(self) -> List[Dict]:
        """Return a list of (instant_time, action_type, status, file_path) tuples
        sorted by instant_time descending (newest first)."""
        commits = []
        for fname in os.listdir(self.hoodie_dir):
            full_path = os.path.join(self.hoodie_dir, fname)
            if not os.path.isfile(full_path):
                continue
            for action in [
                "commit", "deltacommit", "replacecommit", "compaction.requested",
                "clean", "rollback", "savepoint", "restore",
            ]:
                if fname.endswith(f".{action}"):
                    instant_time = fname[: -(len(action) + 1)]
                    commits.append({
                        "instant_time": instant_time,
                        "action": action,
                        "file_path": full_path,
                    })
                    break
        commits.sort(key=lambda c: c["instant_time"], reverse=True)
        return commits

    def read_commit(self, instant_time: str, action: str = "commit") -> Optional[Dict]:
        """Read a specific commit's metadata JSON. Returns None if not found."""
        path = os.path.join(self.hoodie_dir, f"{instant_time}.{action}")
        if not os.path.exists(path):
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None

    def get_table_config(self) -> Dict:
        """Read hoodie.properties from .hoodie/."""
        props_path = os.path.join(self.hoodie_dir, "hoodie.properties")
        if not os.path.exists(props_path):
            return {}
        config = {}
        with open(props_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    config[k.strip()] = v.strip()
        return config


# ────────────────────── Convenience ───────────────────────────────────────


# Matches Hudi-bundle-shaped jar filenames and captures the version.
# Examples:
#   hudi-spark3.4-bundle_2.12-1.1.1.jar       -> "1.1.1"
#   hudi-utilities-bundle_2.12-0.14.1.jar     -> "0.14.1"
#   hudi-spark3.5_2.13-0.15.0-SNAPSHOT.jar    -> "0.15.0-SNAPSHOT"
#   hudi-utilities-slim-bundle_2.12-1.0.0.jar -> "1.0.0"
#
# Non-greedy middle section: any combination of dashes, underscores, digits,
# letters, and dots — but the version-suffix-then-.jar at the end is what
# anchors the match.
_HUDI_JAR_VERSION_RE = re.compile(
    r"hudi[\w.-]*?-([0-9]+\.[0-9]+\.[0-9]+(?:[-_][A-Za-z0-9._]+)?)\.jar",
    re.IGNORECASE,
)


def detect_hudi_version_from_env(env: Dict) -> Optional[str]:
    """Find the Hudi bundle jar in the Spark environment and return its version.

    Checks (in order) every source Spark surfaces classpath info under:
      1. sparkProperties — `spark.jars`, plus any key/value that mentions Hudi
      2. classpathEntries — every classpath entry (typically the most reliable)
      3. systemProperties — `java.class.path` and `sun.java.command`

    Returns the version string of the first Hudi-bundle-style jar found,
    capturing both the numeric `X.Y.Z` core and an optional qualifier
    (`-SNAPSHOT`, `-rc1`, etc.), or None if no Hudi jar matched.

    The old implementation only consulted sparkProperties and assumed the
    jar's version was the last `-`-separated token after stripping `.jar` —
    that mis-parsed common shapes like `hudi-spark3.4-bundle_2.12-1.1.1.jar`
    (where the last token would be `1.1.1` but the regex anchor makes the
    intent explicit and survives qualifier suffixes).
    """
    for path in _classpath_candidates(env):
        m = _HUDI_JAR_VERSION_RE.search(path)
        if m:
            return m.group(1)
    return None


def _classpath_candidates(env: Dict) -> Iterable[str]:
    """Yield classpath-like strings from every source Spark exposes.

    Generator so that callers can short-circuit on the first match without
    materializing the entire list.
    """
    # 1. sparkProperties — comma-separated `spark.jars` list, or any value
    #    that already mentions Hudi (e.g. a custom `hudi.bundle.version` key)
    for k, v in env.get("sparkProperties", []) or []:
        if not v:
            continue
        if k == "spark.jars" or "hudi" in (k or "").lower() or "hudi" in v.lower():
            for token in v.split(","):
                if token:
                    yield token
    # 2. classpathEntries — list of [path, source] tuples. This is usually
    #    the place that has the individual jars enumerated, even when
    #    spark.jars / java.class.path use wildcards like `/opt/spark/jars/*`.
    for entry in env.get("classpathEntries", []) or []:
        if isinstance(entry, (list, tuple)) and entry:
            path = entry[0]
            if isinstance(path, str):
                yield path
        elif isinstance(entry, str):
            yield entry
    # 3. systemProperties — `java.class.path` is the actual JVM classpath
    #    (often a wildcard, in which case this yields nothing useful);
    #    `sun.java.command` carries the original `java -cp ... MainClass`
    #    command line, which on Spark can contain bundle paths.
    for k, v in env.get("systemProperties", []) or []:
        if not v:
            continue
        if k not in ("java.class.path", "sun.java.command"):
            continue
        # tolerate both Unix ':' and comma separators
        for sep in (":", ","):
            for part in v.split(sep):
                if part:
                    yield part
