"""Spark UI REST API client and Hudi commit-metadata reader.

The Spark UI client uses the documented REST API at /api/v1/applications/...
The Hudi reader walks the .hoodie/ directory for commit metadata.

Both are read-only.
"""
import json
import os
import urllib.request
from typing import Any, Dict, List, Optional


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

    def get_stage_attempt(self, stage_id: int, attempt_id: int = 0) -> Dict:
        """Stage with full task list + accumulables."""
        return self._get(f"/stages/{stage_id}/{attempt_id}")

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


def detect_hudi_version_from_env(env: Dict) -> Optional[str]:
    """Look at Spark conf to find a Hudi bundle version, if registered."""
    spark_props = env.get("sparkProperties", [])
    for k, v in spark_props:
        if "hudi" in k.lower() and "bundle" in v.lower():
            return v
        if k == "spark.jars" and "hudi-spark" in v:
            # extract version from path like .../hudi-spark3.4-bundle_2.12-1.1.1.jar
            for token in v.split(","):
                if "hudi-spark" in token and ".jar" in token:
                    base = os.path.basename(token)
                    parts = base.replace(".jar", "").split("-")
                    if len(parts) > 0:
                        return parts[-1]  # version is last token
    return None
