"""Unit tests for client.py helpers.

Focus: detect_hudi_version_from_env, which the existing implementation
returned None on every live environment we tried because it (a) only
consulted sparkProperties (b) made shaky assumptions about the jar-name
parse. The rewrite checks multiple sources via a versioned-jar regex.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import detect_hudi_version_from_env  # noqa: E402


# ──────────────────────── helpers ─────────────────────────────────────────


def env_with_spark_jars(jars_csv):
    return {"sparkProperties": [["spark.jars", jars_csv]]}


def env_with_classpath(*paths):
    """Build an env whose classpathEntries are [path, "System Classpath"] pairs.
    Matches the Spark UI REST API's serialization of Seq[(String,String)]."""
    return {"classpathEntries": [[p, "System Classpath"] for p in paths]}


def env_with_system_property(key, value):
    return {"systemProperties": [[key, value]]}


# ──────────────────────── tests ───────────────────────────────────────────


class TestDetectHudiVersionFromSparkJars(unittest.TestCase):
    """Source 1: sparkProperties[k=spark.jars]."""

    def test_simple_bundle_jar(self):
        env = env_with_spark_jars(
            "/opt/spark/jars/hudi-spark3.4-bundle_2.12-1.1.1.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "1.1.1")

    def test_amongst_other_jars(self):
        env = env_with_spark_jars(
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.0.0.jar,"
            "/opt/spark/jars/onehouse-internal.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "1.0.0")

    def test_snapshot_qualifier(self):
        env = env_with_spark_jars(
            "/opt/spark/jars/hudi-spark3.5-bundle_2.12-0.15.0-SNAPSHOT.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "0.15.0-SNAPSHOT")

    def test_rc_qualifier(self):
        env = env_with_spark_jars(
            "/opt/spark/jars/hudi-utilities-bundle_2.12-1.0.0-rc1.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "1.0.0-rc1")

    def test_utilities_slim_bundle(self):
        env = env_with_spark_jars(
            "/opt/spark/jars/hudi-utilities-slim-bundle_2.12-0.14.1.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "0.14.1")

    def test_no_hudi_jar(self):
        env = env_with_spark_jars("/opt/spark/jars/hadoop-aws-3.3.4.jar")
        self.assertIsNone(detect_hudi_version_from_env(env))


class TestDetectHudiVersionFromClasspathEntries(unittest.TestCase):
    """Source 2: classpathEntries — typically more reliable than spark.jars,
    which is often empty / wildcarded."""

    def test_finds_hudi_in_classpath(self):
        env = env_with_classpath(
            "/opt/spark/jars/spark-core_2.12-3.5.0.jar",
            "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        )
        self.assertEqual(detect_hudi_version_from_env(env), "1.1.1")

    def test_finds_when_spark_jars_is_empty(self):
        # Common reality: spark.jars unset, but classpathEntries lists every jar.
        env = {
            "sparkProperties": [["spark.jars", ""]],
            "classpathEntries": [
                ["/opt/spark/jars/hudi-spark3.4-bundle_2.12-0.15.0.jar", "System Classpath"],
            ],
        }
        self.assertEqual(detect_hudi_version_from_env(env), "0.15.0")

    def test_string_entry_form(self):
        # Some serializations make classpathEntries a list of strings (not pairs)
        env = {"classpathEntries": [
            "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.0.0.jar",
        ]}
        self.assertEqual(detect_hudi_version_from_env(env), "1.0.0")

    def test_no_hudi_among_many_jars(self):
        env = env_with_classpath(
            "/opt/spark/jars/spark-core_2.12-3.5.0.jar",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars/postgresql-42.5.0.jar",
        )
        self.assertIsNone(detect_hudi_version_from_env(env))


class TestDetectHudiVersionFromSystemProperties(unittest.TestCase):
    """Source 3: systemProperties — `java.class.path` or `sun.java.command`."""

    def test_finds_in_java_class_path_colon_separated(self):
        env = env_with_system_property(
            "java.class.path",
            "/opt/spark/conf:/opt/spark/jars/spark-core.jar:"
            "/opt/spark/jars/hudi-spark3.4-bundle_2.12-1.1.1.jar:/opt/spark/jars/x.jar",
        )
        self.assertEqual(detect_hudi_version_from_env(env), "1.1.1")

    def test_wildcard_classpath_yields_none(self):
        # Common deployment pattern — java.class.path is just a wildcard, so
        # we can't see individual jars there. Detection should fall through
        # to the other sources (here we provide none) and return None.
        env = env_with_system_property("java.class.path", "/opt/spark/conf:/opt/spark/jars/*")
        self.assertIsNone(detect_hudi_version_from_env(env))

    def test_finds_in_sun_java_command(self):
        env = env_with_system_property(
            "sun.java.command",
            "org.apache.spark.deploy.SparkSubmit --jars "
            "/opt/spark/jars/hudi-spark3.5-bundle_2.12-0.14.0.jar local:///app.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "0.14.0")

    def test_ignores_unrelated_system_props(self):
        env = {"systemProperties": [
            ["os.name", "Linux"],
            ["user.dir", "/home/spark"],
        ]}
        self.assertIsNone(detect_hudi_version_from_env(env))


class TestDetectHudiVersionMultipleSources(unittest.TestCase):
    """Combined-source tests + priority + first-match-wins behavior."""

    def test_first_match_wins_when_multiple_hudi_jars_present(self):
        # Two different versions on the classpath — implementation returns
        # the first one encountered in source-priority order.
        env = {
            "classpathEntries": [
                ["/opt/spark/jars/hudi-utilities-bundle_2.12-0.14.1.jar", "System Classpath"],
                ["/opt/spark/jars/hudi-spark3.4-bundle_2.12-1.1.1.jar", "System Classpath"],
            ],
        }
        # Either one is acceptable; we just want a NON-None answer that
        # matches one of the present versions.
        result = detect_hudi_version_from_env(env)
        self.assertIn(result, ("0.14.1", "1.1.1"))

    def test_falls_through_sources(self):
        # spark.jars is empty, classpathEntries has no hudi, but
        # systemProperties.java.class.path does.
        env = {
            "sparkProperties": [["spark.jars", ""]],
            "classpathEntries": [["/opt/spark/jars/hadoop-aws-3.3.4.jar", "System Classpath"]],
            "systemProperties": [[
                "java.class.path",
                "/opt/spark/jars/hudi-spark3.4-bundle_2.12-1.1.1.jar",
            ]],
        }
        self.assertEqual(detect_hudi_version_from_env(env), "1.1.1")

    def test_empty_env(self):
        self.assertIsNone(detect_hudi_version_from_env({}))

    def test_env_without_any_classpath_info(self):
        env = {"sparkProperties": [["spark.app.name", "my-app"]]}
        self.assertIsNone(detect_hudi_version_from_env(env))


class TestDetectHudiVersionRegression(unittest.TestCase):
    """Cases that the previous implementation specifically got wrong."""

    def test_previous_impl_returned_none_on_classpath_only(self):
        # The old code never consulted classpathEntries, so a real-world env
        # with spark.jars unset and the Hudi jar only on classpathEntries
        # returned None. The fix must return the version.
        env = {
            "sparkProperties": [],  # no spark.jars at all
            "classpathEntries": [
                ["/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar", "System Classpath"],
            ],
        }
        self.assertEqual(detect_hudi_version_from_env(env), "1.1.1")

    def test_previous_impl_dropped_snapshot_qualifier(self):
        # Old code's `parts[-1]` heuristic returned "1.1.1" even when the
        # filename was `...-1.1.1-SNAPSHOT.jar` if hyphen-splitting tripped.
        # New regex captures the qualifier explicitly.
        env = env_with_spark_jars(
            "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1-SNAPSHOT.jar"
        )
        self.assertEqual(detect_hudi_version_from_env(env), "1.1.1-SNAPSHOT")


if __name__ == "__main__":
    unittest.main()
