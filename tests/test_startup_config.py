"""Exercise both real startup scripts in a disposable Linux container.

Requires Docker and the locally built spark-pg-delta image. Service commands
are stubbed: these tests verify configuration, not catalog/server startup.
"""

from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.integration


@pytest.fixture(params=["delta_jdbc", "iceberg_jdbc"])
def startup(request):
    return (ROOT / "infra" / request.param / "start.sh").read_text(encoding="utf-8")


def generate(startup, **environment):
    command = [
        "docker", "run", "--rm", "-i", "--network", "none",
        "--entrypoint", "bash",
    ]
    for name, value in environment.items():
        command.extend(["-e", f"{name}={value}"])
    command.extend(["spark-pg-delta", "-s"])
    # Run the complete script without starting PostgreSQL or Spark. No host
    # mounts, published ports or existing container/volume names are used.
    script = (
        "pg_ctlcluster() { :; }\npg_isready() { :; }\nsu() { :; }\n"
        + startup
        + "\nprintf '\\nCONFIG_START\\n'\ncat /opt/spark/conf/spark-defaults.conf\n"
    )
    # Binary stdin preserves LF on Windows, as the Dockerfile's sed does.
    result = subprocess.run(
        command, input=script.encode("utf-8"), capture_output=True, timeout=45
    )
    result.stdout = result.stdout.decode("utf-8")
    result.stderr = result.stderr.decode("utf-8")
    return result


def config(result):
    assert result.returncode == 0, result.stdout + result.stderr
    return dict(
        line.split(None, 1)
        for line in result.stdout.split("CONFIG_START\n", 1)[1].splitlines()
        if line.startswith("spark.")
    )


def test_defaults(startup):
    settings = config(generate(startup, SPARK_MEMORY="28", SPARK_CORES="12"))
    assert settings["spark.master"] == "local[12]"
    assert settings["spark.driver.memory"] == "28g"
    assert settings["spark.driver.maxResultSize"] == "4g"
    assert settings["spark.default.parallelism"] == "12"
    assert settings["spark.sql.shuffle.partitions"] == "36"
    assert settings["spark.sql.autoBroadcastJoinThreshold"] == "104857600"
    assert settings["spark.memory.fraction"] == "0.8"
    assert settings["spark.memory.storageFraction"] == "0.3"
    assert settings["spark.sql.adaptive.enabled"] == "true"
    assert not any(key.startswith("spark.executor.") for key in settings)


@pytest.mark.parametrize("threshold", ["-1", "0", "10485760"])
def test_overrides(startup, threshold):
    settings = config(generate(
        startup, SPARK_MEMORY="8", SPARK_CORES="4",
        SPARK_SHUFFLE_PARTITIONS="17", SPARK_BROADCAST_THRESHOLD=threshold,
        SPARK_MEMORY_FRACTION="0.6", SPARK_STORAGE_FRACTION="0.5",
    ))
    assert settings["spark.master"] == "local[4]"
    assert settings["spark.driver.memory"] == "8g"
    assert settings["spark.default.parallelism"] == "4"
    assert settings["spark.sql.shuffle.partitions"] == "17"
    assert settings["spark.sql.autoBroadcastJoinThreshold"] == threshold
    assert settings["spark.memory.fraction"] == "0.6"
    assert settings["spark.memory.storageFraction"] == "0.5"


def test_empty_override_preserves_default(startup):
    settings = config(generate(startup, SPARK_CORES="10", SPARK_SHUFFLE_PARTITIONS=""))
    assert settings["spark.sql.shuffle.partitions"] == "30"


@pytest.mark.parametrize(("name", "value"), [
    ("SPARK_MEMORY", "0"),
    ("SPARK_CORES", "2.5"),
    ("SPARK_SHUFFLE_PARTITIONS", "-1"),
    ("SPARK_BROADCAST_THRESHOLD", "-2"),
    ("SPARK_BROADCAST_THRESHOLD", "10mb"),
    ("SPARK_MEMORY_FRACTION", "1.1"),
    ("SPARK_STORAGE_FRACTION", "-0.1"),
    ("SPARK_SHUFFLE_PARTITIONS", "8\nspark.master local[1]"),
])
def test_invalid_values_stop_before_services(startup, name, value):
    result = generate(startup, **{name: value})
    assert result.returncode != 0
    assert f"Invalid {name}:" in result.stderr
    assert "Starting PostgreSQL..." not in result.stdout
