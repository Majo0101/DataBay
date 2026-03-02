from pathlib import Path

import pytest

from databay.etl import Octopus
from databay.runtime.docker import DockConfig, dock
from databay.runtime.spark import spark_connect


class _FakeDataFrame:
    def __init__(self):
        self.views = []

    def createOrReplaceTempView(self, name):
        self.views.append(name)


class _FakeReader:
    def __init__(self, df):
        self.df = df
        self.options = {}
        self.last_csv_path = None

    def option(self, key, value):
        self.options[key] = value
        return self

    def csv(self, path):
        self.last_csv_path = path
        return self.df


class _FakeSpark:
    def __init__(self, reader):
        self.read = reader


@pytest.fixture(scope="module")
def spark_session():
    cfg = DockConfig(
        image="spark-delta-pg",
        name="spark-delta-pg",
        bind_mounts={"C/landing": "/data/apache"},
    )

    try:
        dock(cfg)
        spark = spark_connect(
            app_name="pytest-databay-octopus",
            host="localhost",
            port=15002,
            timeout=20,
            check_interval=0.5,
        )
    except Exception as exc:
        pytest.skip(f"Runtime Spark startup unavailable: {exc}")
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def landing_csv_path() -> str:
    landing_dir = Path("C:/landing")
    csv_files = sorted(landing_dir.glob("*.csv"))
    if not csv_files:
        pytest.skip("No CSV file found in C:/landing for load_csv test")
    return str(csv_files[0]).replace("\\", "/")


def test_octopus_load_csv_reads_mounted_csv(spark_session, landing_csv_path):
    octopus = Octopus(spark=spark_session)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    dfs = octopus.load_csv(
        spark=spark_session,
        sources=[("/data/apache", "landing_csv")],
        dock_cfg=dock_cfg,
        delimiter=",",
        header=True,
        infer_schema=True,
        mode="both",
    )

    assert dfs is not None
    assert "landing_csv" in dfs
    df = dfs["landing_csv"]
    assert df is not None
    assert len(df.columns) > 0
    assert df.count() > 0


def test_octopus_load_csv_raises_for_unmounted_path(spark_session):
    octopus = Octopus(spark=spark_session)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    with pytest.raises(ValueError, match="does not start with any mounted path"):
        octopus.load_csv(
            spark=spark_session,
            sources=[("/data/missing", "missing_csv")],
            dock_cfg=dock_cfg,
            mode="both",
        )


def test_octopus_load_csv_raises_for_invalid_container_path(spark_session):
    octopus = Octopus(spark=spark_session)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    with pytest.raises(ValueError, match="Container path must start with"):
        octopus.load_csv(
            spark=spark_session,
            sources=[("C:/landing/file.csv", "invalid_csv")],
            dock_cfg=dock_cfg,
            mode="both",
        )


@pytest.mark.parametrize("invalid_mode", ["", "invalid", "view_only"])
def test_octopus_load_csv_raises_for_invalid_mode(spark_session, invalid_mode):
    octopus = Octopus(spark=spark_session)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    with pytest.raises(ValueError, match="mode must be one of"):
        octopus.load_csv(
            spark=spark_session,
            sources=[("/data/apache", "landing_csv")],
            dock_cfg=dock_cfg,
            mode=invalid_mode,
        )


@pytest.mark.parametrize("invalid_csv_read_mode", ["", "strict", "permiss"])
def test_octopus_load_csv_raises_for_invalid_csv_read_mode(spark_session, invalid_csv_read_mode):
    octopus = Octopus(spark=spark_session)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    with pytest.raises(ValueError, match="csv_read_mode must be one of"):
        octopus.load_csv(
            spark=spark_session,
            sources=[("/data/apache", "landing_csv")],
            dock_cfg=dock_cfg,
            csv_read_mode=invalid_csv_read_mode,
        )


def test_octopus_load_csv_view_mode_creates_temp_view_and_returns_none():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    result = octopus.load_csv(
        spark=fake_spark,
        sources=[("/data/apache", "landing_csv")],
        dock_cfg=dock_cfg,
        mode="view",
    )

    assert result is None
    assert fake_df.views == ["landing_csv"]
    assert fake_reader.last_csv_path == "/data/apache"


def test_octopus_load_csv_dfs_mode_returns_dict_without_creating_view():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    result = octopus.load_csv(
        spark=fake_spark,
        sources=[("/data/apache", "landing_csv")],
        dock_cfg=dock_cfg,
        mode="dfs",
    )

    assert result is not None
    assert "landing_csv" in result
    assert result["landing_csv"] is fake_df
    assert fake_df.views == []
    assert fake_reader.last_csv_path == "/data/apache"
    assert fake_reader.options["mode"] == "PERMISSIVE"


def test_octopus_load_csv_sets_custom_csv_read_mode():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    result = octopus.load_csv(
        spark=fake_spark,
        sources=[("/data/apache", "landing_csv")],
        dock_cfg=dock_cfg,
        mode="dfs",
        csv_read_mode="failfast",
    )

    assert result is not None
    assert fake_reader.options["mode"] == "FAILFAST"


def test_octopus_load_csv_uses_self_spark_when_spark_argument_is_none():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    result = octopus.load_csv(
        spark=None,
        sources=[("/data/apache", "landing_csv")],
        dock_cfg=dock_cfg,
        mode="dfs",
    )

    assert result is not None
    assert "landing_csv" in result
    assert result["landing_csv"] is fake_df


def test_octopus_load_csv_raises_when_no_spark_is_available():
    octopus = Octopus(spark=None)
    dock_cfg = DockConfig(bind_mounts={"C/landing": "/data/apache"})

    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        octopus.load_csv(
            spark=None,
            sources=[("/data/apache", "landing_csv")],
            dock_cfg=dock_cfg,
            mode="both",
        )
