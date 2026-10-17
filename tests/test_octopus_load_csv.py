import os

import pytest

from databay.etl import Octopus

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
def landing_csv_path() -> str:
    path = os.environ.get("DATABAY_TEST_CSV_PATH")
    if not path:
        pytest.skip("Set DATABAY_TEST_CSV_PATH to a CSV accessible to the Spark server")
    return path


@pytest.mark.integration
def test_octopus_load_csv_reads_csv(spark_session, landing_csv_path):
    octopus = Octopus(spark=spark_session)

    dfs = octopus.load_csv(
        sources=[(landing_csv_path, "landing_csv")],
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


@pytest.mark.parametrize("invalid_mode", ["", "invalid", "view_only"])
def test_octopus_load_csv_raises_for_invalid_mode(invalid_mode):
    octopus = Octopus(spark=object())

    with pytest.raises(ValueError, match="mode must be one of"):
        octopus.load_csv(
            sources=[("/data/apache", "landing_csv")],
            mode=invalid_mode,
        )


@pytest.mark.parametrize("invalid_csv_read_mode", ["", "strict", "permiss"])
def test_octopus_load_csv_raises_for_invalid_csv_read_mode(invalid_csv_read_mode):
    octopus = Octopus(spark=object())

    with pytest.raises(ValueError, match="csv_read_mode must be one of"):
        octopus.load_csv(
            sources=[("/data/apache", "landing_csv")],
            csv_read_mode=invalid_csv_read_mode,
        )


def test_octopus_load_csv_view_mode_creates_temp_view_and_returns_none():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)

    result = octopus.load_csv(
        sources=[("/data/apache", "landing_csv")],
        mode="view",
    )

    assert result is None
    assert fake_df.views == ["landing_csv"]
    assert fake_reader.last_csv_path == "/data/apache"


@pytest.mark.parametrize("path", [
    "relative/customers.csv", "/data/customers.csv", "s3a://bucket/customers.csv",
    "abfss://container@account.dfs.core.windows.net/customers.csv",
])
def test_octopus_load_csv_dfs_mode_returns_dict_without_creating_view(path):
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)

    result = octopus.load_csv(
        sources=[(path, "landing_csv")],
        mode="dfs",
    )

    assert result is not None
    assert "landing_csv" in result
    assert result["landing_csv"] is fake_df
    assert fake_df.views == []
    assert fake_reader.last_csv_path == path
    assert fake_reader.options["mode"] == "PERMISSIVE"


def test_octopus_load_csv_sets_custom_csv_read_mode():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)

    result = octopus.load_csv(
        sources=[("/data/apache", "landing_csv")],
        mode="dfs",
        csv_read_mode="failfast",
    )

    assert result is not None
    assert fake_reader.options["mode"] == "FAILFAST"


def test_octopus_load_csv_accepts_positional_sources_with_instance_session():
    fake_df = _FakeDataFrame()
    fake_reader = _FakeReader(fake_df)
    fake_spark = _FakeSpark(fake_reader)

    octopus = Octopus(spark=fake_spark)

    result = octopus.load_csv(
        [("/data/apache", "landing_csv")],
        mode="dfs",
    )

    assert result is not None
    assert "landing_csv" in result
    assert result["landing_csv"] is fake_df


def test_octopus_load_csv_raises_when_no_spark_is_available():
    octopus = Octopus(spark=None)

    with pytest.raises(RuntimeError, match="SparkSession is not set"):
        octopus.load_csv(
            sources=[("/data/apache", "landing_csv")],
            mode="both",
        )
