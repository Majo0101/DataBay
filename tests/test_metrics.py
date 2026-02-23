import pytest
from pyspark.sql import Row, SparkSession
import databay

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest-databay").getOrCreate()

# Test 1: Identical datasets - 100% match
def test_identical_datasets(spark):
    data = [Row(id=1, name="Alice", age=25, city="New York"),
            Row(id=2, name="Bob", age=30, city="London"),
            Row(id=3, name="Charlie", age=35, city="Paris")]
    df_a = spark.createDataFrame(data)
    df_b = spark.createDataFrame(data)
    result = databay.compare_datasets(df_a, df_b, cols=["*"], name_a="A", name_b="B")
    result_dict = {row["metric"] + "_" + row["scope"]: row for row in result.collect()}
    assert result_dict["row_count_A"]["count_value"] == 3
    assert result_dict["row_count_B"]["count_value"] == 3
    assert result_dict["diff_rows_A"]["count_value"] == 0
    assert result_dict["diff_rows_B"]["count_value"] == 0
    assert result_dict["common_rows_both"]["count_value"] == 3
    assert result_dict["jaccard_pct_both"]["percent_value"] == 100.0

# Test 2: Partial matches
def test_partial_matches(spark):
    data_a = [Row(id=1, name="Alice", age=25, city="New York"),
              Row(id=2, name="Bob", age=30, city="London"),
              Row(id=3, name="Charlie", age=35, city="Paris"),
              Row(id=4, name="David", age=28, city="Berlin"),
              Row(id=5, name="Eve", age=32, city="Madrid")]
    data_b = [Row(id=1, name="Alice", age=25, city="New York"),
              Row(id=2, name="Bob", age=31, city="London"),
              Row(id=3, name="Charlie", age=35, city="Paris"),
              Row(id=4, name="David", age=28, city="Munich"),
              Row(id=6, name="Frank", age=29, city="Rome")]
    df_a = spark.createDataFrame(data_a)
    df_b = spark.createDataFrame(data_b)
    result = databay.compare_datasets(df_a, df_b, cols=["*"])
    result_dict = {row["metric"] + "_" + row["scope"]: row for row in result.collect()}
    assert result_dict["row_count_Dataset A"]["count_value"] == 5
    assert result_dict["row_count_Dataset B"]["count_value"] == 5
    assert result_dict["common_rows_both"]["count_value"] == 2
    assert result_dict["union_rows_both"]["count_value"] == 8
    jaccard = result_dict["jaccard_pct_both"]["percent_value"]
    assert 24 <= jaccard <= 26

# Test 3: Completely different datasets
def test_no_matches(spark):
    data_a = [Row(id=1, name="Alice", age=25), Row(id=2, name="Bob", age=30), Row(id=3, name="Charlie", age=35)]
    data_b = [Row(id=10, name="Xavier", age=40), Row(id=11, name="Yara", age=45), Row(id=12, name="Zoe", age=50)]
    df_a = spark.createDataFrame(data_a)
    df_b = spark.createDataFrame(data_b)
    result = databay.compare_datasets(df_a, df_b, cols=["*"])
    result_dict = {row["metric"] + "_" + row["scope"]: row for row in result.collect()}
    assert result_dict["common_rows_both"]["count_value"] == 0
    assert result_dict["union_rows_both"]["count_value"] == 6
    assert result_dict["jaccard_pct_both"]["percent_value"] == 0.0

# More tests can be added for edge cases, empty datasets, duplicates, NULLs, etc.
