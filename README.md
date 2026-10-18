# DataBay

A Python library for profiling, comparing and loading Spark datasets.
Works with direct Spark sessions and Spark Connect, from scripts or notebooks.

- Check null rates and duplicate rows.
- Compare datasets by selected columns.
- Read and write PostgreSQL, Microsoft SQL and Oracle through JDBC.
- Load CSV files and feed Delta or Iceberg tables.
- Retrieve database credentials from Bitwarden when needed.

## Install

Requires Python 3.10+ and PySpark 4.2.0. Your environment supplies Spark, JDBC
drivers and catalogs. Local Spark also requires Java 17+.

Install from Git, replacing `COMMIT_SHA` with the commit you want to use:

```bash
python -m pip install "databay @ git+https://github.com/Majo0101/DataBay.git@COMMIT_SHA"
```

## Quick start

```python
from databay import null_rate, duplicate_check, compare_datasets
from databay.runtime import spark_direct

spark = spark_direct()
# For Spark Connect, use spark_connect() from databay.runtime instead.

df = spark.createDataFrame(
    [(1, "Ana"), (2, None), (2, "Peter")], "id long, name string"
)

null_rate(df).show()
duplicate_check(df, cols=["id"]).show()
compare_datasets(df, df, cols=["id", "name"]).show()
```

Use your existing Spark session if you already have one. `spark_connect()` uses
`SPARK_REMOTE` or defaults to `localhost:15002`; choose one mode per Python process.

## Read from a database

```python
from databay.etl import Octopus

octopus = Octopus(spark=spark, engine="postgresql", env_file=".env")
frames = octopus.read_jdbc([("SELECT * FROM customers", "customers")])
frames["customers"].show()
```

The `.env` file contains `HOST`, `DATABASE`, `USER`, `PASSWORD` and optional `PORT`.
`read_jdbc()` returns lazy Spark DataFrames. Use `write_jdbc()` to write to a
database or `feed_spark()` to load Delta or Iceberg tables.

## Notebooks

- [DataBay examples](examples/main.ipynb)
- [PostgreSQL with Bitwarden](examples/postgres_bitwarden.ipynb)

Register `sparksql_magic(spark)` from `databay.runtime` to use `%%sparksql` in notebooks.

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest -m "not integration"
```

## License

Use requires prior written permission from Marian Bodnar.
Contact [bodnar.marian@gmail.com](mailto:bodnar.marian@gmail.com). See [LICENSE](LICENSE).
