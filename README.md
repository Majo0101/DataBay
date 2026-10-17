# DataBay

Profile, compare and load Spark datasets from Python or notebooks.
Your infrastructure provides Spark, JDBC drivers, catalogs and mounted data.

## Install

Install from Git, pinned to a commit:

```bash
python -m pip install "databay @ git+https://github.com/Majo0101/DataBay.git@COMMIT_SHA"
```

Requires Python 3.10+ and PySpark 4.2.0. Local Spark requires Java 17+.

## Use Spark

Use your existing session or choose one mode per Python process:

```python
from databay.runtime import spark_direct, spark_connect
from databay import null_rate, duplicate_check, compare_datasets

spark = spark_direct()  # Inherit the infrastructure's Spark configuration.
# Or: spark = spark_connect()  # SPARK_REMOTE, otherwise localhost:15002.

df = spark.createDataFrame(
    [(1, "Ana"), (2, None), (2, "Peter")], "id long, name string"
)
null_rate(df).show()
duplicate_check(df, cols=["id"]).show()
compare_datasets(df, df, cols=["id", "name"]).show()
```

For notebooks, register `sparksql_magic(spark)` from `databay.runtime`.
Then use `%%sparksql pandas result --limit=10000` to display and assign Pandas results.

## Load data

```python
from databay.etl import Octopus

octopus = Octopus(spark=spark, engine="postgresql", env_file=".env")
frames = octopus.read_jdbc([("SELECT * FROM customers", "customers")])
frames["customers"].show()

csv_frames = octopus.load_csv([("/data/customers.csv", "customers")], delimiter=",")
```

JDBC supports PostgreSQL, MSSQL and Oracle. `write_jdbc()` writes to databases;
`feed_spark()` loads Delta or Iceberg tables configured in your Spark environment.
Paths and database hosts must be reachable from Spark.

The `.env` file uses `HOST`, `PORT`, `DATABASE`, `USER` and `PASSWORD`.
Declared values must be filled in directly; `${...}` references are rejected.
Omit `env_file` to use process environment variables instead.

## Optional Bitwarden

Keep connection settings in `.env` and obtain the password from an unlocked vault:

```python
from databay.etl import Octopus, bitwarden_password

octopus = Octopus(
    spark=spark,
    engine="postgresql",
    env_file=".env",
    password_provider=lambda: bitwarden_password("ITEM_ID"),
)
```

Infrastructure provides `bw` and `BW_SESSION`. The provider overrides `PASSWORD`,
so that key can be omitted from `.env`. It runs once per JDBC operation;
failures raise an error without falling back to another password.

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest -m "not integration"
```

See the [example notebook](examples/main.ipynb) and function docstrings for more.

## License

Use requires prior written permission from Marian Bodnar.
Contact [bodnar.marian@gmail.com](mailto:bodnar.marian@gmail.com). See [LICENSE](LICENSE).
