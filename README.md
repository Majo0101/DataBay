# DataBay

Python library for profiling, comparing and loading Apache Spark datasets.
Requires Python 3.10+ and PySpark 4.2.0 (installed with Spark Connect support).
Provide your own Spark session, JDBC drivers and lakehouse catalogs.
Running Spark locally requires Java 17 or later.

## Installation

From the repository root:

```bash
python -m pip install .
```

For development: `python -m pip install -e ".[dev]"`.

To install during an infrastructure build, pin a Git commit:

```bash
python -m pip install "databay @ git+https://github.com/Majo0101/DataBay.git@COMMIT_SHA"
```

The infrastructure supplies Spark, JDBC drivers, catalogs and optional Bitwarden
CLI access. DataBay does not build or manage containers.

## Usage

Use your existing Spark session, or choose a runtime:

```python
from databay.runtime import spark_direct, spark_connect
from databay import null_rate, duplicate_check, compare_datasets

# Direct Spark in the container (JVM / SparkContext).
spark = spark_direct()

# Alternatively, in a separate Python process/kernel: Spark Connect.
# spark = spark_connect()
df = spark.createDataFrame(
    [(1, "Ana"), (2, None), (2, "Peter")], "id long, name string"
)
null_rate(df).show()
duplicate_check(df, cols=["id"]).show()
compare_datasets(df, df, cols=["id", "name"]).show()
```

- `databay.core`: data quality, key and relationship checks, schema and value comparisons.
- `databay.etl.Octopus`: JDBC reads/writes, CSV loading, Delta/Iceberg table loading.
- `databay.runtime`: direct/Connect session helpers and `sparksql_magic(spark)` for notebooks.

`spark_direct()` without `master` inherits the infrastructure's Spark configuration.
JDBC drivers, catalogs, resources and mounts are configured by the infrastructure.
Direct mode explicitly uses the classic API; Connect uses an existing server and accepts a complete
`sc://` URI (TLS/authentication parameters included). With no explicit URI,
`spark_connect()` uses `SPARK_REMOTE`, or falls back to `localhost:15002`.

Choose the mode before starting the Python process/kernel. A Connect-configured
process (`SPARK_REMOTE` / `SPARK_CONNECT_MODE_ENABLED`) cannot use `spark_direct()`.
Helpers may reuse an existing session in the same mode. The caller owns its
lifecycle; changing startup settings requires a fresh Spark context/process.
Connect preserves the client reattachment setting unless explicitly overridden.
Register `sparksql_magic(spark)` explicitly in a notebook; it works in both modes.
Use `%%sparksql pandas result --limit=10000` to display and assign a bounded Pandas result.

Pass your session to `Octopus(spark=spark, engine="postgresql", env_file=".env")`.
The credentials file uses `HOST`, `PORT`, `DATABASE`, `USER`, `PASSWORD`.
Omit `env_file` to use environment variables. An explicit file must exist and
contain nonempty literal values. References such as `${DB_PASSWORD}` are rejected.
Missing keys do not fall back to the process environment.
CSV paths and database hosts must be accessible from Spark.

```python
from databay.etl import Octopus

frames = Octopus(spark=spark).load_csv(
    sources=[("/path/visible/to/spark/customers.csv", "customers")],
    delimiter=",",
)
```

## Optional Bitwarden password

The existing `.env` workflow is unchanged. To obtain only the password from the
infrastructure's Bitwarden CLI, omit PASSWORD from the file and pass a provider:

```python
from databay.etl import Octopus, bitwarden_password

octopus = Octopus(
    spark=spark,
    engine="postgresql",
    env_file=".env",
    password_provider=lambda: bitwarden_password("YOUR_ITEM_ID"),
)
frames = octopus.read_jdbc([("SELECT * FROM customers", "customers")])
```

Infra must install `bw` and provide an unlocked vault session (BW_SESSION).
DataBay does not log in or unlock it. The provider is called once per JDBC method
invocation and explicitly overrides PASSWORD; errors never fall back to `.env`.
Passwords are not cached in the Octopus instance or written back to the file.
Lazy DataFrames retain the password snapshot from their originating operation.
The same provider works with direct Spark and Spark Connect. Custom zero-argument
password providers are supported too.

## Migrating to 0.4.0

Docker management APIs and the `load_csv(dock_cfg=...)` argument were removed.
`load_csv()` uses the instance session; pass only sources and CSV options.
SQL magic uses `--limit=10000`; the previous space-separated syntax is rejected.
Parameter details are in function docstrings and adjacent `.pyi` files.
See [the example notebook](examples/main.ipynb).

## Tests

```bash
python -m pytest -m "not integration"
```

Integration tests use your external services and skip when unconfigured:

- `DATABAY_TEST_SPARK_MODE`: `connect` (default) or `direct`.
- `DATABAY_TEST_SPARK_MASTER`: direct Spark master (default `local[2]`).
- `DATABAY_TEST_SPARK_REMOTE`: Spark Connect URI; `local[2]` explicitly starts a
  disposable local Connect server for tests.
- `DATABAY_TEST_JDBC_ENV`: PostgreSQL credentials file (default `.env.test`).
- `DATABAY_TEST_CSV_PATH`: existing CSV path accessible to Spark.
- `DATABAY_TEST_TABLE_FORMATS`: configured formats, e.g. `delta,iceberg`.
- `DATABAY_TEST_ICEBERG_CATALOG`: Iceberg catalog (default `lake`).

Run with `python -m pytest -m integration`. Use a disposable test database:
these tests create and overwrite tables; slow tests write large datasets.

## License

Prior written permission from Marian Bodnar is required to use DataBay.
Contact [bodnar.marian@gmail.com](mailto:bodnar.marian@gmail.com).
See [LICENSE](LICENSE).
