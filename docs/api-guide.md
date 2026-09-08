# Choosing a DataBay function

Complete parameter and return-value guidance is available in function docstrings
and adjacent `.pyi` files for Pylance. Examples assume existing Spark DataFrames.

## Profiling unfamiliar data

| Question | Function | Important behavior |
| --- | --- | --- |
| Which columns vary? | `select_informative_columns(df, min_distinct_values=2)` | Default 1 keeps constants; blanks are missing by default. |
| Which columns contain NULLs? | `null_rate(df, threshold=5)` | Inclusive percentage; blanks and NaN are not NULL. |
| Are keys repeated? | `duplicate_check(df, cols=["id"], show_summary_only=True)` | Counts all rows in repeated groups, including the first occurrence. |
| Is a candidate key unique and populated? | `pk_uniqueness_check(df, ["id"])` | Returns duplicate and NULL statistics, not a boolean. |
| Do values match a format? | `regex_check(df, {"postcode": "^[0-9]{5}$"})` | NULL fails; anchor patterns for whole-value validation. |
| Do rows satisfy business rules? | `row_level_rules(df, {"positive": "amount > 0"})` | Spark SQL expressions; FALSE and NULL fail. |
| How are fields related? | `cardinality_check(df, "customer_id", "order_id")` | Counts distinct partners; excludes NULL-containing keys. |
| How would two tables join? | `cardinality_check_tables(orders, customers, "customer_id", "id")` | Classifies row multiplicities on shared non-null keys only. |
| Where do known keys appear? | `find_key_set({"orders": orders}, customers, "id", ["customer_id"])` | Compares distinct string representations and reports search-key coverage. |

```python
from databay import select_informative_columns

useful, report = select_informative_columns(
    df, min_distinct_values=2, min_non_null_percentage=10,
    preserve=["id"], return_report=True,
)
report.show(truncate=False)
```

`preserve` overrides all thresholds. Column-name parameters in core functions
refer to literal names, including dots. Inside SQL expressions, quote such names
with backticks, for example: ``` `customer.id` IS NOT NULL ```.

## Comparing datasets

- `compare_schema(source, target)` compares column presence and types.
- `compare_datasets(source, target, ["*"])` compares row values while retaining
  duplicate multiplicity.
- `compare_columns_by_key(source, target, ["id"], ["amount"], join_type="full")`
  compares by key and includes unmatched rows.
- `numeric_diff_check(source, target, ["id"], ["amount"], tolerance=0.01)` compares
  numeric values on shared keys using absolute tolerance. `diff_type="both"`
  adds signed percentage differences to details.

Duplicate keys can multiply joined rows. Numeric comparisons do not classify
NULL operands as equal or different; profile missing values separately.

## Loading and writing

`Octopus(env_file=".env", spark=spark, engine="postgresql")` uses HOST, PORT,
DATABASE, USER and PASSWORD. JDBC hosts must be reachable from the Spark server.

- `read_jdbc([("SELECT * FROM customers", "customers")])` returns lazy Spark
  DataFrames in a dictionary. Partition bounds require `parallel_read=True`;
  they divide reads rather than filter source rows.
- `feed_spark(queries, target_schema="raw")` immediately overwrites Delta tables
  by default. Use `table_format="iceberg"` and, for the bundled Iceberg runtime,
  `target_schema="lake.raw"` to create or replace Iceberg tables (data and schema).
  The selected format and catalog must already be configured on the server.
- `write_jdbc(df, target_table="customers", num_partitions=4)` writes immediately,
  appending by default. The cap applies per table write, not to all DB clients.
- `load_csv(spark, [("/data/apache/customers.csv", "customers")], cfg, delimiter=",")`
  takes container paths under configured bind mounts. Default mode `both`
  registers temporary views and returns a DataFrame dictionary; `view` returns
  None and `dfs` returns DataFrames without registering views.

JDBC `schema` overrides read types through the driver; omitted fields retain
inferred types. It takes precedence over `infer_schema=False`.

## Notebook results and lifecycle

After registering `sparksql_magic(spark)` in IPython/Jupyter:

```sql
%%sparksql pandas customers_pd --limit 5000
SELECT * FROM customers ORDER BY id
```

This displays and assigns a Pandas DataFrame. The default cap is 10000 rows;
Spark retrieves at most cap + 1 before conversion to detect truncation. The extra
row is removed with a warning. A row cap is not a memory cap, and without
`ORDER BY` the selected rows are not guaranteed.

`%%sparksql result_df` assigns a Spark DataFrame; `%%sparksql view result_view`
registers a temporary view; a bare `%%sparksql` displays Spark rows.

`dock(cfg)` reuses containers by name without applying changed settings.
`dock_spark_init(cfg)` starts/reuses a container, replaces an active Spark session
and registers the SQL magic. `dock_shutdown(cfg)` stops the container;
`remove=True` removes it but retains named volumes and bind-mounted files.

For Compose-managed runtimes, use `docker compose up -d` after changing environment
values and `docker compose up -d --build` after changing the image/startup script.

Analytical functions may execute Spark actions while building reports; returned
DataFrames can still contain lazy work. Cache ownership across notebook analyses
stays with the caller: release it only after consuming actions finish.
