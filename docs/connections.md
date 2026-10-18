# Connections and credentials

[Back to DataBay](../README.md)

Use an existing Spark session or create one with `spark_direct()` or
`spark_connect()` from `databay.runtime`. The examples below assume `spark`
is already available. JDBC drivers and database connectivity are provided by
your Spark environment.

## Database connections

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
Connections use database usernames and passwords; Microsoft interactive and
token authentication are not supported.

For a single-user local container, connect to Spark through its localhost endpoint.
Database `HOST` values must resolve from inside the container; `localhost` there
refers to the container itself.

The `.env` file uses `HOST`, `PORT`, `DATABASE`, `USER` and `PASSWORD`.
Declared values must be filled in directly; `${...}` references are rejected.
Omit `env_file` to use process environment variables instead.

Engine names are case-insensitive and validated at construction. Use a hostname
or IP address in `HOST` and a separate optional `PORT` (1–65535), not a JDBC URL.
IPv6 addresses and Microsoft SQL `host\instance` names are supported. PostgreSQL
database names are URL-encoded; Microsoft SQL names with special characters are
brace-escaped (closing braces require Microsoft JDBC 8.4+). Oracle `DATABASE`
is a service name containing letters, digits, underscores, dots, dollar signs or
hyphens. With `password_provider`, endpoint settings are validated before
retrieving the password. A `connection_provider` returns all settings together,
which are validated after its single call.
Local query, schema, partition and target arguments are checked before invoking
either credentials provider; result-column validation still requires JDBC access.

## JDBC timeouts

`read_jdbc()`, `feed_spark()` and `write_jdbc()` accept three optional keyword-only
timeouts, expressed as positive integer seconds. All default to `None`, which
omits the property and preserves the existing Spark/driver defaults. Zero,
negative values, booleans and fractional seconds are rejected before either
credentials provider is called.

```python
frames = octopus.read_jdbc(
    [("SELECT * FROM customers", "customers")],
    connect_timeout=15,
    query_timeout=300,
    socket_timeout=360,
)
```

| Parameter | Purpose | PostgreSQL | Microsoft SQL | Oracle Thin |
| --- | --- | --- | --- | --- |
| `connect_timeout` | Connection establishment | `connectTimeout` + `loginTimeout` (seconds) | `loginTimeout` (seconds) | `oracle.net.CONNECT_TIMEOUT` (milliseconds) + `oracle.net.OUTBOUND_CONNECT_TIMEOUT` (seconds) |
| `query_timeout` | Individual statement execution | Spark `queryTimeout` (seconds) | Spark `queryTimeout` (seconds) | Spark `queryTimeout` (seconds) |
| `socket_timeout` | Waiting for socket reads | `socketTimeout` (seconds) | `socketTimeout` (milliseconds) | `oracle.jdbc.ReadTimeout` (milliseconds) |

DataBay converts units automatically. `connect_timeout` is limited to 65,535
seconds for Microsoft SQL and 2,147,483 seconds for PostgreSQL/Oracle.
`socket_timeout` is limited to 2,147,483 seconds and `query_timeout` to
2,147,483,647 seconds to avoid overflowing driver integers.

These settings are not a total deadline for a Spark job or a multi-table call.
They do not limit password prompts, Bitwarden CLI commands or Spark Connect RPCs.
Driver behavior varies: query cancellation and write-batch limits depend on the
driver, while socket timeouts may close a connection or interrupt a long query.
Keep `socket_timeout` above `query_timeout` if queries need that much time to run.
Oracle's outbound timeout excludes user authentication and applies per address;
DNS resolution and driver retries can also affect elapsed time.

Returned lazy DataFrames retain their timeout settings for subsequent actions.
Calling Octopus again does not change existing DataFrames. Failed multi-table
operations can leave earlier writes committed; DataBay does not automatically
retry timed-out writes.

See the official [Spark JDBC options](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
[PostgreSQL properties](https://jdbc.postgresql.org/documentation/use/),
[Microsoft SQL properties](https://learn.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties),
and [Oracle connection properties](https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/oracle/jdbc/OracleConnection.html).

## Optional Bitwarden

Keep connection settings in `.env` and obtain the password interactively from Bitwarden:

```python
from databay.etl import Octopus, bitwarden_password

octopus = Octopus(
    spark=spark,
    engine="postgresql",
    env_file=".env",
    password_provider=lambda: bitwarden_password("ITEM_ID"),
)
```

Infrastructure provides `bw` and an already logged-in account. At each JDBC
operation, the provider prompts for the master password using a hidden input,
unlocks the vault, retrieves the selected password, and locks the vault in a
`finally` block. Master password and session are passed only to child processes;
they are not saved in the notebook's environment. A lock failure stops the operation.
The provider overrides `PASSWORD`, so that key can be omitted from `.env`.
Failures raise an error without falling back to another password.
If hidden input is unavailable, the operation fails before reading the password.
Bitwarden CLI output is decoded as UTF-8, including non-ASCII credentials.

Vault lifecycle failures raise `BitwardenError`, a `RuntimeError` subclass exported
from `databay.etl`. Its `stage` is `prompt`, `unlock`, `get` or `lock`; `lock_failed`
indicates that the final lock command failed. If both the original operation and
locking fail, both are reported and `stage` retains the original operation.
Octopus preserves this exception through both providers. Other provider exceptions
still receive generic messages to avoid exposing arbitrary provider output.

Credentials are never returned after a lock failure. Run `bw lock` manually when
the error recommends it; the vault's lock state is unknown. DataBay makes one
cleanup attempt and does not retry automatically. `KeyboardInterrupt` remains a
cancellation; if locking also fails or is interrupted, it carries `stage` and
`lock_failed=True` and a safe message recommending `bw lock`. No raw CLI output,
master password or session is included in these error messages.

Local secret references are dropped after use; Python cannot guarantee that
strings are erased from memory. The database password must remain available to
Spark/JDBC, including lazy DataFrames. Locking also invalidates existing CLI
sessions for this account/configuration. This interactive provider is intended
for sequential notebook use, not concurrent or unattended jobs.

Read all connection settings from one Bitwarden login item:

```python
from databay.etl import Octopus, bitwarden_connection

octopus = Octopus(
    spark=spark,
    engine="postgresql",
    connection_provider=lambda: bitwarden_connection("YOUR_BITWARDEN_ITEM_ID"),
)
```

Use the item's standard Username and Password fields and custom `host`,
`database`, and optional `port` fields (case-insensitive names). The helper
prompts, retrieves the entire item once, and locks before returning settings.
The connection provider runs once per JDBC operation, with no environment
fallback or instance caching. It cannot be combined with `env_file` or
`password_provider`. Select the database engine explicitly on Octopus.
See [the PostgreSQL notebook](../examples/postgres_bitwarden.ipynb).
