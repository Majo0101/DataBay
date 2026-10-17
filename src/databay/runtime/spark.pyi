from __future__ import annotations

from pyspark.sql import SparkSession


def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check a TCP endpoint, resolving both IPv4 and IPv6 addresses."""
    ...


def spark_direct(
    app_name: str = "DataBay",
    master: str | None = None,
) -> SparkSession:
    """Get or create a classic Spark session with a direct JVM connection.

    master: Optional Spark master (e.g. "local[2]" or "spark://host:7077").
        When omitted, use the configuration supplied by the environment.

    JDBC drivers, catalogs and resources come from the infrastructure.
    Does not stop existing sessions or change environment variables. If Connect
    is already selected by SPARK_REMOTE or SPARK_CONNECT_MODE_ENABLED, raises
    RuntimeError. Select the desired mode before starting the Python process.
    An existing classic session may be reused; startup settings such as the
    master and driver packages cannot be changed on a running SparkContext.
    The caller owns the returned session and decides when to stop it.
    """
    ...


def spark_connect(
    app_name: str = "DataBay",
    host: str = "localhost",
    port: int = 15002,
    timeout: float = 8,
    check_interval: float = 0.2,
    reattachable_execute: bool | None = None,
    *,
    remote: str | None = None,
) -> SparkSession:
    """Connect to an existing Spark Connect server.

    remote: Full sc:// URI, including optional TLS/authentication parameters.
        Overrides host/port. When omitted, use SPARK_REMOTE if configured,
        otherwise construct a URI from host and port.
    timeout: TCP readiness polling budget in seconds. Each TCP probe can take
        up to one second; this does not bound Spark RPCs.
    check_interval: Seconds between TCP readiness checks.
    reattachable_execute: None preserves the client setting (default).
        An explicit True/False overrides it, including on a reused session.

    JDBC drivers, catalogs and resources come from the server infrastructure.
    Does not start a server or stop existing sessions. getOrCreate() may reuse
    a Connect session; select a different endpoint in a separate Python process
    or after explicitly stopping the old session. The caller owns the session.
    """
    ...


def sparksql_magic(spark: SparkSession) -> bool:
    """
    %%sparksql                 → df.show()
    %%sparksql pandas          → display up to 10000 Pandas rows
    %%sparksql pandas customers_pd --limit=5000 → display and assign Pandas result
    %%sparksql varname         → assign df to a Python variable
    %%sparksql view viewname   → register df as a Spark temporary view
    Replaces only {python_variable} identifier placeholders in SQL queries.
    Other braces (JSON, regex quantifiers and doubled braces) remain unchanged.
    Pandas syntax: pandas [variable] [--limit=positive_integer].
    The variable must be a Python identifier, not a keyword; an existing variable
    is replaced after successful conversion. The default limit is 10000.
    Spark applies limit + 1 before toPandas to detect truncation in one action;
    the extra row is discarded and a warning is issued when rows are omitted.
    A row limit is not a memory limit: wide rows may still use substantial RAM.
    Without ORDER BY, the selected rows are not guaranteed.
    """
    ...
