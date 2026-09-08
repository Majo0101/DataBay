from __future__ import annotations

from pyspark.sql import SparkSession

def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a specific port is open on a given host.
    Tests both IPv4 and IPv6 connections.

    Args:
        host: Hostname or IP address to check
        port: Port number to test
        timeout: Connection timeout in seconds (default: 1.0)

    Returns:
        True if port is open and accepting connections, False otherwise
    """
    ...

def spark_connect(
    app_name: str = "DataBay",
    host: str = "localhost",
    port: int = 15002,
    timeout: int = 8,
    check_interval: float = 0.2,
) -> SparkSession:
    """Connect to a Spark cluster via Spark Connect protocol.
    Waits for the port to become available before establishing connection.

    Args:
        app_name: Name for the Spark application (default: "DataBay")
        host: Hostname or IP address of Spark Connect server (default: "localhost")
        port: Port number for Spark Connect (default: 15002)
        timeout: Maximum time in seconds to wait for connection (default: 8)
        check_interval: Time in seconds between connection attempts (default: 0.2)

    Returns:
        SparkSession connected to the remote Spark cluster

    Raises:
        TimeoutError: If Spark Connect is not available within the timeout period
    """
    ...

def sparksql_magic(spark: SparkSession) -> bool:
    """%%sparksql                 → df.show()
    %%sparksql pandas          → display up to 10000 Pandas rows
    %%sparksql pandas customers_pd --limit 5000 → display and assign Pandas result
    %%sparksql varname         → assign df to a Python variable
    %%sparksql view viewname   → register df as a Spark temporary view
    Supports {python_variable} placeholders inside SQL queries.
    Pandas syntax: pandas [variable] [--limit positive_integer].
    The variable must be a Python identifier, not a keyword; an existing variable
    is replaced after successful conversion. The default limit is 10000.
    Spark applies limit + 1 before toPandas to detect truncation in one action;
    the extra row is discarded and a warning is issued when rows are omitted.
    A row limit is not a memory limit: wide rows may still use substantial RAM.
    Without ORDER BY, the selected rows are not guaranteed.
    """
    ...
