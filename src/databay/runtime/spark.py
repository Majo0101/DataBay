import time
import keyword
import warnings

from pyspark.sql import SparkSession

from IPython.core.magic import register_cell_magic
from IPython.display import display
from IPython import get_ipython # type: ignore

def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """
    Check if a specific port is open on a given host.
    Tests both IPv4 and IPv6 connections.
    
    Args:
        host: Hostname or IP address to check
        port: Port number to test
        timeout: Connection timeout in seconds (default: 1.0)
    
    Returns:
        True if port is open and accepting connections, False otherwise
    """
    import socket
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            with socket.socket(family, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                if s.connect_ex((host, port)) == 0:
                    return True
        except:
            pass
    return False


def spark_connect(
    app_name: str = "DataBay",
    host: str = "localhost",
    port: int = 15002,
    timeout: int = 8,
    check_interval: float = 0.2,
    reattachable_execute: bool = False,
    ):
    """
    Connect to a Spark cluster via Spark Connect protocol.
    Waits for the port to become available before establishing connection.
    
    Args:
        app_name: Name for the Spark application (default: "DataBay")
        host: Hostname or IP address of Spark Connect server (default: "localhost")
        port: Port number for Spark Connect (default: 15002)
        timeout: Maximum time in seconds to wait for connection (default: 8)
        check_interval: Time in seconds between connection attempts (default: 0.2)
        reattachable_execute: Enable Spark Connect execution reattachment.
            The requested value is applied even when an existing session is reused.
            Disabled by default to avoid PySpark 4.0.1 client deadlocks during
            workloads with many short actions. Enable it for remote connections
            where recovering an interrupted result stream is more important.
    
    Returns:
        SparkSession connected to the remote Spark cluster
    
    Raises:
        TimeoutError: If Spark Connect is not available within the timeout period
    """
    start = time.time()

    while True:
        if is_port_open(host, port):
            break
        if time.time() - start > timeout:
            raise TimeoutError(f"Spark Connect not available on {host}:{port} within {timeout}s")
        time.sleep(check_interval)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .remote(f"sc://{host}:{port}")
        .getOrCreate()
    )

    if reattachable_execute:
        spark.client.enable_reattachable_execute()
    else:
        spark.client.disable_reattachable_execute()

    return spark


def sparksql_magic(spark:SparkSession):
    """
    %%sparksql                 → df.show()
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

    @register_cell_magic
    def sparksql(line, cell):
        if spark is None:
            raise RuntimeError("No active SparkSession found.")

        args = line.strip().split()
        pandas_mode = bool(args) and args[0].lower() == "pandas"
        pandas_variable = None
        pandas_limit = 10000
        if pandas_mode:
            remaining = args[1:]
            if remaining and not remaining[0].startswith("--"):
                pandas_variable = remaining.pop(0)
                if not pandas_variable.isidentifier() or keyword.iskeyword(pandas_variable):
                    raise ValueError("Pandas variable must be a valid Python identifier, not a keyword")
            if remaining:
                if len(remaining) != 2 or remaining[0] != "--limit":
                    raise ValueError("Usage: %%sparksql pandas [variable] [--limit positive_integer]")
                try:
                    pandas_limit = int(remaining[1])
                except ValueError:
                    raise ValueError("--limit must be a positive integer") from None
                if not 1 <= pandas_limit <= 2147483646:
                    raise ValueError("--limit must be between 1 and 2147483646")

        # Clean cell content (no Quarto dependency)
        query_raw = cell.strip()

        # Inject Python variables into SQL
        ns = get_ipython().user_ns # pyright: ignore[reportOptionalMemberAccess]
        try:
            query = query_raw.format(**ns)
        except KeyError as e:
            raise KeyError(f"Missing Python variable in SQL template: {e}")

        df = spark.sql(query)

        if not args:
            df.show(truncate=False)
            return

        if pandas_mode:
            pandas_df = df.limit(pandas_limit + 1).toPandas()
            if len(pandas_df) > pandas_limit:
                pandas_df = pandas_df.iloc[:pandas_limit].copy()
                warnings.warn(
                    f"Pandas result truncated to {pandas_limit} rows. "
                    "Increase --limit to retrieve more rows. A row limit is not "
                    "a memory limit; without ORDER BY the selected rows are not guaranteed.",
                    UserWarning,
                    stacklevel=2,
                )
            if pandas_variable is not None:
                ns[pandas_variable] = pandas_df
            display(pandas_df)
            return

        if len(args) == 2 and args[0].lower() == "view":
            view_name = args[1]
            df.createOrReplaceTempView(view_name)
            print(f"View '{view_name}' created.")
            return

        var_name = args[0]
        get_ipython().user_ns[var_name] = df # pyright: ignore[reportOptionalMemberAccess]
        print(f"Query result saved to variable '{var_name}'")

    return True
