import time
import os
import socket
from urllib.parse import urlsplit
import keyword
import re
import warnings

from pyspark.sql import SparkSession


def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check a TCP endpoint, resolving both IPv4 and IPv6 addresses."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


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
    if "SPARK_REMOTE" in os.environ or "SPARK_CONNECT_MODE_ENABLED" in os.environ:
        raise RuntimeError(
            "Direct Spark conflicts with the current Spark Connect environment. "
            "Use a separate Python process/kernel configured for direct Spark."
        )
    if master is not None and (not master or master.startswith("sc://")):
        raise ValueError("master must be a Spark master URL, not a Spark Connect URI")
    builder = SparkSession.builder.config("spark.api.mode", "classic").appName(app_name)
    if master is not None:
        builder = builder.master(master)
    return builder.getOrCreate()


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
    if timeout < 0 or check_interval <= 0:
        raise ValueError("timeout must be nonnegative and check_interval must be positive")
    if remote is None:
        remote = os.environ.get("SPARK_REMOTE")
    if remote is None:
        address = f"[{host}]" if ":" in host and not host.startswith("[") else host
        remote = f"sc://{address}:{port}"
    try:
        endpoint = urlsplit(remote)
        if endpoint.scheme != "sc" or not endpoint.hostname:
            raise ValueError
        endpoint_host = endpoint.hostname
        endpoint_port = endpoint.port if endpoint.port is not None else 15002
        if endpoint_port == 0:
            raise ValueError
    except (TypeError, ValueError):
        # Do not include the URI: it may contain credentials.
        raise ValueError("remote must be a valid sc://host[:port] Spark Connect URI") from None

    deadline = time.monotonic() + timeout
    while not is_port_open(endpoint_host, endpoint_port):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Spark Connect not available on {endpoint_host}:{endpoint_port} within {timeout}s"
            )
        time.sleep(min(check_interval, remaining))

    spark = (
        SparkSession.builder.appName(app_name)
        .remote(remote)
        .getOrCreate()
    )
    if reattachable_execute is True:
        spark.client.enable_reattachable_execute()
    elif reattachable_execute is False:
        spark.client.disable_reattachable_execute()
    return spark


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

    if spark is None:
        raise RuntimeError("SparkSession is not set")
    from IPython import get_ipython
    from IPython import display as ipython_display

    shell = get_ipython()
    if shell is None:
        raise RuntimeError("SQL magic requires an active IPython/Jupyter session")

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
                if len(remaining) != 1 or not remaining[0].startswith("--limit="):
                    raise ValueError("Usage: %%sparksql pandas [variable] [--limit=positive_integer]")
                try:
                    pandas_limit = int(remaining[0].split("=", 1)[1])
                except ValueError:
                    raise ValueError("--limit must be a positive integer") from None
                if not 1 <= pandas_limit <= 2147483646:
                    raise ValueError("--limit must be between 1 and 2147483646")

        view_name = None
        var_name = None
        if args and not pandas_mode:
            if args[0].lower() == "view":
                if len(args) != 2 or not args[1] or args[1].startswith("--"):
                    raise ValueError("Usage: %%sparksql view view_name")
                view_name = args[1]
            else:
                if len(args) != 1 or not args[0].isidentifier() or keyword.iskeyword(args[0]):
                    raise ValueError("Usage: %%sparksql [variable] | view view_name | pandas [variable] [--limit=positive_integer]")
                var_name = args[0]

        # Clean cell content (no Quarto dependency)
        query_raw = cell.strip()

        # Inject Python variables into SQL
        ns = shell.user_ns
        def replace_variable(match):
            name = match.group(1)
            if name not in ns:
                raise KeyError(f"Missing Python variable in SQL template: {name}")
            return str(ns[name])

        query = re.sub(r"(?<!\{)\{([^\W\d]\w*)\}(?!\})", replace_variable, query_raw)

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
            ipython_display.display(pandas_df)
            return

        if view_name is not None:
            df.createOrReplaceTempView(view_name)
            print(f"View '{view_name}' created.")
            return

        ns[var_name] = df
        print(f"Query result saved to variable '{var_name}'")

    shell.register_magic_function(sparksql, "cell", "sparksql")
    return True
