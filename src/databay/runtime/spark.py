import time

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

    return spark


def sparksql_magic(spark:SparkSession):
    """
    %%sparksql                 → df.show()
    %%sparksql pandas          → display Pandas DataFrame
    %%sparksql varname         → assign df to a Python variable
    %%sparksql view viewname   → register df as a Spark temporary view
    Supports {python_variable} placeholders inside SQL queries.
    """

    @register_cell_magic
    def sparksql(line, cell):
        if spark is None:
            raise RuntimeError("No active SparkSession found.")

        # Clean cell content (no Quarto dependency)
        query_raw = cell.strip()

        # Inject Python variables into SQL
        ns = get_ipython().user_ns # pyright: ignore[reportOptionalMemberAccess]
        try:
            query = query_raw.format(**ns)
        except KeyError as e:
            raise KeyError(f"Missing Python variable in SQL template: {e}")

        df = spark.sql(query)
        args = line.strip().split()

        if not args:
            df.show(truncate=False)
            return

        if len(args) == 1 and args[0].lower() == "pandas":
            display(df.toPandas())
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