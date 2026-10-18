from __future__ import annotations

from typing import Callable, Dict, List, Literal, Optional, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class Octopus:
    """Data integration utility for Spark operations with database connectivity.
    Handles JDBC connections, data extraction, and CSV loading.
    """

    def __init__(
        self,
        env_file: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        engine: Optional[str] = None,
        *,
        password_provider: Optional[Callable[[], str]] = None,
        connection_provider: Optional[Callable[[], dict[str, str]]] = None,
    ) -> None:
        """Initialize Octopus with optional environment file and SparkSession.

        Args:
            env_file: Path to .env file containing database credentials
            spark: Active SparkSession instance (optional)
            engine: Database engine type (postgresql, mssql, oracle) (optional).
                Validated and normalized to lowercase at construction.
            connection_provider: Optional callable returning HOST, DATABASE and
                optional PORT with USER/PASSWORD.
                Cannot be combined with env_file or password_provider.
                Called once per JDBC operation without environment fallback.
            password_provider: Optional zero-argument callable returning a password.
                Called once per read_jdbc/feed_spark/write_jdbc invocation, not at
                construction. Overrides PASSWORD without fallback on failure.

        Notes:
            Credentials use HOST, PORT, DATABASE, USER and PASSWORD. Without
            env_file, read the OS environment. An explicit file is the sole
            settings source; every declared value must be nonempty and literal.
            Variable references (${...}) are rejected; no interpolation occurs.
            With password_provider, the other settings still come from the
            selected source; PASSWORD can be omitted from the file. Provider
            results are not cached in this instance or written to the file.
            Returned lazy DataFrames retain their operation's password snapshot.
            JDBC runs on the Spark server, so HOST must be reachable there.

        Raises:
            OSError: If an explicit credentials file cannot be opened.
            ValueError: If the file contains no settings, empty values or variable references.

        Example:
            >>> octopus = Octopus(env_file=".env", spark=spark, engine="postgresql")
        """
        ...

    def feed_spark(
        self,
        queries: List[Tuple[str, str]],
        target_schema: str,
        batch_size: int = 10000,
        num_partitions: Optional[int] = None,
        parallel_read: bool = False,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        infer_schema: bool = True,
        schema: Optional[StructType] = None,
        trust_server_certificate: bool = True,
        encrypt: bool = False,
        table_format: Literal["delta", "iceberg"] = "delta",
        *,
        connect_timeout: Optional[int] = None,
        query_timeout: Optional[int] = None,
        socket_timeout: Optional[int] = None,
    ) -> None:
        """Load data from database via JDBC into Delta or Iceberg tables.
        Creates schema if it doesn't exist and overwrites existing tables.

        Parallel reads require partition_column, lower_bound < upper_bound and
        num_partitions >= 1. Bounds divide reads; they do not filter source rows.
        These options are ignored when parallel_read=False. The Spark server
        requires the selected format and its catalog configuration. This method
        writes immediately and returns None. Tables are written sequentially;
        a later failure does not roll back earlier writes.

        Timeouts are driver-enforced, not an overall Spark job deadline. They do
        not bound credential prompts or Spark Connect RPCs. Lazy DataFrames retain
        their timeout settings for later actions. See README for property mappings.

        Args:
            connect_timeout: Optional positive connection timeout in seconds.
                None preserves driver defaults; limits vary by engine.
            query_timeout: Optional positive per-statement timeout in seconds.
                None preserves Spark defaults. Write/batch behavior depends on the driver.
            socket_timeout: Optional positive socket read timeout in seconds.
                None preserves driver defaults. This can also interrupt a running query.
            queries: List of (query, table_name) tuples to execute and save
            target_schema: Target namespace, optionally catalog-qualified
                (for example "lake.raw" for an Iceberg catalog).
            batch_size: Number of rows to fetch per round trip (default: 10000)
            num_partitions: Number of JDBC partitions when parallel_read=True
            parallel_read: Enable JDBC parallel read partitioning (default: False)
            partition_column: Numeric/date column used for JDBC partitioning
            lower_bound: Minimum bound for partition_column when parallel_read=True
            upper_bound: Maximum bound for partition_column when parallel_read=True
            infer_schema: If True, uses JDBC metadata; if False, casts all to string (default: True)
            schema: Optional StructType of JDBC read type overrides (full or partial).
                    Names must exactly match query result columns; unspecified columns keep
                    JDBC-inferred types. Takes precedence over infer_schema. Conversions
                    depend on JDBC driver support; nullability/metadata are not enforced.
            trust_server_certificate: For MSSQL, trust server certificate (default: True)
            encrypt: For MSSQL, use encryption for connection (default: False)
            table_format: "delta" (default) or "iceberg". Delta retains the existing
                overwrite behavior. Iceberg uses createOrReplace, replacing table
                data and schema. This does not convert existing tables between formats
                or configure/install the server catalog.

        Raises:
            RuntimeError: If SparkSession or engine is not initialized
            ValueError: If parallel-read settings or schema overrides are invalid

        Example:
            >>> octopus.feed_spark([("SELECT * FROM customers", "customers")], "raw")
            >>> octopus.feed_spark(
            ...     [("SELECT * FROM customers", "customers")],
            ...     "lake.raw", table_format="iceberg")
        """
        ...

    def read_jdbc(
        self,
        queries: List[Tuple[str, str]],
        batch_size: int = 10000,
        num_partitions: Optional[int] = None,
        parallel_read: bool = False,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        infer_schema: bool = True,
        schema: Optional[StructType] = None,
        trust_server_certificate: bool = True,
        encrypt: bool = False,
        *,
        connect_timeout: Optional[int] = None,
        query_timeout: Optional[int] = None,
        socket_timeout: Optional[int] = None,
    ) -> Dict[str, DataFrame]:
        """Read data from database via JDBC into Spark DataFrames (without writing tables).

        Returns lazy Spark DataFrames keyed by the supplied aliases, not Pandas
        data or collected rows. Later actions execute the reads. Repeated aliases
        replace earlier entries in the returned dictionary.
        Parallel reads require partition_column, lower_bound < upper_bound and
        num_partitions >= 1. Bounds divide reads; they do not filter source rows.
        These options are ignored when parallel_read=False.

        Timeouts are driver-enforced, not an overall Spark job deadline. They do
        not bound credential prompts or Spark Connect RPCs. Lazy DataFrames retain
        their timeout settings for later actions. See README for property mappings.

        Args:
            connect_timeout: Optional positive connection timeout in seconds.
                None preserves driver defaults; limits vary by engine.
            query_timeout: Optional positive per-statement timeout in seconds.
                None preserves Spark defaults. Write/batch behavior depends on the driver.
            socket_timeout: Optional positive socket read timeout in seconds.
                None preserves driver defaults. This can also interrupt a running query.
            queries: List of (query, alias) tuples used to construct DataFrames
            batch_size: Number of rows to fetch per round trip (default: 10000)
            num_partitions: Number of JDBC partitions when parallel_read=True
            parallel_read: Enable JDBC parallel read partitioning (default: False)
            partition_column: Numeric/date column used for JDBC partitioning
            lower_bound: Minimum bound for partition_column when parallel_read=True
            upper_bound: Maximum bound for partition_column when parallel_read=True
            infer_schema: If True, uses JDBC metadata; if False, casts all to string (default: True)
            schema: Optional StructType of JDBC read type overrides (full or partial).
                    Names must exactly match query result columns; unspecified columns keep
                    JDBC-inferred types. Takes precedence over infer_schema. Conversions
                    depend on JDBC driver support; nullability/metadata are not enforced.
            trust_server_certificate: For MSSQL, trust server certificate (default: True)
            encrypt: For MSSQL, use encryption for connection (default: False)

        Returns:
            Dict[str, DataFrame]: Mapping of table_name to loaded Spark DataFrame

        Raises:
            RuntimeError: If SparkSession or engine is not initialized
            ValueError: If parallel_read options are invalid

        Example:
            >>> frames = octopus.read_jdbc([("SELECT * FROM customers", "customers")])
            >>> frames["customers"].show()
        """
        ...

    def write_jdbc(
        self,
        data: Union[DataFrame, Dict[str, DataFrame]],
        target_table: Optional[str] = None,
        target_schema: Optional[str] = None,
        mode: str = "append",
        batch_size: int = 10000,
        truncate: bool = False,
        trust_server_certificate: bool = True,
        encrypt: bool = False,
        num_partitions: int = 4,
        *,
        connect_timeout: Optional[int] = None,
        query_timeout: Optional[int] = None,
        socket_timeout: Optional[int] = None,
    ) -> None:
        """Write Spark DataFrame(s) to a JDBC database table.

        Timeouts are driver-enforced, not an overall Spark job deadline. They do
        not bound credential prompts or Spark Connect RPCs. Lazy DataFrames retain
        their timeout settings for later actions. See README for property mappings.

        Args:
            connect_timeout: Optional positive connection timeout in seconds.
                None preserves driver defaults; limits vary by engine.
            query_timeout: Optional positive per-statement timeout in seconds.
                None preserves Spark defaults. Write/batch behavior depends on the driver.
            socket_timeout: Optional positive socket read timeout in seconds.
                None preserves driver defaults. This can also interrupt a running query.
            data: DataFrame or dict[str, DataFrame]. If dict, keys are table names.
            target_table: Required when data is a single DataFrame.
            target_schema: Optional schema prefix for destination table(s).
            mode: Write mode - "append", "overwrite", "error", "errorifexists", "ignore".
            batch_size: Number of rows per write batch (default: 10000).
            truncate: For overwrite mode, request table truncation instead of drop/recreate.
            trust_server_certificate: For MSSQL, trust server certificate (default: True).
            encrypt: For MSSQL, use encryption for connection (default: False).
            num_partitions: Positive integer limiting parallel JDBC write partitions
                per table (default: 4). Spark coalesces excess partitions; fewer
                input partitions are not increased. Applies to each table in a
                dict, written sequentially. This is not a database-wide connection
                limit across clients. A lower value can reduce database load but
                increase write duration.

        Raises:
            RuntimeError: If SparkSession or engine is not initialized.
            ValueError: If arguments are invalid.
        """
        ...

    def load_csv(
        self,
        sources: List[Tuple[str, str]],
        delimiter: str = "|",
        header: bool = True,
        infer_schema: bool = False,
        mode: str = "both",
        csv_read_mode: str = "PERMISSIVE",
    ) -> Optional[Dict[str, DataFrame]]:
        """Load CSV files using the instance session and Spark-accessible paths or URIs.

        Args:
            sources: List of (path, table_name) tuples; paths are resolved by Spark
            delimiter: CSV delimiter character (default: "|")
            header: Whether CSV has header row (default: True)
            infer_schema: Whether to infer schema from data (default: False)
            mode: Output mode - "view", "dfs", or "both" (default: "both")
            csv_read_mode: Spark CSV parser mode - "PERMISSIVE", "DROPMALFORMED", or "FAILFAST"

        Returns:
            None for mode="view"; dict of {table_name: DataFrame} for "dfs" or
            "both". The latter also registers temporary views.

        Example:
            >>> frames = octopus.load_csv(
            ...     [("/data/apache/customers.csv", "customers")],
            ...     delimiter=",", mode="both")

        Raises:
            RuntimeError: If SparkSession is None
            ValueError: If an output or CSV parser mode is invalid
        """
        ...
