import os
import csv
import time
import ipaddress
import re
from urllib.parse import quote

from typing import Callable, Optional

from pyspark.sql.types import StructType

from databay.core._columns import literal_col
from databay.etl.credentials import BitwardenError
from databay.etl._status import SingleLineStatus


def _table_progress(total):
    """Replace one notebook display, or one terminal line, as tables advance."""
    status = SingleLineStatus(carriage_return=True)
    return lambda index, name: status.update(f"[{index}/{total}] {name}")


class Octopus:
    """
    Data integration utility for Spark operations with database connectivity.
    Handles JDBC connections, data extraction, and CSV loading.
    """
    
    def __init__(
        self, env_file=None, spark=None, engine=None, *,
        password_provider: Optional[Callable[[], str]] = None,
        connection_provider: Optional[Callable[[], dict[str, str]]] = None,
    ):
        """
        Initialize Octopus with optional environment file and SparkSession.
        
        Args:
            env_file: Path to .env file containing database credentials
            spark: Active SparkSession instance (optional)
            engine: Database engine type (postgresql, mssql, oracle) (optional).
                Validated and normalized to lowercase at construction.
            connection_provider: Optional zero-argument callable returning HOST, PORT,
                DATABASE, USER and PASSWORD in a dictionary. PORT is optional.
                Called once per JDBC operation; never falls back to environment.
                Cannot be combined with env_file or password_provider.
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
        self.os = os
        self.time = time
        self.csv = csv
        self.spark = spark
        if engine is not None:
            if not isinstance(engine, str):
                raise ValueError("engine must be a supported database engine name")
            engine = engine.strip().lower()
            self._jdbc_driver(engine)
        self.engine = engine
        if password_provider is not None and not callable(password_provider):
            raise TypeError("password_provider must be callable")
        if connection_provider is not None:
            if not callable(connection_provider):
                raise TypeError("connection_provider must be callable")
            if env_file is not None or password_provider is not None:
                raise ValueError("connection_provider cannot be combined with env_file or password_provider")
        self._connection_provider = connection_provider
        self._password_provider = password_provider
        self.env_vars = None  # None selects the OS environment; a dict selects explicit settings.

        if env_file is not None:
            from dotenv import dotenv_values
            with open(env_file, encoding="utf-8") as stream:
                self.env_vars = dotenv_values(stream=stream, interpolate=False)
            if not self.env_vars:
                raise ValueError("Credentials file is empty or contains no settings")
            for key, value in self.env_vars.items():
                if value is None or not value.strip():
                    raise ValueError(f"Credentials setting {key!r} must have a nonempty literal value")
                if "${" in value:
                    raise ValueError(f"Credentials setting {key!r} must not reference other variables")


    def _get_env(self, key: str):
        """Get environment variable from instance dict or global OS environment"""
        if self.env_vars is not None:
            return self.env_vars.get(key)
        return self.os.getenv(key)

    def _jdbc_connection(self):
        """Resolve one password snapshot for the entire public JDBC operation."""
        settings = None
        if self._connection_provider is not None:
            try:
                supplied = self._connection_provider()
            except BitwardenError:
                raise
            except Exception:
                raise RuntimeError("Connection provider failed; no fallback settings were used") from None
            if not isinstance(supplied, dict):
                raise ValueError("Connection provider must return a settings dictionary")
            settings = dict(supplied)
            required = ("HOST", "DATABASE", "USER", "PASSWORD")
            for key in required:
                if not isinstance(settings.get(key), str) or not settings[key].strip():
                    raise ValueError(f"Connection provider requires nonempty {key}; no fallback settings were used")
            if settings.get("PORT") is not None and (
                not isinstance(settings["PORT"], str)
                or not settings["PORT"].isdecimal()
                or not 1 <= int(settings["PORT"]) <= 65535
            ):
                raise ValueError("Connection provider PORT must be a valid port string")
        password = None
        # Validate endpoint settings before invoking a password-only provider.
        url = self._jdbc_url(self.engine, settings=settings)
        if self._password_provider is not None:
            try:
                password = self._password_provider()
            except BitwardenError:
                raise
            except Exception:
                # Provider errors may contain secrets or captured CLI output.
                raise RuntimeError("Password provider failed; no fallback password was used") from None
            if not isinstance(password, str) or not password.strip():
                raise ValueError("Password provider must return a nonempty string; no fallback password was used")
        return (
            url,
            self._jdbc_base_options(self.engine, password=password, settings=settings),
        )

    def _jdbc_driver(self, engine: str):
        """Get JDBC driver class name for the specified engine"""
        drivers = {
            "postgresql": "org.postgresql.Driver",
            "postgres": "org.postgresql.Driver",
            "postgre": "org.postgresql.Driver",
            "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "oracle": "oracle.jdbc.OracleDriver",
        }
        
        driver = drivers.get(engine.lower())
        if not driver:
            raise ValueError(f"Unsupported engine: {engine}")
        return driver

    def _jdbc_base_options(self, engine: str, *, password: Optional[str] = None, settings=None):
        """Build username/password JDBC properties from the selected settings."""
        get_setting = self._get_env if settings is None else settings.get
        opts = {
            "user": get_setting("USER"),
            "driver": self._jdbc_driver(engine),
        }
        
        if password is None:
            password = get_setting("PASSWORD")
        
        opts["password"] = password

        return opts

    def _jdbc_url(self, engine: str, *, settings=None):
        """
        Construct JDBC connection URL based on database engine type.
        Reads HOST, PORT, and DATABASE from environment variables.
        
        Args:
            engine: Database engine type (postgresql/postgres/postgre, mssql/sqlserver, oracle)
            
        Returns:
            Formatted JDBC connection URL string
            
        Raises:
            ValueError: If engine type is not supported
            RuntimeError: If required environment variables are missing
            
        """
        get_setting = self._get_env if settings is None else settings.get
        host = get_setting("HOST")
        port = get_setting("PORT")
        db = get_setting("DATABASE")
        if not all([host, db]):
            missing = [var for var, val in [("HOST", host), ("DATABASE", db)] if not val]
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

        if not isinstance(host, str) or not isinstance(db, str) or not db.strip():
            raise ValueError("HOST and DATABASE must be nonempty strings")
        server, separator, instance = host.partition("\\")
        if separator and (engine not in ("mssql", "sqlserver") or not re.fullmatch(r"[\w$-]+", instance, re.ASCII)):
            raise ValueError("HOST contains an invalid SQL Server instance name")
        if ":" in server:
            address = server[1:-1] if server.startswith("[") and server.endswith("]") else server
            try:
                ipaddress.IPv6Address(address)
            except ValueError:
                raise ValueError("HOST must be a hostname or IP address; specify PORT separately") from None
            if "%" in address:
                raise ValueError("HOST must not contain an IPv6 scope identifier")
            server = f"[{address}]"
        else:
            try:
                ascii_host = server.encode("idna").decode("ascii")
            except UnicodeError:
                raise ValueError("HOST must be a valid hostname or IP address") from None
            labels = ascii_host.removesuffix(".").split(".")
            if len(ascii_host) > 254 or any(
                not re.fullmatch(r"[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?", label)
                for label in labels
            ):
                raise ValueError("HOST must be a valid hostname or IP address")
            server = ascii_host
        host = server + (f"\\{instance}" if separator else "")
        if port not in (None, "") and (
            not isinstance(port, str) or not port.isascii() or not port.isdecimal()
            or not 1 <= int(port) <= 65535
        ):
            raise ValueError("PORT must be a valid port string")
        if any(ord(char) < 32 or ord(char) == 127 for char in db):
            raise ValueError("DATABASE must not contain control characters")

        if engine in ("postgre", "postgres", "postgresql"):
            db = quote(db, safe="")
            return f"jdbc:postgresql://{host}:{port}/{db}" if port else f"jdbc:postgresql://{host}/{db}"

        if engine in ("mssql", "sqlserver"):
            if any(char in db for char in ";={}[]\"'") or db != db.strip() or " " in db:
                db = "{" + db.replace("}", "}}") + "}"
            base_url = f"jdbc:sqlserver://{host}"
            if port:
                base_url += f":{port}"
            return f"{base_url};databaseName={db}"

        if engine == "oracle":
            if not re.fullmatch(r"[\w.$-]+", db, re.ASCII):
                raise ValueError("DATABASE must be a valid Oracle service name")
            return f"jdbc:oracle:thin:@//{host}:{port}/{db}" if port else f"jdbc:oracle:thin:@//{host}/{db}"

        raise ValueError(f"Unsupported engine: {engine}")


    def _jdbc_timeout_options(self, connect_timeout, query_timeout, socket_timeout):
        """Validate seconds and map them to Spark/JDBC properties without I/O."""
        connect_limit = 65535 if self.engine in ("mssql", "sqlserver") else 2147483
        for name, value, limit in (
            ("connect_timeout", connect_timeout, connect_limit),
            ("query_timeout", query_timeout, 2147483647),
            ("socket_timeout", socket_timeout, 2147483),
        ):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= limit
            ):
                raise ValueError(f"{name} must be None or an integer between 1 and {limit} seconds")
        options = {}
        if query_timeout is not None:
            options["queryTimeout"] = str(query_timeout)
        if self.engine in ("postgresql", "postgres", "postgre"):
            if connect_timeout is not None:
                options["connectTimeout"] = str(connect_timeout)
                options["loginTimeout"] = str(connect_timeout)
            if socket_timeout is not None:
                options["socketTimeout"] = str(socket_timeout)
        elif self.engine in ("mssql", "sqlserver"):
            if connect_timeout is not None:
                options["loginTimeout"] = str(connect_timeout)
            if socket_timeout is not None:
                options["socketTimeout"] = str(socket_timeout * 1000)
        elif self.engine == "oracle":
            if connect_timeout is not None:
                options["oracle.net.CONNECT_TIMEOUT"] = str(connect_timeout * 1000)
                options["oracle.net.OUTBOUND_CONNECT_TIMEOUT"] = str(connect_timeout)
            if socket_timeout is not None:
                options["oracle.jdbc.ReadTimeout"] = str(socket_timeout * 1000)
        return options

    @staticmethod
    def _validate_schema(schema):
        if schema is not None:
            if not isinstance(schema, StructType):
                raise TypeError("schema must be a pyspark.sql.types.StructType")
            names = schema.fieldNames()
            if len(names) != len(set(names)):
                raise ValueError("schema must not contain duplicate column names")

    @staticmethod
    def _validate_name(value, name):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a nonempty string")

    def _validate_read_arguments(
        self, queries, batch_size, schema, parallel_read, partition_column,
        lower_bound, upper_bound, num_partitions,
    ):
        self._validate_schema(schema)
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0:
            raise ValueError("batch_size must be an integer >= 0 for JDBC reads")
        if parallel_read:
            if not isinstance(partition_column, str) or not partition_column.strip():
                raise ValueError("partition_column is required when parallel_read=True")
            if lower_bound is None or upper_bound is None:
                raise ValueError("lower_bound and upper_bound are required when parallel_read=True")
            if not isinstance(num_partitions, int) or isinstance(num_partitions, bool) or num_partitions < 1:
                raise ValueError("num_partitions must be >= 1 when parallel_read=True")
            try:
                ordered = lower_bound < upper_bound
            except TypeError:
                ordered = False
            if not ordered:
                raise ValueError("lower_bound must be less than upper_bound")
        try:
            queries = list(queries)
        except TypeError:
            raise ValueError("queries must contain (query, name) pairs") from None
        for entry in queries:
            if not isinstance(entry, (tuple, list)) or len(entry) != 2:
                raise ValueError("queries must contain (query, name) pairs")
            self._validate_name(entry[0], "query")
            self._validate_name(entry[1], "query name")
        return queries

    def _load_jdbc(self, reader, schema: Optional[StructType]):
        """Apply JDBC type overrides and reject names absent from the query result."""
        self._validate_schema(schema)
        if schema is not None:
            custom_schema = ", ".join(
                f"`{field.name.replace('`', '``')}` {field.dataType.simpleString()}"
                for field in schema.fields
            )
            reader = reader.option("customSchema", custom_schema)

        df = reader.load()
        if schema is not None:
            # Spark ignores unknown customSchema fields; fail before returning or writing data.
            columns = set(df.columns)
            missing = [name for name in schema.fieldNames() if name not in columns]
            if missing:
                raise ValueError(f"schema column(s) {missing!r} not found in JDBC query result")
        return df

    def feed_spark(
        self,
        queries,
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
        table_format: str = "delta",
        *,
        connect_timeout: Optional[int] = None,
        query_timeout: Optional[int] = None,
        socket_timeout: Optional[int] = None,
    ):
        """
        Load data from database via JDBC into Delta or Iceberg tables.
        Creates schema if it doesn't exist and overwrites existing tables.

        Parallel reads require partition_column, lower_bound < upper_bound and
        num_partitions >= 1. Bounds divide reads; they do not filter source rows.
        These options are ignored when parallel_read=False. The Spark server
        requires the selected format and its catalog configuration. This method
        writes immediately and returns None. Tables are written sequentially;
        a later failure does not roll back earlier writes. Progress messages show
        the current table and its position in the batch on one updating line.
        
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
        if self.spark is None:
            raise RuntimeError("SparkSession is not set")
        if self.engine is None:
            raise RuntimeError("Engine is not set")

        if table_format not in ("delta", "iceberg"):
            raise ValueError("table_format must be 'delta' or 'iceberg'")

        self._validate_name(target_schema, "target_schema")
        queries = self._validate_read_arguments(
            queries, batch_size, schema, parallel_read, partition_column,
            lower_bound, upper_bound, num_partitions,
        )

        timeout_options = self._jdbc_timeout_options(connect_timeout, query_timeout, socket_timeout)
        url, opts = self._jdbc_connection()
        opts.update(timeout_options)
        
        # Add MSSQL-specific SSL/encryption options
        if self.engine.lower() in ("mssql", "sqlserver"):
            opts["trustServerCertificate"] = str(trust_server_certificate).lower()
            opts["encrypt"] = str(encrypt).lower()

        progress = _table_progress(len(queries))
        for index, (query, table_name) in enumerate(queries, start=1):
            progress(index, table_name)
            reader = (
                self.spark.read
                .format("jdbc")
                .option("url", url)
                .option("dbtable", f"({query}) q")
                .option("fetchsize", batch_size)
                .options(**opts)
            )

            if parallel_read:
                reader = (
                    reader
                    .option("partitionColumn", partition_column)
                    .option("lowerBound", lower_bound)
                    .option("upperBound", upper_bound)
                    .option("numPartitions", num_partitions)
                )

            df = self._load_jdbc(reader, schema)

            if schema is None and not infer_schema:
                df = df.select([literal_col(c).cast("string").alias(c) for c in df.columns])

            target = f"{target_schema}.{table_name}"
            if table_format == "iceberg":
                self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_schema}")
                df.writeTo(target).using("iceberg").createOrReplace()
            else:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_schema}")
                df.write.format("delta").mode("overwrite").saveAsTable(target)

    def read_jdbc(
        self,
        queries,
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
    ):
        """
        Read data from database via JDBC into Spark DataFrames (without writing tables).

        Returns lazy Spark DataFrames keyed by the supplied aliases, not Pandas
        data or collected rows. Later actions execute the reads. Repeated aliases
        replace earlier entries in the returned dictionary. Progress messages report
        the current DataFrame on one updating line, not completed data downloads.
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
        if self.spark is None:
            raise RuntimeError("SparkSession is not set")
        if self.engine is None:
            raise RuntimeError("Engine is not set")

        queries = self._validate_read_arguments(
            queries, batch_size, schema, parallel_read, partition_column,
            lower_bound, upper_bound, num_partitions,
        )

        timeout_options = self._jdbc_timeout_options(connect_timeout, query_timeout, socket_timeout)
        url, opts = self._jdbc_connection()
        opts.update(timeout_options)

        # Add MSSQL-specific SSL/encryption options
        if self.engine.lower() in ("mssql", "sqlserver"):
            opts["trustServerCertificate"] = str(trust_server_certificate).lower()
            opts["encrypt"] = str(encrypt).lower()

        dfs = {}

        progress = _table_progress(len(queries))
        for index, (query, table_name) in enumerate(queries, start=1):
            progress(index, table_name)
            reader = (
                self.spark.read
                .format("jdbc")
                .option("url", url)
                .option("dbtable", f"({query}) q")
                .option("fetchsize", batch_size)
                .options(**opts)
            )

            if parallel_read:
                reader = (
                    reader
                    .option("partitionColumn", partition_column)
                    .option("lowerBound", lower_bound)
                    .option("upperBound", upper_bound)
                    .option("numPartitions", num_partitions)
                )

            df = self._load_jdbc(reader, schema)

            if schema is None and not infer_schema:
                df = df.select([literal_col(c).cast("string").alias(c) for c in df.columns])

            dfs[table_name] = df

        return dfs

    def write_jdbc(
        self,
        data,
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
    ):
        """
        Write Spark DataFrame(s) to a JDBC database table.
        Show the current table and batch position on one updating line.
        
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
        if self.spark is None:
            raise RuntimeError("SparkSession is not set")
        if self.engine is None:
            raise RuntimeError("Engine is not set")

        valid_modes = {"append", "overwrite", "error", "errorifexists", "ignore"}
        if mode not in valid_modes:
            raise ValueError(f"Unsupported write mode: {mode}")
        if (
            not isinstance(num_partitions, int)
            or isinstance(num_partitions, bool)
            or num_partitions < 1
        ):
            raise ValueError("num_partitions must be an integer >= 1")

        targets = {}
        if isinstance(data, dict):
            targets = data
            if target_table is not None:
                raise ValueError("target_table must be None when data is a dict[str, DataFrame]")
        else:
            if not target_table:
                raise ValueError("target_table is required when data is a single DataFrame")
            targets[target_table] = data

        if target_schema is not None:
            self._validate_name(target_schema, "target_schema")
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 1:
            raise ValueError("batch_size must be an integer >= 1 for JDBC writes")
        for table_name in targets:
            self._validate_name(table_name, "target_table")

        timeout_options = self._jdbc_timeout_options(connect_timeout, query_timeout, socket_timeout)
        url, opts = self._jdbc_connection()
        opts.update(timeout_options)

        # Add MSSQL-specific SSL/encryption options
        if self.engine.lower() in ("mssql", "sqlserver"):
            opts["trustServerCertificate"] = str(trust_server_certificate).lower()
            opts["encrypt"] = str(encrypt).lower()

        progress = _table_progress(len(targets))
        for index, (table_name, df) in enumerate(targets.items(), start=1):
            progress(index, table_name)
            if target_schema:
                dbtable = f"{target_schema}.{table_name}"
            else:
                dbtable = table_name

            writer = (
                df.write
                .format("jdbc")
                .option("url", url)
                .option("dbtable", dbtable)
                .option("batchsize", batch_size)
                .option("numPartitions", num_partitions)
                .options(**opts)
                .mode(mode)
            )

            if mode == "overwrite" and truncate:
                writer = writer.option("truncate", "true")

            writer.save()

    def load_csv(
        self,
        sources,
        delimiter="|",
        header=True,
        infer_schema=False,
        mode="both",
        csv_read_mode="PERMISSIVE",
    ):
        """
        Load CSV files using the instance session and Spark-accessible paths or URIs.
        
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
        spark = self.spark
        if spark is None:
            raise RuntimeError("SparkSession is not set")
        if mode not in {"view", "dfs", "both"}:
            raise ValueError("mode must be one of: 'view', 'dfs', 'both'")

        csv_read_mode = str(csv_read_mode).upper()
        if csv_read_mode not in {"PERMISSIVE", "DROPMALFORMED", "FAILFAST"}:
            raise ValueError(
                "csv_read_mode must be one of: 'PERMISSIVE', 'DROPMALFORMED', 'FAILFAST'"
            )

        create_view = mode in {"view", "both"}
        keep_dfs = mode in {"dfs", "both"}
        dfs = {} if keep_dfs else None

        for path, name in sources:
            df = (
                spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .option("delimiter", delimiter)
                .option("mode", csv_read_mode)
                .csv(path)
            )

            if create_view:
                df.createOrReplaceTempView(name)
            if keep_dfs and dfs is not None:
                dfs[name] = df

        return dfs
