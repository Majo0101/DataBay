import os
import csv
import time

from typing import Optional
from databay.runtime.docker import DockConfig

from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class Octopus:
    """
    Data integration utility for Spark operations with database connectivity.
    Handles JDBC connections, data extraction, and CSV loading.
    """
    
    def __init__(self, env_file=None, spark=None, engine=None):
        """
        Initialize Octopus with optional environment file and SparkSession.
        
        Args:
            env_file: Path to .env file containing database credentials
            spark: Active SparkSession instance (optional)
            engine: Database engine type (postgresql, mssql, oracle) (optional)

        Notes:
            Credentials use HOST, PORT, DATABASE, USER and PASSWORD. A nonempty
            env file supplies the instance settings; otherwise OS environment is
            used. JDBC runs on the Spark server, so HOST must be reachable there.

        Example:
            >>> octopus = Octopus(env_file=".env", spark=spark, engine="postgresql")
        """
        self.os = os
        self.time = time
        self.csv = csv
        self.spark = spark
        self.engine = engine
        self.env_vars = {}  # Store instance-specific environment variables

        if env_file:
            from dotenv import dotenv_values # type: ignore
            self.env_vars = dotenv_values(env_file)


    def _get_env(self, key: str):
        """Get environment variable from instance dict or global OS environment"""
        if self.env_vars:
            return self.env_vars.get(key)
        return self.os.getenv(key)

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

    def _jdbc_base_options(self, engine: str):
        """
        Build base JDBC connection options from environment variables.
        Supports both username/password and interactive authentication for MSSQL.
        
        Args:
            engine: Database engine type (postgresql, mssql, oracle)
            
        Returns:
            Dict containing user, password/auth, and driver configuration
            
        Notes:
            For MSSQL with interactive auth: authentication and enableDeviceCodeFlow 
            are set both in URL and as properties for maximum compatibility.
        """
        opts = {
            "user": self._get_env("USER"),
            "driver": self._jdbc_driver(engine),
        }
        
        password = self._get_env("PASSWORD")
        
        # For MSSQL with interactive auth, add device code flow property
        if engine.lower() in ("mssql", "sqlserver") and not password:
            opts["authentication"] = "ActiveDirectoryInteractive"
            opts["enableDeviceCodeFlow"] = "true"
        else:
            opts["password"] = password
            
        return opts

    def _jdbc_url(self, engine: str):
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
            
        Notes:
            For MSSQL with interactive auth, DATABASE is optional.
            Device code flow is enabled for MSSQL when no password is provided.
        """
        host = self._get_env("HOST")
        port = self._get_env("PORT")
        db = self._get_env("DATABASE")
        password = self._get_env("PASSWORD")

        # For MSSQL with interactive auth, DATABASE is optional
        if engine in ("mssql", "sqlserver") and not password:
            if not host:
                raise RuntimeError(f"Missing required environment variable: HOST")
            
            base_url = f"jdbc:sqlserver://{host}"
            if port:
                base_url += f":{port}"
            
            # Add authentication and device code flow parameters
            params = "authentication=ActiveDirectoryInteractive;enableDeviceCodeFlow=true"
            
            if db:
                return f"{base_url};databaseName={db};{params}"
            else:
                return f"{base_url};{params}"
        
        # For all other cases, DATABASE is required
        if not all([host, db]):
            missing = [var for var, val in [("HOST", host), ("DATABASE", db)] if not val]
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

        if engine in ("postgre", "postgres", "postgresql"):
            return f"jdbc:postgresql://{host}:{port}/{db}" if port else f"jdbc:postgresql://{host}/{db}"

        if engine in ("mssql", "sqlserver"):
            base_url = f"jdbc:sqlserver://{host}"
            if port:
                base_url += f":{port}"
            return f"{base_url};databaseName={db}"

        if engine == "oracle":
            return f"jdbc:oracle:thin:@//{host}:{port}/{db}" if port else f"jdbc:oracle:thin:@//{host}/{db}"

        raise ValueError(f"Unsupported engine: {engine}")


    def _load_jdbc(self, reader, schema: Optional[StructType]):
        """Apply JDBC type overrides and reject names absent from the query result."""
        if schema is not None:
            if not isinstance(schema, StructType):
                raise TypeError("schema must be a pyspark.sql.types.StructType")
            names = schema.fieldNames()
            if len(names) != len(set(names)):
                raise ValueError("schema must not contain duplicate column names")
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
    ):
        """
        Load data from database via JDBC into Delta or Iceberg tables.
        Creates schema if it doesn't exist and overwrites existing tables.

        Parallel reads require partition_column, lower_bound < upper_bound and
        num_partitions >= 1. Bounds divide reads; they do not filter source rows.
        These options are ignored when parallel_read=False. The Spark server
        requires the selected format and its catalog configuration. This method
        writes immediately and returns None. Tables are written sequentially;
        a later failure does not roll back earlier writes.
        
        Args:
            queries: List of (query, table_name) tuples to execute and save
            target_schema: Target namespace, optionally catalog-qualified
                (for example "lake.raw" for the bundled Iceberg runtime).
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

        url = self._jdbc_url(self.engine)
        opts = self._jdbc_base_options(self.engine)
        
        # Add MSSQL-specific SSL/encryption options
        if self.engine.lower() in ("mssql", "sqlserver"):
            opts["trustServerCertificate"] = str(trust_server_certificate).lower()
            opts["encrypt"] = str(encrypt).lower()

        if parallel_read:
            if not partition_column:
                raise ValueError("partition_column is required when parallel_read=True")
            if lower_bound is None or upper_bound is None:
                raise ValueError("lower_bound and upper_bound are required when parallel_read=True")
            if num_partitions is None or num_partitions < 1:
                raise ValueError("num_partitions must be >= 1 when parallel_read=True")
            if lower_bound >= upper_bound:
                raise ValueError("lower_bound must be less than upper_bound")

        for query, table_name in queries:
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
                df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])

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
    ):
        """
        Read data from database via JDBC into Spark DataFrames (without writing tables).

        Returns lazy Spark DataFrames keyed by the supplied aliases, not Pandas
        data or collected rows. Later actions execute the reads. Repeated aliases
        replace earlier entries in the returned dictionary.
        Parallel reads require partition_column, lower_bound < upper_bound and
        num_partitions >= 1. Bounds divide reads; they do not filter source rows.
        These options are ignored when parallel_read=False.
        
        Args:
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

        url = self._jdbc_url(self.engine)
        opts = self._jdbc_base_options(self.engine)

        # Add MSSQL-specific SSL/encryption options
        if self.engine.lower() in ("mssql", "sqlserver"):
            opts["trustServerCertificate"] = str(trust_server_certificate).lower()
            opts["encrypt"] = str(encrypt).lower()

        if parallel_read:
            if not partition_column:
                raise ValueError("partition_column is required when parallel_read=True")
            if lower_bound is None or upper_bound is None:
                raise ValueError("lower_bound and upper_bound are required when parallel_read=True")
            if num_partitions is None or num_partitions < 1:
                raise ValueError("num_partitions must be >= 1 when parallel_read=True")
            if lower_bound >= upper_bound:
                raise ValueError("lower_bound must be less than upper_bound")

        dfs = {}

        for query, table_name in queries:
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
                df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])

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
    ):
        """
        Write Spark DataFrame(s) to a JDBC database table.
        
        Args:
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

        url = self._jdbc_url(self.engine)
        opts = self._jdbc_base_options(self.engine)

        # Add MSSQL-specific SSL/encryption options
        if self.engine.lower() in ("mssql", "sqlserver"):
            opts["trustServerCertificate"] = str(trust_server_certificate).lower()
            opts["encrypt"] = str(encrypt).lower()

        targets = {}
        if isinstance(data, dict):
            targets = data
            if target_table is not None:
                raise ValueError("target_table must be None when data is a dict[str, DataFrame]")
        else:
            if not target_table:
                raise ValueError("target_table is required when data is a single DataFrame")
            targets[target_table] = data

        for table_name, df in targets.items():
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
        spark,
        sources,
        dock_cfg: DockConfig,
        delimiter="|",
        header=True,
        infer_schema=False,
        mode="both",
        csv_read_mode="PERMISSIVE",
    ):
        """
        Load CSV files from Docker-mounted volumes into Spark.
        
        Args:
            spark: Active SparkSession; None falls back to the instance session
            sources: List of (container_path, table_name) tuples where container_path starts with '/'
            dock_cfg: DockConfig containing bind_mounts mapping
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
            ...     spark, [("/data/apache/customers.csv", "customers")],
            ...     dock_cfg, delimiter=",", mode="both")
            
        Raises:
            RuntimeError: If SparkSession is None
            ValueError: If container_path does not start with a mounted path
        """
        if spark is None:
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

        for container_path, name in sources:
            
            # Basic validation - container path must start with /
            if not container_path.startswith("/"):
                raise ValueError(f"Container path must start with '/': {container_path}")
            
            # Check if path starts with one of the mounted container paths
            valid_mount = False
            for mount_container in dock_cfg.bind_mounts.values():
                if container_path.startswith(mount_container):
                    valid_mount = True
                    break
            
            if not valid_mount:
                mounted_paths = ", ".join(dock_cfg.bind_mounts.values())
                raise ValueError(
                    f"Container path '{container_path}' does not start with any mounted path: {mounted_paths}"
                )
            
            df = (
                spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .option("delimiter", delimiter)
                .option("mode", csv_read_mode)
                .csv(container_path)
            )

            if create_view:
                df.createOrReplaceTempView(name)
            if keep_dfs and dfs is not None:
                dfs[name] = df

        return dfs
