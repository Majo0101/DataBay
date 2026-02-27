import os
import csv
import time

from typing import Optional, Any

from pyspark.sql import functions as F


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


    def dump_data(
        self,
        queries,
        output_path: str,
        format: str = "csv",
        delimiter: str = "|",
        batch_size: int = 10000,
    ):
        """
        Extract data from database via JDBC and save to files.
        
        Args:
            queries: List of (query, table_name) tuples to execute and save
            output_path: Target directory path for output files
            format: Output format - "csv", "parquet", or "delta" (default: "csv")
            delimiter: CSV delimiter character (default: "|")
            batch_size: Number of rows to fetch per round trip (default: 10000)
            
        Raises:
            RuntimeError: If SparkSession is not initialized
            ValueError: If output format is not supported
        """
        if self.spark is None:
            raise RuntimeError("SparkSession is not set")
        if self.engine is None:
            raise RuntimeError("Engine is not set")

        url = self._jdbc_url(self.engine)
        opts = self._jdbc_base_options(self.engine)

        for query, table_name in queries:
            df = (
                self.spark.read
                .format("jdbc")
                .option("url", url)
                .option("query", query)
                .option("fetchsize", batch_size)
                .options(**opts)
                .load()
            )

            target = f"{output_path}/{table_name}"

            if format == "csv":
                (
                    df.write
                    .mode("overwrite")
                    .option("header", True)
                    .option("delimiter", delimiter)
                    .csv(target)
                )

            elif format == "parquet":
                df.write.mode("overwrite").parquet(target)

            elif format == "delta":
                df.write.format("delta").mode("overwrite").save(target)

            else:
                raise ValueError(f"Unsupported format: {format}")


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
        schema: Optional[Any] = None,
        trust_server_certificate: bool = True,
        encrypt: bool = False,
    ):
        """
        Load data from database via JDBC into Spark Delta tables.
        Creates schema if it doesn't exist and overwrites existing tables.
        
        Args:
            queries: List of (query, table_name) tuples to execute and save
            target_schema: Target schema/database name in Spark catalog
            batch_size: Number of rows to fetch per round trip (default: 10000)
            num_partitions: Number of JDBC partitions when parallel_read=True
            parallel_read: Enable JDBC parallel read partitioning (default: False)
            partition_column: Numeric/date column used for JDBC partitioning
            lower_bound: Minimum bound for partition_column when parallel_read=True
            upper_bound: Maximum bound for partition_column when parallel_read=True
            infer_schema: If True, uses JDBC metadata; if False, casts all to string (default: True)
            schema: Optional custom PySpark schema. Overrides infer_schema if provided
            trust_server_certificate: For MSSQL, trust server certificate (default: True)
            encrypt: For MSSQL, use encryption for connection (default: False)
            
        Raises:
            RuntimeError: If SparkSession is not initialized
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

            if schema is not None:
                reader = reader.schema(schema)

            df = reader.load()

            if schema is None and not infer_schema:
                df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])

            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_schema}")

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(f"{target_schema}.{table_name}")
            )

    def load_csv(
        self,
        spark,
        sources,
        dock_cfg: DockConfig,
        delimiter="|",
        header=True,
        infer_schema=False,
        as_view=True,
    ):
        """
        Load CSV files from Docker-mounted volumes into Spark.
        
        Args:
            spark: Active SparkSession instance
            sources: List of (host_path, table_name) tuples
            dock_cfg: DockConfig containing bind_mounts mapping
            delimiter: CSV delimiter character (default: "|")
            header: Whether CSV has header row (default: True)
            infer_schema: Whether to infer schema from data (default: False)
            as_view: If True, create temp views; if False, return dict of DataFrames
            
        Returns:
            None if as_view=True, otherwise dict of {table_name: DataFrame}
            
        Raises:
            RuntimeError: If SparkSession is None
            ValueError: If host_path not found in DockConfig bind_mounts
        """
        if spark is None:
            raise RuntimeError("SparkSession is None")

        dfs = {}

        for host_path, name in sources:

            if host_path not in dock_cfg.bind_mounts:
                raise ValueError(f"Path {host_path} is not in DockConfig.bind_mounts")

            container_path = dock_cfg.bind_mounts[host_path]

            df = (
                spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .option("delimiter", delimiter)
                .csv(container_path)
            )

            if as_view:
                df.createOrReplaceTempView(name)
            else:
                dfs[name] = df

        return None if as_view else dfs
