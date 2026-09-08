from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from databay.runtime.docker import DockConfig


class Octopus:
    """
    Data integration utility for Spark operations with database connectivity.
    Handles JDBC connections, data extraction, and CSV loading.
    """
    
    def __init__(
        self,
        env_file: Optional[str] = None,
        spark: Optional[SparkSession] = None,
        engine: Optional[str] = None,
    ) -> None:
        """
        Initialize Octopus with optional environment file and SparkSession.
        
        Args:
            env_file: Path to .env file containing database credentials
            spark: Active SparkSession instance (optional)
            engine: Database engine type (postgresql, mssql, oracle) (optional)
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
    ) -> None:
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
            schema: Optional StructType of JDBC read type overrides (full or partial).
                    Names must exactly match query result columns; unspecified columns keep
                    JDBC-inferred types. Takes precedence over infer_schema. Conversions
                    depend on JDBC driver support; nullability/metadata are not enforced.
            trust_server_certificate: For MSSQL, trust server certificate (default: True)
            encrypt: For MSSQL, use encryption for connection (default: False)
            
        Raises:
            RuntimeError: If SparkSession is not initialized
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
    ) -> Dict[str, DataFrame]:
        """
        Read data from database via JDBC into Spark DataFrames (without writing tables).
        
        Args:
            queries: List of (query, table_name) tuples to execute and collect
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
    ) -> None:
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
        ...

    def load_csv(
        self,
        spark: Optional[SparkSession],
        sources: List[Tuple[str, str]],
        dock_cfg: DockConfig,
        delimiter: str = "|",
        header: bool = True,
        infer_schema: bool = False,
        mode: str = "both",
        csv_read_mode: str = "PERMISSIVE",
    ) -> Optional[Dict[str, DataFrame]]:
        """
        Load CSV files from Docker-mounted volumes into Spark.
        
        Args:
            spark: Active SparkSession instance
            sources: List of (host_path, table_name) tuples
            dock_cfg: DockConfig containing bind_mounts mapping
            delimiter: CSV delimiter character (default: "|")
            header: Whether CSV has header row (default: True)
            infer_schema: Whether to infer schema from data (default: False)
            mode: Output mode - "view", "dfs", or "both" (default: "both")
            csv_read_mode: Spark CSV parser mode - "PERMISSIVE", "DROPMALFORMED", or "FAILFAST"
            
        Returns:
            None or dict of {table_name: DataFrame}
            
        Raises:
            RuntimeError: If SparkSession is None
            ValueError: If host_path not found in DockConfig bind_mounts
        """
        ...
