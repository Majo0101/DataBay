from __future__ import annotations

from typing import Dict, List, Optional, Tuple

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

    def dump_data(
        self,
        queries: List[Tuple[str, str]],
        output_path: str,
        format: str = "csv",
        delimiter: str = "|",
        batch_size: int = 10000,
    ) -> None:
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
            schema: Optional custom PySpark schema. Overrides infer_schema if provided
            trust_server_certificate: For MSSQL, trust server certificate (default: True)
            encrypt: For MSSQL, use encryption for connection (default: False)
            
        Raises:
            RuntimeError: If SparkSession is not initialized
        """
        ...

    def load_csv(
        self,
        spark: SparkSession,
        sources: List[Tuple[str, str]],
        dock_cfg: DockConfig,
        delimiter: str = "|",
        header: bool = True,
        infer_schema: bool = False,
        as_view: bool = True,
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
            as_view: If True, create temp views; if False, return dict of DataFrames
            
        Returns:
            None if as_view=True, otherwise dict of {table_name: DataFrame}
            
        Raises:
            RuntimeError: If SparkSession is None
            ValueError: If host_path not found in DockConfig bind_mounts
        """
        ...
