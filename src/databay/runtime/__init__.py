"""
Runtime utilities for Docker and Spark connectivity
"""

from databay.runtime.docker import (
    DockConfig,
    dock,
    dock_spark_init,
    dock_shutdown,
)

from databay.runtime.spark import (
    is_port_open,
    spark_connect,
    sparksql_magic,
)

__all__ = [
    # Docker
    "DockConfig",
    "dock",
    "dock_spark_init",
    "dock_shutdown",
    # Spark
    "is_port_open",
    "spark_connect",
    "sparksql_magic",
]
