from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

@dataclass
class DockConfig:
    """
    Docker container configuration for Spark runtime.
    
    Attributes:
        image: Docker image to use (e.g., "apache/spark:3.5.0")
        name: Container name for identification and reuse
        ports: Port mappings from container to host {container_port: host_port}
        named_volumes: Docker named volumes to attach {volume_name: mount_path}
        bind_mounts: Host directories to mount {host_path: container_path}
        env: Environment variables to set in container {var_name: value}
        network: Docker network name to attach container to (None for default)
        restart_policy: Container restart policy (e.g., {"Name": "unless-stopped"})
        wsl_uid: Linux UID to run container process as for WSL write permissions
        wsl_gid: Linux GID to run container process as for WSL write permissions
    
    Example:
        >>> config = DockConfig(
        ...     image="apache/spark:3.5.0",
        ...     name="databay-spark",
        ...     ports={"15002/tcp": 15002},
        ...     named_volumes={"spark-data": "/opt/spark/data"},
        ...     bind_mounts={"/local/path": "/container/path"},
        ...     env={"SPARK_MODE": "master"},
        ...     network="spark-network",
        ...     restart_policy={"Name": "unless-stopped"},
        ...     wsl_uid=1000,
        ...     wsl_gid=1000,
        ... )
    """
    image: str = ...
    name: str = ...
    ports: Dict[str, int] = ...
    named_volumes: Dict[str, str] = ...
    bind_mounts: Dict[str, str] = ...
    env: Dict[str, str] = ...
    network: Optional[str] = ...
    restart_policy: Dict[str, Any] = ...
    wsl_uid: Optional[int] = ...
    wsl_gid: Optional[int] = ...


def dock(cfg: DockConfig) -> str:
    """
    Start (or reuse) your Spark container using Docker.
    Returns container id.
    """
    ...

def dock_spark_init(
    cfg: DockConfig,
    app_name: str = "DataBay",
    host: str = "localhost",
    port: int = 15002,
    container_delay: float = 3.0,
    connect_timeout: int = 8,
    check_interval: float = 0.2,
) -> SparkSession:
    """
    Complete initialization: start container, wait, connect to Spark, and register cell magics.
    
    Args:
        cfg: DockConfig for container setup
        app_name: Name for the Spark application (default: "DataBay")
        host: Hostname for Spark Connect (default: "localhost")
        port: Port for Spark Connect (default: 15002)
        container_delay: Seconds to wait after starting container (default: 3.0)
        connect_timeout: Max seconds to wait for Spark connection (default: 8)
        check_interval: Seconds between connection attempts (default: 0.2)
    
    Returns:
        SparkSession connected and ready with cell magics registered
    """
    ...

def dock_connect(
    app_name: str = "DataBay",
    host: str = "localhost",
    port: int = 15002,
    timeout: int = 8,
    check_interval: float = 0.2,
) -> SparkSession:
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
    ...

def dock_shutdown(cfg: DockConfig, remove: bool = False) -> None:
    """
    Stop (and optionally remove) a Docker container.
    
    Args:
        cfg: DockConfig containing the container name to shut down
        remove: If True, removes the container after stopping (default: False)
    
    Returns:
        None. Silently returns if container doesn't exist.
    """
    ...
