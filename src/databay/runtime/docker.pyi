from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

@dataclass
class DockConfig:
    """Configuration used when creating a Spark container.

    Attributes:
        image: Local Docker image; default "spark-delta-pg".
        name: Container name used for lookup/reuse; default "spark-delta-pg".
        ports: Container-to-host mappings; defaults to 4040/tcp and 15002/tcp.
        named_volumes: Volume-to-container-path mappings. Defaults:
            spark-lakehouse -> /lakehouse, spark-metastore -> /metastore/pgdata.
        bind_mounts: Host-to-container paths; default empty. Mounts are read/write.
        env: Container environment; default empty. For project images, set
            SPARK_MEMORY (Java heap GiB) and SPARK_CORES (local worker threads).
        network: Docker network name, or None for Docker's default.
        restart_policy: Defaults to {"Name": "unless-stopped"}.
        wsl_uid: Optional Linux process UID; image must support running as this user.
        wsl_gid: Optional GID; used only when wsl_uid is set.

    Existing containers are reused by name; changed settings are not applied
    automatically. Match names and volume mappings to your Compose configuration
    when reusing its container.

    Example:
        >>> cfg = DockConfig(image="spark-pg-delta", name="spark-pg-delta")
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
    """Start (or reuse) your Spark container using Docker.
    Args:
        cfg: Container image, name, mounts, environment and port mappings.

    Returns:
        Container id. A stopped existing container is started; a running one reused.

    Notes:
        Does not wait for Spark readiness or reconcile existing settings with cfg.
        Named volumes are created if absent. Changing cfg.env, ports or mounts
        requires recreating the container separately.
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
    """Complete initialization: start container, wait, connect to Spark, and register cell magics.

    Args:
        cfg: DockConfig for container setup
        app_name: Name for the Spark application (default: "DataBay")
        host: Hostname for Spark Connect (default: "localhost")
        port: Port for Spark Connect (default: 15002)
        container_delay: Seconds to wait after starting container (default: 3.0)
        connect_timeout: Max seconds to wait for Spark connection (default: 8)
        check_interval: Seconds between connection attempts (default: 0.2)

    Returns:
        SparkSession connected with %%sparksql registered.

    Notes:
        Run in an active IPython/Jupyter session. Stops an existing active Spark
        session before connecting. Uses dock() reuse semantics; it does not apply
        new settings to an existing container or register %%ts / %%skip.
    """
    ...

def dock_shutdown(cfg: DockConfig, remove: bool = False) -> None:
    """Stop (and optionally remove) a Docker container.

    Args:
        cfg: DockConfig containing the container name to shut down
        remove: If True, removes the container after stopping (default: False)

    Returns:
        None. Silently returns if container doesn't exist.

    Notes:
        Named volumes and bind-mounted files are retained. remove=True removes
        the container; API errors during removal are suppressed.
    """
    ...
