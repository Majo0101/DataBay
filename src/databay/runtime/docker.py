import docker
from docker.errors import NotFound, APIError

from pyspark.sql import SparkSession

import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

from .spark import spark_connect, sparksql_magic

@dataclass
class DockConfig:
    """Configuration used when creating a Spark container.

    Attributes:
        image: Local Docker image; default "spark-pg-delta".
        name: Container name used for lookup/reuse; default "spark-pg-delta".
        ports: Container-to-host mappings; defaults to 4040/tcp and 15002/tcp.
        named_volumes: Volume-to-container-path mappings. Defaults:
            spark-lakehouse -> /lakehouse, spark-metastore -> /metastore/pgdata.
        bind_mounts: Host-to-container paths; default empty. Mounts are read/write.
        env: Container environment; default empty. For project images, set
            SPARK_MEMORY (Java heap GiB) and SPARK_CORES (local worker threads).
        extra_hosts: Hostname mappings passed to Docker. The default makes
            host.docker.internal work with Docker Engine on Linux and remains
            compatible with Docker Desktop on Windows.
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
    image: str = "spark-pg-delta"
    name: str = "spark-pg-delta"

    ports: Dict[str, int] = field(default_factory=lambda: {"4040/tcp": 4040, "15002/tcp": 15002})

    # named volumes -> container paths
    named_volumes: Dict[str, str] = field(default_factory=lambda: {"spark-lakehouse": "/lakehouse", "spark-metastore": "/metastore/pgdata"})

    # host path -> container path (Windows path OK)
    bind_mounts: Dict[str, str] = field(default_factory=dict)

    env: Dict[str, str] = field(default_factory=dict)
    extra_hosts: Dict[str, str] = field(
        default_factory=lambda: {"host.docker.internal": "host-gateway"}
    )
    network: Optional[str] = None
    restart_policy: Dict[str, Any] = field(default_factory=lambda: {"Name": "unless-stopped"})
    # Linux user/group to run container processes as (useful for WSL bind-mount writes)
    wsl_uid: Optional[int] = None
    wsl_gid: Optional[int] = None


def dock(cfg: DockConfig) -> str:
    """
    Start (or reuse) your Spark container using Docker.
    Args:
        cfg: Container image, name, mounts, environment and port mappings.

    Returns:
        Container id. A stopped existing container is started; a running one reused.

    Notes:
        Does not wait for Spark readiness or reconcile existing settings with cfg.
        Named volumes are created if absent. Changing cfg.env, ports or mounts
        requires recreating the container separately.
    """
    client = docker.from_env()

    # Ensure named volumes exist
    for vol_name in cfg.named_volumes.keys():
        try:
            client.volumes.get(vol_name)
        except NotFound:
            client.volumes.create(name=vol_name)

    # Build volume bindings
    volumes: Dict[str, Dict[str, str]] = {}
    for vol_name, container_path in cfg.named_volumes.items():
        volumes[vol_name] = {"bind": container_path, "mode": "rw"}
    for host_path, container_path in cfg.bind_mounts.items():
        volumes[host_path] = {"bind": container_path, "mode": "rw"}

    # Reuse existing container if present
    try:
        c = client.containers.get(cfg.name)
        c.reload()
        if c.status != "running":
            c.start()
        return c.id # pyright: ignore[reportReturnType]
    except NotFound:
        pass

    # Run new container
    try:
        user: Optional[str] = None
        if cfg.wsl_uid is not None:
            user = f"{cfg.wsl_uid}:{cfg.wsl_gid}" if cfg.wsl_gid is not None else str(cfg.wsl_uid)

        c = client.containers.run(
            cfg.image,
            name=cfg.name,
            detach=True,
            ports=cfg.ports,
            volumes=volumes,
            environment=cfg.env,
            extra_hosts=cfg.extra_hosts,
            network=cfg.network,
            user=user,
            restart_policy=cfg.restart_policy if cfg.restart_policy else None, # type: ignore
        ) # pyright: ignore[reportCallIssue]
        return c.id if c.id else ""
    except APIError as e:
        raise RuntimeError(f"Docker failed: {e.explanation}") from e
    
def dock_spark_init(
    cfg: DockConfig,
    app_name: str = "DataBay",
    host: str = "localhost",
    port: int = 15002,
    container_delay: float = 3.0,
    connect_timeout: int = 8,
    check_interval: float = 0.2,
    reattachable_execute: bool = False,
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
        reattachable_execute: Enable Spark Connect execution reattachment.
    
    Returns:
        SparkSession connected with %%sparksql registered.

    Notes:
        Run in an active IPython/Jupyter session. Stops an existing active Spark
        session before connecting. Uses dock() reuse semantics; it does not apply
        new settings to an existing container or register %%ts / %%skip.
    """
    # Step 1: Start container
    container_id = dock(cfg)
    print(f"Container started: {container_id[:12]}")
    
    # Step 2: Container delay
    time.sleep(container_delay)
    
    # Step 3: Stop any existing timed-out Spark session
    try:
        existing_spark = SparkSession.getActiveSession()
        if existing_spark:
            print("Stopping existing Spark session...")
            existing_spark.stop()
    except Exception:
        pass
    
    # Step 4: Connect to Spark with delay/retry
    spark = spark_connect(
        app_name=app_name,
        host=host,
        port=port,
        timeout=connect_timeout,
        check_interval=check_interval,
        reattachable_execute=reattachable_execute,
    )
    
    # Step 5: Register cell magics
    sparksql_magic(spark)
    print(f"Spark {spark.version} | Cell magics registered: %%ts, %%skip, %%sparksql")
    
    return spark


def dock_shutdown(cfg: DockConfig, remove: bool = False) -> None:
    """
    Stop (and optionally remove) a Docker container.
    
    Args:
        cfg: DockConfig containing the container name to shut down
        remove: If True, removes the container after stopping (default: False)
    
    Returns:
        None. Silently returns if container doesn't exist.

    Notes:
        Named volumes and bind-mounted files are retained. remove=True removes
        the container; API errors during removal are suppressed.
    """
    client = docker.from_env()

    try:
        c = client.containers.get(cfg.name)
    except NotFound:
        return

    if c.status == "running":
        c.stop()

    if remove:
        try:
            c.remove()
        except APIError:
            pass
