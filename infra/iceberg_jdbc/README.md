# Spark 4.0 + Iceberg + PostgreSQL (Simple Local Setup)

Simple, working Spark environment for local data analysis with Apache Iceberg and PostgreSQL catalog. Auto-configures from just two settings: **RAM** and **CPU cores**.

## Quick Start

### 1. Set Your Resources

Edit `docker-compose.yml`:

```yaml
environment:
  - SPARK_MEMORY=28    # Java heap in GiB; leave extra RAM for overhead
  - SPARK_CORES=12     # Your available CPU cores
```

### 2. Start

```bash
docker compose up -d --build
```

### 3. Connect

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.sql("SHOW DATABASES").show()
```

**Spark UI**: http://localhost:4040

## What You Get

- **Spark 4.0** with remote connection (Spark Connect)
- **Apache Iceberg 1.10.0** for ACID tables and time travel
- **PostgreSQL 14** as catalog/metastore
- **Auto-configuration** - resource-based defaults with optional tuning overrides
- **Persistent storage** - data survives restarts

## How It Works

The container:
1. Reads `SPARK_MEMORY` and `SPARK_CORES` from docker-compose
2. Derives driver memory and parallelism, then applies optional tuning overrides
3. Generates `/opt/spark/conf/spark-defaults.conf`
4. Starts PostgreSQL + Spark Connect

Clients use Spark Connect. The server executes work with `local[N]`, where
`N = SPARK_CORES`; there are no separate executor JVMs to size.
`SPARK_MEMORY` sets the driver Java heap in GiB, not a container memory limit.
Allow additional RAM for native memory, Python workers, PostgreSQL and the OS.
The embedded PostgreSQL stores catalog metadata; the standalone test database
is a separate service and need not run alongside Spark.

## Example Usage

### Create Iceberg Table

```python
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Create a database
spark.sql("CREATE DATABASE IF NOT EXISTS demo").show()

# Create an Iceberg table with DATE column
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.sales (
        id INT,
        product STRING,
        amount DOUBLE,
        sale_date DATE
    ) USING iceberg
""").show()

# Insert data - use DATE() function for date columns
spark.sql("""
    INSERT INTO demo.sales VALUES
    (1, 'Widget A', 99.99, DATE('2026-02-22')),
    (2, 'Widget B', 149.99, DATE('2026-02-22')),
    (3, 'Widget C', 199.99, DATE('2026-02-23'))
""").show()

# Query
spark.sql("SELECT * FROM demo.sales").show()
```

### Iceberg Features

```python
# View table history
spark.sql("SELECT * FROM demo.sales.history").show(truncate=False)

# Check snapshots
spark.sql("SELECT * FROM demo.sales.snapshots").show(truncate=False)

# Time travel - query by snapshot ID
# First, get available snapshot IDs
snapshots = spark.sql("SELECT snapshot_id FROM demo.sales.snapshots ORDER BY committed_at").collect()
if len(snapshots) > 0:
    first_snapshot = snapshots[0].snapshot_id
    spark.sql(f"SELECT * FROM demo.sales VERSION AS OF {first_snapshot}").show()

# Time travel - query by timestamp
from datetime import datetime, timedelta
past_time = (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
spark.sql(f"SELECT * FROM demo.sales TIMESTAMP AS OF '{past_time}'").show()

# Create a new snapshot by adding more data
spark.sql("""
    INSERT INTO demo.sales VALUES (4, 'Widget D', 299.99, DATE('2026-02-23'))
""").show()

# Now compare snapshots to see time travel in action
spark.sql("SELECT * FROM demo.sales.history").show(truncate=False)
```

### Reading CSV Files from Windows

The repository's `data/landing` folder is mounted to `/data/apache` inside the container:

```python
# Read a CSV file from the repository data/landing folder
df = spark.read.csv("/data/apache/mydata.csv", header=True, inferSchema=True)
df.show()

# Write to Iceberg table
df.writeTo("demo.imported_data").using("iceberg").create()

# Or load directly into Iceberg table using SQL
spark.sql("""
    CREATE TABLE demo.csv_data
    USING iceberg
    AS SELECT * FROM csv.`/data/apache/mydata.csv`
""").show()

# Read multiple CSV files
df = spark.read.csv("/data/apache/*.csv", header=True, inferSchema=True)
df.show()
```

## Common Tasks

```bash
# View logs
docker compose logs -f

# Apply changed environment values in docker-compose.yml
docker compose up -d

# Apply changes to Dockerfile or start.sh
docker compose up -d --build

# Stop
docker compose down

# Remove everything including data
docker compose down -v

# Shell into container
docker compose exec spark-pg-iceberg bash

# View generated config
docker compose exec spark-pg-iceberg cat /opt/spark/conf/spark-defaults.conf
```

## Project Structure

```
├── docker-compose.yml    # Set SPARK_MEMORY and SPARK_CORES here
├── Dockerfile            # Image definition
├── start.sh              # Startup script (auto-generates config)
├── spark-defaults.conf   # Template (gets auto-generated)
└── main.ipynb            # Example notebook
```

## Volumes

Persistent and mounted data:

- `spark-lakehouse` - Iceberg table data (Docker volume)
- `spark-metastore` - PostgreSQL database (Docker volume)
- `data/landing` → `/data/apache` - Repository folder for CSV files (bind mount)

## Performance Tuning

Set heap and worker threads for the current machine in `docker-compose.yml`:

```yaml
environment:
  - SPARK_MEMORY=64    # Java heap in GiB, excluding overhead
  - SPARK_CORES=24     # Local worker threads
```

Optional environment variables let you tune individual workloads. Omitted or
empty overrides retain the existing project defaults:

| Variable | Default | Accepted values / purpose |
| --- | --- | --- |
| `SPARK_SHUFFLE_PARTITIONS` | `3 * SPARK_CORES` | Positive integer; initial SQL shuffle partitions |
| `SPARK_BROADCAST_THRESHOLD` | `104857600` (100 MiB) | Nonnegative integer bytes; `-1` disables automatic broadcast |
| `SPARK_MEMORY_FRACTION` | `0.8` | Decimal from 0 to 1; Spark execution/storage memory fraction |
| `SPARK_STORAGE_FRACTION` | `0.3` | Decimal from 0 to 1; fraction of that region protected for storage |

`SPARK_MEMORY` and `SPARK_CORES` accept positive integers. Invalid values stop
startup with the variable name in the error. AQE and partition coalescing remain
enabled. These defaults are heuristics; compare task duration, spills and memory
usage in Spark UI on representative data before changing them.

Uncomment the optional entries in Compose to override them, then run
`docker compose up -d` from this directory and reconnect your Spark session.
`docker compose restart` reuses the existing container environment and does not
apply Compose environment changes. Rebuild with `docker compose up -d --build`
after changing the startup script or Dockerfile.

## What Gets Calculated

From your SPARK_MEMORY and SPARK_CORES, the system auto-calculates:

- Driver memory allocation
- Shuffle partitions (3x cores unless overridden)
- Default parallelism (= cores)
- Max result size (15% of memory, rounded to whole GiB)

**Date/timestamp errors when inserting data?**
- Use `DATE('2026-02-22')` function for DATE columns
- Use `TIMESTAMP('2026-02-22 10:30:00')` for TIMESTAMP columns
- Don't use plain string literals like `'2026-02-22'` with DATE/TIMESTAMP columns

**Container keeps restarting?**
```bash
# Check the logs for errors
docker compose logs

# Common fixes:
# 1. Ensure Docker has enough resources allocated
# 2. Wait 60-90 seconds for full startup
# 3. Apply Compose changes: docker compose up -d
# 4. Rebuild after changing start.sh or Dockerfile: docker compose up -d --build
```

**Time travel queries failing?**
- Snapshot IDs are not sequential (1, 2, 3...)
- Query actual snapshot IDs first: `SELECT snapshot_id FROM table.snapshots`
- Or use timestamp-based queries instead

## Troubleshooting

**Port already in use?**
```bash
# Change ports in docker-compose.yml
ports:
  - "4041:4040"    # Different port
```

**Out of memory?**
- Reduce `SPARK_MEMORY` in docker-compose.yml
- Ensure Docker Desktop has enough RAM allocated

**Connection refused?**
- Wait ~60 seconds for services to start
- Check logs: `docker compose logs`

---

**That's it!** A working Spark + Iceberg environment configured from just two numbers.
