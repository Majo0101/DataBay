# Spark 4.0 + Delta Lake + PostgreSQL (Simple Local Setup)

Simple, working Spark environment for local data analysis with Delta Lake and PostgreSQL-backed Hive metastore. Auto-configures from just two settings: **RAM** and **CPU cores**.

## Quick Start

### 1. Set Your Resources

Edit `docker-compose.yml`:

```yaml
environment:
  - SPARK_MEMORY=28    # Your available RAM in GB
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
- **Delta Lake 4.0** for ACID transactions, time travel, and data versioning
- **PostgreSQL 14** as Hive metastore backend
- **Auto-configuration** - all Spark settings calculated from your RAM/cores
- **Persistent storage** - data survives restarts

## How It Works

The container:
1. Reads `SPARK_MEMORY` and `SPARK_CORES` from docker-compose
2. Calculates optimal Spark settings (memory, partitions, etc.)
3. Generates `/opt/spark/conf/spark-defaults.conf` with Delta Lake config
4. Starts PostgreSQL + Spark Connect

## Example Usage

### Create Delta Lake Table

```python
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Create a database
spark.sql("CREATE DATABASE IF NOT EXISTS demo").show()

# Create a Delta Lake table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.sales (
        id INT,
        product STRING,
        amount DOUBLE,
        sale_date DATE
    ) USING DELTA
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

### Delta Lake ACID Operations

```python
# UPDATE (transactional)
spark.sql("""
    UPDATE demo.sales 
    SET amount = amount * 0.9 
    WHERE product LIKE 'Widget%'
""").show()

# DELETE (transactional)
spark.sql("""
    DELETE FROM demo.sales 
    WHERE amount < 100
""").show()

# MERGE (upsert)
spark.sql("""
    MERGE INTO demo.sales AS target
    USING updates AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""").show()
```

### Delta Lake Time Travel

```python
# View table history
spark.sql("DESCRIBE HISTORY demo.sales").show(truncate=False)

# View table details
spark.sql("DESCRIBE DETAIL demo.sales").show(truncate=False)

# Time travel by version number (versions start at 0)
spark.sql("SELECT * FROM demo.sales VERSION AS OF 0").show()
```

### Delta Lake Optimization

```python
# Optimize table (compaction)
spark.sql("OPTIMIZE demo.sales").show()

# Z-order for better query performance
spark.sql("OPTIMIZE demo.sales ZORDER BY (product)").show()

# Vacuum old files (DRY RUN for testing - shows what would be deleted)
# Disable retention check for demo purposes
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM demo.sales RETAIN 0 HOURS DRY RUN").show(truncate=False)

# Production-safe vacuum (at least 168 hours retention recommended)
# spark.sql("VACUUM demo.sales RETAIN 168 HOURS").show()
```

### Reading CSV Files from Windows

The repository's `data/landing` folder is mounted to `/data/apache` inside the container:

```python
# Read a CSV file from the repository data/landing folder
df = spark.read.csv("/data/apache/mydata.csv", header=True, inferSchema=True)
df.show()

# Write to Delta Lake table
df.write.format("delta").mode("overwrite").saveAsTable("demo.imported_data")

# Or append to existing table
df.write.format("delta").mode("append").saveAsTable("demo.imported_data")

# Load directly into Delta table using SQL
spark.sql("""
    CREATE TABLE demo.csv_data
    USING DELTA
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

# Restart with new settings
docker compose restart

# Stop
docker compose down

# Remove everything including data
docker compose down -v

# Shell into container
docker compose exec spark-pg-delta bash

# View generated config
docker compose exec spark-pg-delta cat /opt/spark/conf/spark-defaults.conf
```

## Project Structure

```
├── docker-compose.yml    # Set SPARK_MEMORY and SPARK_CORES here
├── Dockerfile            # Image definition
├── start.sh              # Startup script (auto-generates config)
├── spark-defaults.conf   # Template (gets auto-generated)
└── main.ipynb            # Working examples with Delta Lake features
```

## Volumes

Persistent and mounted data:

- `spark-lakehouse` - Delta Lake table data (Docker volume)
- `spark-metastore` - PostgreSQL Hive metastore database (Docker volume)
- `data/landing` → `/data/apache` - Repository folder for CSV files (bind mount)

## Performance Tuning

Just adjust these two values in docker-compose.yml:

```yaml
environment:
  - SPARK_MEMORY=64    # More memory = larger datasets
  - SPARK_CORES=24     # More cores = more parallelism
```

Everything else is calculated automatically.

## What Gets Calculated

From your SPARK_MEMORY and SPARK_CORES, the system auto-calculates:

- Driver memory allocation
- Executor memory allocation  
- Shuffle partitions (3x cores)
- Default parallelism (= cores)
- Max result size (15% of memory)
- Delta Lake optimizations

## Key Differences: Delta Lake vs Iceberg

| Feature | Delta Lake | Iceberg |
|---------|-----------|---------|
| **Catalog** | Hive Metastore (PostgreSQL) | JDBC Catalog (PostgreSQL) |
| **Time Travel** | VERSION AS OF / TIMESTAMP AS OF | VERSION AS OF / TIMESTAMP AS OF |
| **ACID Ops** | UPDATE, DELETE, MERGE | UPDATE, DELETE, MERGE |
| **Optimization** | OPTIMIZE, ZORDER | Table maintenance via snapshots |
| **Metadata Access** | DESCRIBE HISTORY | .history, .snapshots tables |
| **Vacuum** | VACUUM command | Expire snapshots |

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
- Check logs: `docker compose logs -f`

**Date/timestamp errors when inserting data?**
- Use `DATE('2026-02-22')` function for DATE columns
- Use `TIMESTAMP('2026-02-22 10:30:00')` for TIMESTAMP columns
- Don't use plain string literals like `'2026-02-22'` with DATE/TIMESTAMP columns

**Hive metastore errors?**
- Ensure PostgreSQL is fully started (check logs)
- Metastore schema is auto-created on first table creation
- If issues persist, try: `docker compose down -v && docker compose up -d --build`

**Delta Lake version conflicts?**
- This uses Delta Lake 4.0 for Spark 4.0
- Ensure you're not mixing different Delta Lake versions

---

**That's it!** A working Spark + Delta Lake environment configured from just two numbers.
