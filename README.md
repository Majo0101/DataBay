# DataBay

**Data quality and comparison tooling for lakehouse datasets**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-4.0+-orange.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

DataBay is a Python library designed for data engineers working with Apache Spark and lakehouse architectures. It provides a comprehensive suite of tools for data quality analysis, dataset comparison, ETL operations, and Spark runtime management.


## Installation

```bash
pip install -e .
```

### Requirements
- Python 3.10+
- Apache Spark 4.0+ (via PySpark)
- Docker (for container runtime features)

## Quick Start

### 1. Initialize Spark with Docker

```python
from databay.runtime.docker import DockConfig, dock_spark_init

# Configure Spark container
config = DockConfig(
    image="spark-delta-pg",
    name="databay-spark",
    ports={"4040/tcp": 4040, "15002/tcp": 15002},
    named_volumes={
        "spark-lakehouse": "/lakehouse",
        "spark-metastore": "/metastore/pgdata"
    }
)

# Start container and get SparkSession
spark = dock_spark_init(config)
```

### 2. Data Quality Analysis

```python
from databay import null_rate, duplicate_check, compare_datasets

# Check null rates across columns
null_analysis = null_rate(df, threshold=5.0)  # Only show columns with >5% nulls
null_analysis.show()

# Find duplicate records
duplicates = duplicate_check(df, subset=["id", "customer_name"])
duplicates.show()

# Compare two datasets
comparison = compare_datasets(
    df_prod, df_test,
    cols=["id", "name", "value"],
    name_a="Production",
    name_b="Test"
)
comparison.show()
```

### 3. Column-by-Column Comparison

```python
from databay import compare_columns_by_key

# Detailed comparison by key
diff_analysis = compare_columns_by_key(
    df_a=prod_data,
    df_b=test_data,
    key_cols=["customer_id"],
    compare_cols=["name", "email", "balance"],
    show_summary_only=False
)
diff_analysis.show()
```

### 4. ETL Operations with Octopus

```python
from databay.etl import Octopus

# Initialize with database credentials
octopus = Octopus(env_file=".env", spark=spark, engine="postgresql")

# Extract data from database
queries = [
    ("SELECT * FROM customers", "customers"),
    ("SELECT * FROM orders", "orders")
]
octopus.feed_spark(
    queries=queries,
    target_schema="analytics",
    batch_size=10000,
    num_partitions=4
)

# Now query in Spark
spark.sql("SELECT * FROM analytics.customers").show()
```

### 5. Jupyter Magic Commands

```python
# Register magics (done automatically by dock_spark_init)
from databay.runtime.spark import sparksql_magic
sparksql_magic(spark)
```

```sql
%%sparksql
SELECT customer_id, COUNT(*) as order_count
FROM analytics.orders
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 10
```

```python
# Save query results to variable
%%sparksql top_customers
SELECT * FROM analytics.customers WHERE tier = 'platinum'
```

### 6. Visualizations

```python
from databay.reporting.plot import plot_match_percentage

# Create interactive quality chart
fig = plot_match_percentage(
    diff_analysis,
    title='Column Match: PROD vs TEST',
    column_col='column',
    match_col='match_%'
)
fig.show()
```

## Module Overview

### `databay.core`

#### **metrics.py**
- `compare_datasets()` - Full dataset comparison with similarity metrics
- `compare_columns_by_key()` - Key-based column comparison
- `compare_schema()` - Schema structure comparison
- `numeric_diff_check()` - Numeric value deviation analysis

#### **quality.py**
- `null_rate()` - Missing value statistics
- `pk_uniqueness_check()` - Primary key validation
- `duplicate_check()` - Duplicate record detection

#### **rules.py**
- Custom validation rule framework
- Assertion-based data quality checks

### `databay.etl`

#### **octopus.py**
- `Octopus` class - Multi-database ETL orchestrator
  - `.read_jdbc()` - Read JDBC results into DataFrames
  - `.feed_spark()` - Load to Spark Delta tables
  - `.write_jdbc()` - Write DataFrames to JDBC tables
  - `.load_csv()` - Import CSV from volumes

### `databay.runtime`

#### **docker.py**
- `DockConfig` - Container configuration dataclass
- `dock()` - Start/reuse Spark containers
- `dock_spark_init()` - Complete initialization workflow
- `dock_shutdown()` - Graceful container shutdown

#### **spark.py**
- `spark_connect()` - Spark Connect protocol connection
- `sparksql_magic()` - Register Jupyter cell magics
- `is_port_open()` - Network port availability check

### `databay.reporting`

#### **plot.py**
- `plot_match_percentage()` - Quality bar charts
- Data quality color scales (red → yellow → green)

#### **tables.py**
- HTML table generation
- Formatted output for notebooks

## Configuration

### Docker Container Setup

DataBay uses Docker to manage Spark containers. The `DockConfig` dataclass provides flexible configuration:

```python
from databay.runtime.docker import DockConfig

config = DockConfig(
    image="spark-delta-pg",           # Docker image name
    name="my-spark",                  # Container name
    ports={
        "4040/tcp": 4040,             # Spark UI
        "15002/tcp": 15002            # Spark Connect
    },
    named_volumes={
        "spark-data": "/data"         # Persistent data
    },
    bind_mounts={
        r"C:\data": "/host/data"      # Mount local directories
    },
    env={
        "SPARK_MODE": "master"        # Environment variables
    },
    network="spark-net",               # Docker network
    restart_policy={
        "Name": "unless-stopped"      # Auto-restart policy
    }
)
```

### Database Credentials (.env)

For Octopus ETL operations, store credentials in a `.env` file:

```env
# PostgreSQL
USER=myuser
PASSWORD=mypassword
HOST=localhost
PORT=5432
DATABASE=analytics

# MSSQL with interactive auth
USER=user@domain.com
TENANT_ID=your-tenant-id
```



## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

**Marian Bodnar**  
Email: bodnar.marian@gmail.com

## Acknowledgments

- Built on Apache Spark and PySpark
- Docker integration via `docker-py`
- Visualizations powered by Plotly
- Inspired by modern lakehouse architectures (Delta Lake, Iceberg)

---

**DataBay** - Making data quality analysis simple and accessible for data engineers.
