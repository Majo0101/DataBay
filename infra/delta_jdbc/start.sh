#!/bin/bash
set -e

echo "=========================================="
echo "  Starting Spark + PostgreSQL + Delta Lake"
echo "=========================================="

# Default values if not set
SPARK_MEMORY=${SPARK_MEMORY:-28}
SPARK_CORES=${SPARK_CORES:-12}

# Validate values before using them in arithmetic or generated configuration.
for name in SPARK_MEMORY SPARK_CORES; do
    if [[ ! ${!name} =~ ^[1-9][0-9]*$ ]]; then
        echo "Invalid $name: expected a positive integer" >&2
        exit 1
    fi
done

SPARK_SHUFFLE_PARTITIONS=${SPARK_SHUFFLE_PARTITIONS:-$(awk -v cores="$SPARK_CORES" 'BEGIN {printf "%.0f", cores * 3}')}
SPARK_BROADCAST_THRESHOLD=${SPARK_BROADCAST_THRESHOLD:-104857600}
SPARK_MEMORY_FRACTION=${SPARK_MEMORY_FRACTION:-0.8}
SPARK_STORAGE_FRACTION=${SPARK_STORAGE_FRACTION:-0.3}

if [[ ! $SPARK_SHUFFLE_PARTITIONS =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid SPARK_SHUFFLE_PARTITIONS: expected a positive integer" >&2
    exit 1
fi
if [[ ! $SPARK_BROADCAST_THRESHOLD =~ ^(-1|0|[1-9][0-9]*)$ ]]; then
    echo "Invalid SPARK_BROADCAST_THRESHOLD: expected bytes (nonnegative integer) or -1" >&2
    exit 1
fi
for name in SPARK_MEMORY_FRACTION SPARK_STORAGE_FRACTION; do
    if [[ ! ${!name} =~ ^(0(\.[0-9]+)?|1(\.0+)?)$ ]]; then
        echo "Invalid $name: expected a number between 0 and 1" >&2
        exit 1
    fi
done

echo "Configuration: ${SPARK_MEMORY}GiB Java heap, ${SPARK_CORES} local worker threads"

# Ensure conf directory exists and has permissions
mkdir -p /opt/spark/conf
chown -R spark:spark /opt/spark/conf

# Derive local driver settings; tuning defaults can be overridden above.
DRIVER_MEMORY="${SPARK_MEMORY}g"
DRIVER_MAX_RESULT=$(awk "BEGIN {printf \"%.0f\", $SPARK_MEMORY * 0.15}")g

# Generate Spark configuration for Delta Lake
cat > /opt/spark/conf/spark-defaults.conf << EOF
# Auto-generated from: ${SPARK_MEMORY}GiB Java heap, ${SPARK_CORES} local worker threads

spark.app.name PowerRig
spark.master local[$SPARK_CORES]

# Remote connection
spark.connect.grpc.binding.port 15002
spark.driver.bindAddress 0.0.0.0
spark.driver.host 0.0.0.0

# Memory & execution
spark.driver.memory $DRIVER_MEMORY
spark.driver.maxResultSize $DRIVER_MAX_RESULT
spark.sql.shuffle.partitions $SPARK_SHUFFLE_PARTITIONS
spark.default.parallelism $SPARK_CORES

# Performance
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.execution.arrow.pyspark.enabled true
spark.sql.files.maxPartitionBytes 268435456
spark.sql.autoBroadcastJoinThreshold $SPARK_BROADCAST_THRESHOLD
spark.memory.fraction $SPARK_MEMORY_FRACTION
spark.memory.storageFraction $SPARK_STORAGE_FRACTION
spark.serializer org.apache.spark.serializer.KryoSerializer

# Delta Lake extensions
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.schema.autoMerge.enabled true
spark.sql.legacy.createHiveTableByDefault false

# Warehouse location
spark.sql.warehouse.dir /lakehouse/warehouse

# PostgreSQL-backed Hive Metastore
spark.sql.catalogImplementation hive
spark.hadoop.javax.jdo.option.ConnectionURL jdbc:postgresql://localhost:5432/hive_metastore
spark.hadoop.javax.jdo.option.ConnectionDriverName org.postgresql.Driver
spark.hadoop.javax.jdo.option.ConnectionUserName hive
spark.hadoop.javax.jdo.option.ConnectionPassword hive

# DataNucleus Configuration
spark.hadoop.datanucleus.autoCreateSchema true
spark.hadoop.datanucleus.fixedDatastore false
spark.hadoop.datanucleus.autoStartMechanismMode ignored
spark.hadoop.datanucleus.schema.autoCreateTables true
spark.hadoop.datanucleus.schema.autoCreateColumns true
spark.hadoop.datanucleus.schema.validateTables false
spark.hadoop.datanucleus.schema.validateConstraints false

# Hive Metastore Configuration
spark.hadoop.hive.metastore.schema.verification false
spark.hadoop.hive.metastore.warehouse.dir /lakehouse/warehouse

# Logging
spark.ui.showConsoleProgress false
spark.driver.extraJavaOptions -Dlog4j2.formatMsgNoLookups=true
EOF

echo "Configuration generated"
echo "=========================================="

# Start PostgreSQL
echo "Starting PostgreSQL..."
pg_ctlcluster 14 main start
until pg_isready -U postgres -t 1; do sleep 1; done
echo "PostgreSQL ready"

# Start Spark Connect Server
echo "Starting Spark Connect Server..."
su - spark -c "
    export JAVA_HOME=/opt/java/openjdk
    export SPARK_HOME=/opt/spark
    export SPARK_LOG_DIR=/opt/spark/logs
    export SPARK_PID_DIR=/opt/spark/run
    export SPARK_CONF_DIR=/opt/spark/conf
    export SPARK_NO_DAEMONIZE=true
    /opt/spark/sbin/start-connect-server.sh
" &

SPARK_PID=$!

echo "=========================================="
echo "  Ready!"
echo "  Spark UI:      http://localhost:4040"
echo "  Spark Connect: sc://localhost:15002"
echo "=========================================="

# Graceful shutdown
trap "echo 'Shutting down...'; kill $SPARK_PID 2>/dev/null || true; pg_ctlcluster 14 main stop; exit 0" SIGTERM SIGINT

wait $SPARK_PID
pg_ctlcluster 14 main stop
