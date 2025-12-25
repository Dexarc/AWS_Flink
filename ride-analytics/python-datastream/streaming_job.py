import sys
import os
import time

# Send all output to CloudWatch
sys.stdout = sys.stderr

print("=" * 60)
print("STARTING PYFLINK MSK -> S3 TABLES JOB")
print("=" * 60)

try:
    # ------------------------------------------------------------
    # Imports
    # ------------------------------------------------------------
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.java_gateway import get_gateway

    print("PyFlink imports successful")

    # ------------------------------------------------------------
    # EARLY CLASSLOADER WORKAROUND
    # ------------------------------------------------------------
    gateway = get_gateway()
    jvm = gateway.jvm

    base_dir = os.path.dirname(os.path.abspath(__file__))
    jar_file = os.path.join(base_dir, "lib", "pyflink-dependencies.jar")

    if not os.path.exists(jar_file):
        raise RuntimeError(f"Dependency JAR not found: {jar_file}")

    jar_url = jvm.java.net.URL(f"file://{jar_file}")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)

    print(f"Injected dependency JAR: {jar_file}")

    # ------------------------------------------------------------
    # Create streaming TableEnvironment
    # ------------------------------------------------------------
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    print("TableEnvironment created (streaming mode)")

    # ------------------------------------------------------------
    # Runtime configuration
    # ------------------------------------------------------------
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")

    print("Pipeline configuration applied")

    # ------------------------------------------------------------
    # Configuration - UPDATE THESE VALUES
    # ------------------------------------------------------------
    MSK_BOOTSTRAP_SERVERS = "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
    KAFKA_TOPIC = "bid-events"
    S3_WAREHOUSE = "arn:aws:s3tables:ap-south-1:149815625933:bucket/python-saren"
    NAMESPACE = "sink"

    # ------------------------------------------------------------
    # Create Iceberg catalog
    # ------------------------------------------------------------
    print("Creating S3 Tables Iceberg catalog")

    table_env.execute_sql(f"""
        CREATE CATALOG s3_tables WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',
            'warehouse' = '{S3_WAREHOUSE}'
        )
    """)

    table_env.use_catalog("s3_tables")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    table_env.use_database(NAMESPACE)

    print(f"Using Iceberg catalog s3_tables.{NAMESPACE}")

    # ------------------------------------------------------------
    # Create Kafka source in default catalog
    # ------------------------------------------------------------
    table_env.use_catalog("default_catalog")
    table_env.use_database("default_database")

    print("Creating Kafka source table")

    table_env.execute_sql(f"""
        CREATE TABLE kafka_events (
            event_payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{MSK_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-s3tables-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'raw',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
        )
    """)

    print("Kafka source table created")

    # ------------------------------------------------------------
    # Create Iceberg sink table
    # ------------------------------------------------------------
    table_env.use_catalog("s3_tables")
    table_env.use_database(NAMESPACE)

    print("Creating Iceberg sink table")

    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS raw_events (
            json_payload STRING,
            ingestion_time TIMESTAMP(3)
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    print("Iceberg sink table created")

    # ------------------------------------------------------------
    # Submit streaming INSERT
    # ------------------------------------------------------------
    print("Submitting streaming INSERT job")

    table_env.execute_sql("""
        INSERT INTO raw_events
        SELECT 
            event_payload as json_payload,
            CURRENT_TIMESTAMP as ingestion_time
        FROM default_catalog.default_database.kafka_events
    """)

    print("Job submitted successfully")
    print("Streaming MSK -> S3 Tables")

    # ------------------------------------------------------------
    # Keep Python process alive
    # ------------------------------------------------------------
    while True:
        time.sleep(60)

except Exception:
    print("=" * 60)
    print("FATAL ERROR")
    print("=" * 60)
    import traceback
    traceback.print_exc()
    print("=" * 60)
    raise