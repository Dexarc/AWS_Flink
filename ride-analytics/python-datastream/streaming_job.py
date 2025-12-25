import sys
import os
import time

# Send logs to CloudWatch
sys.stdout = sys.stderr

print("=" * 60)
print("STARTING PYFLINK MSK -> S3 TABLES JOB (JSON FORMAT)")
print("=" * 60)

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.java_gateway import get_gateway

    print("PyFlink imports successful")

    # Classloader workaround
    gateway = get_gateway()
    jvm = gateway.jvm
    base_dir = os.path.dirname(os.path.abspath(__file__))
    jar_file = os.path.join(base_dir, "lib", "pyflink-dependencies.jar")

    if not os.path.exists(jar_file):
        raise RuntimeError(f"Dependency JAR not found: {jar_file}")

    jar_url = jvm.java.net.URL(f"file://{jar_file}")
    jvm.Thread.currentThread().getContextClassLoader().addURL(jar_url)
    print(f"Injected dependency JAR: {jar_file}")

    # TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    print("TableEnvironment created")

    # Runtime configuration
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "60s")
    config.set_string("execution.checkpointing.timeout", "2min")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    
    # ADD DEBUG LOGGING
    config.set_string("taskmanager.log.level", "DEBUG")
    print("Pipeline configuration applied")

    # Settings
    MSK_BOOTSTRAP_SERVERS = (
        "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
    )
    KAFKA_TOPIC = "bid-events"
    S3_WAREHOUSE = "arn:aws:s3tables:ap-south-1:149815625933:bucket/python-saren"
    NAMESPACE = "sink"

    # Iceberg catalog
    print("Creating Iceberg catalog")
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
    print("Iceberg catalog ready")

    # Kafka source
    table_env.use_catalog("default_catalog")
    table_env.use_database("default_database")
    print("Creating Kafka source with JSON format")

    table_env.execute_sql(f"""
        CREATE TABLE kafka_events (
            event_name STRING,
            user_id BIGINT,
            city_id INT,
            platform STRING,
            session_id STRING,
            timestamp_ms BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{MSK_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-s3tables-json-v1',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'AWS_MSK_IAM',
            'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
            'properties.sasl.client.callback.handler.class' = 
                'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
        )
    """)
    print("✓ Kafka source created")

    # Iceberg sink
    table_env.use_catalog("s3_tables")
    table_env.use_database(NAMESPACE)
    print("Creating Iceberg sink table")

    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS parsed_events (
            event_name STRING,
            user_id BIGINT,
            city_id INT,
            platform STRING,
            session_id STRING,
            event_time TIMESTAMP(3),
            ingestion_time TIMESTAMP(3)
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print("✓ Iceberg sink created")

    # Streaming INSERT
    print("Submitting streaming INSERT with JSON parsing")
    
    table_env.execute_sql("""
        INSERT INTO parsed_events
        SELECT
            event_name,
            user_id,
            city_id,
            platform,
            session_id,
            TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time,
            CURRENT_TIMESTAMP AS ingestion_time
        FROM default_catalog.default_database.kafka_events
    """)

    print("✓ Job submitted successfully (JSON format)")
    print("=" * 60)

    # Keep alive
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