import sys
import os
import time

# Send all output to CloudWatch
sys.stdout = sys.stderr

print("=" * 60)
print("STARTING PYFLINK S3 TABLES STREAMING JOB")
print("=" * 60)

try:
    # ------------------------------------------------------------
    # Imports
    # ------------------------------------------------------------
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.java_gateway import get_gateway

    print("PyFlink imports successful")

    # ------------------------------------------------------------
    # EARLY CLASSLOADER HACK (intentional, documented)
    #
    # On AWS Managed Flink + PyFlink + Iceberg, the planner may try
    # to resolve Iceberg/Hadoop classes before lib/*.jar is visible.
    # We explicitly inject the shaded JAR to avoid ClassNotFoundException
    # during CREATE CATALOG.
    #
    # IMPORTANT:
    # - JAR must exist at lib/pyflink-dependencies.jar in the ZIP
    # - Do NOT set pipeline.jars or pipeline.classpaths
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
    # Runtime configuration (KDA-safe)
    # ------------------------------------------------------------
    config = table_env.get_config().get_configuration()
    config.set_string("table.exec.resource.default-parallelism", "1")
    config.set_string("execution.checkpointing.interval", "30s")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")

    print("Pipeline configuration applied")

    # ------------------------------------------------------------
    # S3 Tables / Iceberg configuration
    # ------------------------------------------------------------
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
    # Create Iceberg sink table
    # ------------------------------------------------------------
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS test_events (
            id STRING,
            event_type STRING,
            metric_value DOUBLE,
            event_time BIGINT,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """)

    print("Iceberg sink table created")

    # ------------------------------------------------------------
    # Create datagen source in DEFAULT catalog
    # ------------------------------------------------------------
    table_env.use_catalog("default_catalog")
    table_env.use_database("default_database")

    table_env.execute_sql("""
        CREATE TABLE datagen_source (
            id STRING,
            event_type STRING,
            metric_value DOUBLE,
            event_time BIGINT
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.id.length' = '10',
            'fields.event_type.length' = '5',
            'fields.metric_value.min' = '0',
            'fields.metric_value.max' = '1000'
        )
    """)

    print("Datagen source created in default_catalog")

    # ------------------------------------------------------------
    # Switch back to Iceberg catalog and submit streaming INSERT
    # ------------------------------------------------------------
    table_env.use_catalog("s3_tables")
    table_env.use_database(NAMESPACE)

    print("Submitting streaming INSERT")

    table_env.execute_sql("""
        INSERT INTO test_events
        SELECT * FROM default_catalog.default_database.datagen_source
    """)

    print("Job submitted successfully")

    # ------------------------------------------------------------
    # Keep Python process alive (required on KDA)
    # ------------------------------------------------------------
    print("Job running; keeping Python process alive")

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
