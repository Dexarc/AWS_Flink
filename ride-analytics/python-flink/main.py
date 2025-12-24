"""
MSK to Iceberg (Glue Catalog) - PyFlink Table API
Fully supported version for Amazon Managed Service for Apache Flink
"""

import os
import sys
import json
from pyflink.table import EnvironmentSettings, TableEnvironment

# Local properties file path (only used when running locally)
LOCAL_PROPERTIES_FILE = "application_properties.json"

# Check if running locally
is_local = os.environ.get("IS_LOCAL") is not None

def get_application_properties():
    """Load properties from local JSON file (local development only)"""
    if os.path.isfile(LOCAL_PROPERTIES_FILE):
        with open(LOCAL_PROPERTIES_FILE, "r") as f:
            return json.load(f)
    else:
        print(f"Local properties file '{LOCAL_PROPERTIES_FILE}' not found")
        return None

def property_map(props, group_id):
    """Extract PropertyMap from a group"""
    if props is None:
        return None
    for group in props:
        if group["PropertyGroupId"] == group_id:
            return group["PropertyMap"]
    return None

def main():
    try:
        print("=" * 80)
        print("Starting PyFlink Application - MSK → Iceberg (Glue Catalog)")
        print("=" * 80)

        # Step 1: Create Table Environment
        print("\n[Step 1] Creating Table Environment...")
        env_settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(env_settings)

        # Checkpointing configuration
        t_env.get_config().get_configuration().set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
        t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "1 min")
        t_env.get_config().get_configuration().set_string("execution.checkpointing.min-pause", "30s")
        t_env.get_config().get_configuration().set_string("execution.checkpointing.timeout", "10 min")

        print("✓ Table Environment created")

        # Step 2: Local setup (JAR loading)
        if is_local:
            print("\n[Step 2] Configuring LOCAL development mode...")
            current_dir = os.path.dirname(os.path.realpath(__file__))
            jar_path = f"file:///{current_dir}/target/pyflink-dependencies.jar"
            t_env.get_config().get_configuration().set_string("pipeline.jars", jar_path)
            print(f"✓ Local JAR loaded: {jar_path}")
        else:
            print("\n[Step 2] Running on AWS Managed Service for Apache Flink")

        # Step 3: Load configuration
        print("\n[Step 3] Loading configuration...")
        props = get_application_properties() if is_local else None

        if is_local and props:
            print("✓ Properties loaded from local application_properties.json")
        elif not is_local:
            print("✓ Properties injected via Managed Flink runtime properties")
        else:
            print("⚠ Using hardcoded defaults")

        # Kafka configuration
        kafka_props = property_map(props, "KafkaSource0") if is_local else None
        if kafka_props:
            bootstrap_servers = kafka_props["bootstrap.servers"]
            topic_name = kafka_props["topic.name"]
            group_id = kafka_props["group.id"]
        else:
            bootstrap_servers = "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
            topic_name = "user_events"
            group_id = "flink-s3-tables-pyflink"

        print(f"  Kafka Topic: {topic_name}")
        print(f"  Consumer Group: {group_id}")

        # Iceberg (Glue Catalog) configuration
        iceberg_props = property_map(props, "IcebergConfig0") if is_local else None
        if iceberg_props:
            catalog_name = iceberg_props["catalog.name"]
            warehouse_path = iceberg_props["warehouse.path"]
            database_name = iceberg_props["database.name"]
            table_name = iceberg_props["table.name"]
            aws_region = iceberg_props["aws.region"]
        else:
            catalog_name = "glue_catalog"
            warehouse_path = "s3://testing-poc-flink-table-bucket/warehouse/"
            database_name = "pocflink"
            table_name = "ride_events"
            aws_region = "ap-south-1"

        print(f"  Catalog: {catalog_name}")
        print(f"  Warehouse: {warehouse_path}")
        print(f"  Database: {database_name}")
        print(f"  Table: {table_name}")
        print(f"  Region: {aws_region}")

        # Step 4: Create Kafka source
        print("\n[Step 4] Creating Kafka source table...")
        kafka_source_table = "kafka_source"

        kafka_ddl = f"""
            CREATE TABLE {kafka_source_table} (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                `timestamp` BIGINT,
                ride_id STRING,
                metadata ROW<
                    surge_multiplier DOUBLE,
                    estimated_wait_minutes INT,
                    fare_amount DOUBLE,
                    driver_rating DOUBLE
                >,
                proctime AS PROCTIME()
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic_name}',
                'properties.bootstrap.servers' = '{bootstrap_servers}',
                'properties.group.id' = '{group_id}',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'AWS_MSK_IAM',
                'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
                'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
            )
        """
        t_env.execute_sql(kafka_ddl)
        print(f"✓ Kafka source created: {kafka_source_table}")

        # Step 5: Create Glue Catalog
        print("\n[Step 5] Creating Glue Catalog...")
        catalog_ddl = f"""
            CREATE CATALOG {catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
                'warehouse' = '{warehouse_path}',
                'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                'aws.region' = '{aws_region}'
            )
        """
        t_env.execute_sql(catalog_ddl)
        print(f"✓ Glue Catalog created: {catalog_name}")

        # Step 6: Setup database
        print("\n[Step 6] Setting up database...")
        t_env.execute_sql(f"USE CATALOG `{catalog_name}`")
        t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
        t_env.execute_sql(f"USE `{database_name}`")
        print(f"✓ Using database: {database_name}")

        # Step 7: Create sink table
        print("\n[Step 7] Creating Iceberg sink table...")
        sink_ddl = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                event_id STRING,
                user_id STRING,
                event_type STRING,
                event_timestamp BIGINT,
                ride_id STRING,
                surge_multiplier DOUBLE,
                estimated_wait_minutes INT,
                fare_amount DOUBLE,
                driver_rating DOUBLE,
                event_hour STRING
            ) PARTITIONED BY (event_hour)
            WITH (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'format-version' = '2'
            )
        """
        t_env.execute_sql(sink_ddl)
        print(f"✓ Iceberg table created: {table_name}")

        # Step 8: Start streaming insert
        print("\n[Step 8] Starting streaming insert...")
        insert_sql = f"""
            INSERT INTO `{table_name}`
            SELECT
                event_id,
                user_id,
                event_type,
                `timestamp` AS event_timestamp,
                ride_id,
                metadata.surge_multiplier,
                metadata.estimated_wait_minutes,
                metadata.fare_amount,
                metadata.driver_rating,
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)), 'yyyy-MM-dd-HH') AS event_hour
            FROM {kafka_source_table}
        """
        result = t_env.execute_sql(insert_sql)
        print("✓ Streaming job submitted successfully!")
        print("=" * 80)

        if is_local:
            print("\n[Local Mode] Waiting for job completion...")
            result.wait()

    except Exception as e:
        print("\n" + "=" * 80)
        print("ERROR: Application failed to start")
        print("=" * 80)
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        print("\nStack Trace:")
        import traceback
        traceback.print_exc()
        print("=" * 80)
        sys.exit(1)

if __name__ == "__main__":
    main()