import logging
import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger(__name__)


def main():
    LOG.info("Starting PyFlink MSK → S3 Tables (Iceberg REST)")

    # ------------------------------------------------------------------
    # FIXED CONFIG (NO RUNTIME VARS)
    # ------------------------------------------------------------------
    REGION = "ap-south-1"

    KAFKA_BOOTSTRAP = (
        "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
        "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
    )

    KAFKA_TOPIC = "user_events"
    KAFKA_GROUP = "pyflink-s3tables"

    S3_TABLE_WAREHOUSE = (
        "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink"
    )

    # ------------------------------------------------------------------
    # TABLE ENV
    # ------------------------------------------------------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    cfg = t_env.get_config()
    cfg.set("execution.checkpointing.interval", "60000")
    cfg.set("execution.checkpointing.mode", "EXACTLY_ONCE")

    # ------------------------------------------------------------------
    # KAFKA SOURCE
    # ------------------------------------------------------------------
    t_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        event_id STRING,
        user_id STRING,
        event_type STRING,
        event_timestamp BIGINT,
        ride_id STRING,
        surge_multiplier DOUBLE,
        estimated_wait_minutes INT,
        fare_amount DOUBLE,
        driver_rating DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{KAFKA_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
        'properties.group.id' = '{KAFKA_GROUP}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'properties.security.protocol' = 'SASL_SSL',
        'properties.sasl.mechanism' = 'AWS_MSK_IAM',
        'properties.sasl.jaas.config' =
            'software.amazon.msk.auth.iam.IAMLoginModule required;',
        'properties.sasl.client.callback.handler.class' =
            'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
    )
    """)

    # ------------------------------------------------------------------
    # ICEBERG REST CATALOG → S3 TABLES
    # ------------------------------------------------------------------
    t_env.execute_sql(f"""
    CREATE CATALOG s3tables_rest WITH (
        'type' = 'iceberg',
        'catalog-type' = 'rest',
        'uri' = 'https://s3tables.{REGION}.amazonaws.com/iceberg',
        'warehouse' = '{S3_TABLE_WAREHOUSE}',
        'rest.sigv4-enabled' = 'true',
        'rest.signing-name' = 's3tables',
        'rest.signing-region' = '{REGION}'
    )
    """)

    t_env.use_catalog("s3tables_rest")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS sink")
    t_env.use_database("sink")

    # ------------------------------------------------------------------
    # SINK TABLE
    # ------------------------------------------------------------------
    t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS user_events (
        event_id STRING,
        user_id STRING,
        event_type STRING,
        event_timestamp BIGINT,
        ride_id STRING,
        surge_multiplier DOUBLE,
        estimated_wait_minutes INT,
        fare_amount DOUBLE,
        driver_rating DOUBLE,
        event_date STRING
    )
    PARTITIONED BY (event_date)
    WITH (
        'format-version' = '2',
        'write.format.default' = 'parquet'
    )
    """)

    # ------------------------------------------------------------------
    # STREAMING INSERT
    # ------------------------------------------------------------------
    LOG.info("Streaming data into S3 Table bucket...")

    t_env.execute_sql("""
    INSERT INTO user_events
    SELECT
        event_id,
        user_id,
        event_type,
        event_timestamp,
        ride_id,
        surge_multiplier,
        estimated_wait_minutes,
        fare_amount,
        driver_rating,
        DATE_FORMAT(
            FROM_UNIXTIME(event_timestamp / 1000),
            'yyyy-MM-dd'
        )
    FROM kafka_source
    """).wait()


if __name__ == "__main__":
    main()
