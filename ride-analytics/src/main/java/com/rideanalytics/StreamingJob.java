package com.amazonaws.services.msf;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
            .withZone(ZoneId.systemDefault());

    public static void main(String[] args) throws Exception {
        
        // Load properties from KDA Runtime
        Properties appProperties = loadApplicationProperties(args);
        
        // Get parameters with defaults
        String bootstrapServers = appProperties.getProperty("kafka.bootstrap.servers",
            "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
            "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
            "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098");
        
        // Support comma-separated list of topics
        String kafkaTopicsStr = appProperties.getProperty("kafka.topics", "user_events");
        String[] kafkaTopics = kafkaTopicsStr.split(",");
        
        String consumerGroup = appProperties.getProperty("kafka.consumer.group", "flink-s3-tables-datastream");
        String startOffset = appProperties.getProperty("kafka.offset", "earliest");
        
        String s3Warehouse = appProperties.getProperty("s3.warehouse", 
            "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink");
        String namespace = appProperties.getProperty("table.namespace", "sink");
        
        int parallelism = Integer.parseInt(appProperties.getProperty("parallelism", "1"));
        long checkpointInterval = Long.parseLong(appProperties.getProperty("checkpoint.interval", "60000"));
        
        LOG.info("=== Configuration ===");
        LOG.info("Kafka Topics: {}", kafkaTopicsStr);
        LOG.info("Consumer Group: {}", consumerGroup);
        LOG.info("Starting Offset: {}", startOffset);
        LOG.info("Namespace: {}", namespace);
        LOG.info("S3 Warehouse: {}", s3Warehouse);
        LOG.info("Parallelism: {}", parallelism);
        LOG.info("=====================");
        
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointInterval / 2);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        LOG.info("Starting MSK to S3 Tables (Iceberg) - Multi-Topic DataStream API");

        // S3 Tables Catalog (shared across all topics)
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "software.amazon.s3tables.iceberg.S3TablesCatalog");
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, s3Warehouse);
        
        CatalogLoader catalogLoader = CatalogLoader.custom(
            "s3_tables",
            catalogProps,
            new Configuration(),
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        );

        // Process each topic
        for (String kafkaTopic : kafkaTopics) {
            kafkaTopic = kafkaTopic.trim();
            
            // Generate table name from topic name
            String tableName = kafkaTopic.replaceAll("[-.]", "_").toLowerCase();
            
            LOG.info("Processing topic: {} -> table: {}.{}", kafkaTopic, namespace, tableName);
            
            processTopicToTable(
                env, 
                bootstrapServers, 
                kafkaTopic, 
                consumerGroup, 
                startOffset,
                catalogLoader,
                namespace,
                tableName
            );
        }

        env.execute("MSK to S3 Tables - Multi-Topic");
    }
    
    /**
     * Process a single Kafka topic and write to corresponding Iceberg table
     */
    private static void processTopicToTable(
        StreamExecutionEnvironment env,
        String bootstrapServers,
        String kafkaTopic,
        String consumerGroup,
        String startOffset,
        CatalogLoader catalogLoader,
        String namespace,
        String tableName
    ) throws Exception {
        
        // Kafka Source for this topic
        OffsetsInitializer offsetInit = startOffset.equalsIgnoreCase("earliest") 
            ? OffsetsInitializer.earliest() 
            : OffsetsInitializer.latest();
        
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(kafkaTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(offsetInit)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty("security.protocol", "SASL_SSL")
            .setProperty("sasl.mechanism", "AWS_MSK_IAM")
            .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
            .setProperty("sasl.client.callback.handler.class", 
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
            .build();

        DataStream<String> kafkaStream = env.fromSource(
            source, 
            WatermarkStrategy.noWatermarks(), 
            "Kafka Source - " + kafkaTopic
        );

        // Transform
        DataStream<RowData> rowDataStream = kafkaStream
            .map(json -> {
                try {
                    JsonNode node = mapper.readTree(json);
                    JsonNode metadata = node.get("metadata");
                    
                    GenericRowData row = new GenericRowData(10);
                    row.setField(0, StringData.fromString(node.get("event_id").asText()));
                    row.setField(1, StringData.fromString(node.get("user_id").asText()));
                    row.setField(2, StringData.fromString(node.get("event_type").asText()));
                    row.setField(3, node.get("timestamp").asLong());
                    row.setField(4, StringData.fromString(node.get("ride_id").asText()));
                    row.setField(5, metadata.get("surge_multiplier").asDouble());
                    row.setField(6, metadata.get("estimated_wait_minutes").asInt());
                    row.setField(7, metadata.get("fare_amount").asDouble());
                    row.setField(8, metadata.get("driver_rating").asDouble());
                    
                    long timestamp = node.get("timestamp").asLong();
                    String eventHour = hourFormatter.format(Instant.ofEpochMilli(timestamp));
                    row.setField(9, StringData.fromString(eventHour));
                    
                    return (RowData) row;
                } catch (Exception e) {
                    LOG.error("Failed to parse from topic {}: {}", kafkaTopic, json, e);
                    return null;
                }
            })
            .filter(row -> row != null)
            .name("Parse and Transform - " + kafkaTopic);

        // Create table if needed
        TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
        
        try {
            org.apache.iceberg.catalog.Catalog catalog = catalogLoader.loadCatalog();
            
            if (!catalog.tableExists(tableId)) {
                LOG.info("Creating table: {}", tableId);
                
                org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                    org.apache.iceberg.types.Types.NestedField.required(1, "event_id", org.apache.iceberg.types.Types.StringType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(2, "user_id", org.apache.iceberg.types.Types.StringType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(3, "event_type", org.apache.iceberg.types.Types.StringType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(4, "event_timestamp", org.apache.iceberg.types.Types.LongType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(5, "ride_id", org.apache.iceberg.types.Types.StringType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(6, "surge_multiplier", org.apache.iceberg.types.Types.DoubleType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(7, "estimated_wait_minutes", org.apache.iceberg.types.Types.IntegerType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(8, "fare_amount", org.apache.iceberg.types.Types.DoubleType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(9, "driver_rating", org.apache.iceberg.types.Types.DoubleType.get()),
                    org.apache.iceberg.types.Types.NestedField.required(10, "event_hour", org.apache.iceberg.types.Types.StringType.get())
                );
                
                org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec.builderFor(schema)
                    .identity("event_hour")
                    .build();
                
                Map<String, String> tableProps = new HashMap<>();
                tableProps.put("write.format.default", "parquet");
                tableProps.put("write.parquet.compression-codec", "snappy");
                tableProps.put("format-version", "2");
                tableProps.put("write.upsert.enabled", "true");
                
                catalog.createTable(tableId, schema, spec, tableProps);
                LOG.info("Table created successfully: {}", tableId);
            } else {
                LOG.info("Table already exists: {}", tableId);
            }
        } catch (Exception e) {
            LOG.error("Error with table: {}", tableId, e);
            throw e;
        }
        
        // Write to Iceberg
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);
        tableLoader.open();

        FlinkSink.forRowData(rowDataStream)
            .tableLoader(tableLoader)
            .equalityFieldColumns(java.util.Arrays.asList("event_id", "event_hour"))
            .upsert(true)
            .append();
        
        LOG.info("Sink configured for topic {} -> table {}", kafkaTopic, tableId);
    }
    
    /**
     * Load application properties from KDA Runtime
     */
    private static Properties loadApplicationProperties(String[] args) throws IOException {
        Map<String, Properties> applicationProperties = 
            KinesisAnalyticsRuntime.getApplicationProperties();
        
        if (applicationProperties.containsKey("ApplicationProperties")) {
            LOG.info("Loaded properties from ApplicationProperties group");
            return applicationProperties.get("ApplicationProperties");
        }
        
        // Fallback to first available property group
        if (!applicationProperties.isEmpty()) {
            String firstKey = applicationProperties.keySet().iterator().next();
            LOG.info("Loaded properties from {} group", firstKey);
            return applicationProperties.get(firstKey);
        }
        
        LOG.warn("No property groups found, using default values");
        return new Properties();
    }
}