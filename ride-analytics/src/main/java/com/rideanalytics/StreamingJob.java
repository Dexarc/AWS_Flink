package com.amazonaws.services.msf;

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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class StreamingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH")
            .withZone(ZoneId.systemDefault());

    public static void main(String[] args) throws Exception {
        
        String bootstrapServers = "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
                                  "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
                                  "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098";
        
        // Setup Flink with proper checkpointing
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // CRITICAL: Configure checkpointing properly for Iceberg
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // Enable checkpoint recovery without data loss
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        env.setParallelism(1);

        LOG.info("Starting MSK to S3 Tables (Iceberg) - DataStream API with Deduplication");

        // Step 1: Build Kafka Source - CHANGED TO EARLIEST
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics("user_events")
            .setGroupId("flink-s3-tables-datastream")
            .setStartingOffsets(OffsetsInitializer.earliest())  // ✅ Read from earliest
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
            "Kafka Source"
        );

        // Step 2: Transform JSON to Flink RowData (flattening metadata)
        DataStream<RowData> rowDataStream = kafkaStream
            .map(json -> {
                try {
                    LOG.info("Processing record: {}", json);  // Changed to INFO for visibility
                    JsonNode node = mapper.readTree(json);
                    JsonNode metadata = node.get("metadata");
                    
                    // Create RowData with 10 fields matching Iceberg schema order
                    GenericRowData row = new GenericRowData(10);
                    
                    // Fields MUST match the order defined in Iceberg schema (field IDs 1-10)
                    row.setField(0, StringData.fromString(node.get("event_id").asText()));
                    row.setField(1, StringData.fromString(node.get("user_id").asText()));
                    row.setField(2, StringData.fromString(node.get("event_type").asText()));
                    row.setField(3, node.get("timestamp").asLong());
                    row.setField(4, StringData.fromString(node.get("ride_id").asText()));
                    row.setField(5, metadata.get("surge_multiplier").asDouble());
                    row.setField(6, metadata.get("estimated_wait_minutes").asInt());
                    row.setField(7, metadata.get("fare_amount").asDouble());
                    row.setField(8, metadata.get("driver_rating").asDouble());
                    
                    // Add hourly partition from timestamp
                    long timestamp = node.get("timestamp").asLong();
                    String eventHour = hourFormatter.format(Instant.ofEpochMilli(timestamp));
                    row.setField(9, StringData.fromString(eventHour));
                    
                    LOG.info("Created row with event_id: {}, event_hour: {}", 
                        node.get("event_id").asText(), eventHour);
                    
                    return (RowData) row;
                } catch (Exception e) {
                    LOG.error("Failed to parse: {}", json, e);
                    return null;
                }
            })
            .filter(row -> row != null)
            .name("Parse and Transform");

        // Step 3: Create S3 Tables Catalog Loader
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, "software.amazon.s3tables.iceberg.S3TablesCatalog");
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, 
                "arn:aws:s3tables:ap-south-1:149815625933:bucket/flink-transform-sink");
        
        CatalogLoader catalogLoader = CatalogLoader.custom(
            "s3_tables",
            catalogProps,
            new Configuration(),
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        );

        // Step 4: Create table if it doesn't exist
        TableIdentifier tableId = TableIdentifier.of("sink", "ride_events");
        
        try {
            org.apache.iceberg.catalog.Catalog catalog = catalogLoader.loadCatalog();
            
            // Check if table exists, if not create it
            if (!catalog.tableExists(tableId)) {
                LOG.info("Table does not exist, creating: {}", tableId);
                
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
                
                // ✅ FIXED: Added format-version 2 for upsert support
                Map<String, String> tableProps = new HashMap<>();
                tableProps.put("write.format.default", "parquet");
                tableProps.put("write.parquet.compression-codec", "snappy");
                tableProps.put("format-version", "2");  // Required for upsert mode
                tableProps.put("write.upsert.enabled", "true");  // Enable upsert explicitly
                
                catalog.createTable(tableId, schema, spec, tableProps);
                LOG.info("Table created successfully with format-version 2 and upsert enabled");
            } else {
                LOG.info("Table already exists: {}", tableId);
            }
        } catch (Exception e) {
            LOG.error("Error creating/updating table", e);
            throw e;
        }
        
        // CRITICAL: Create and open the TableLoader
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);
        tableLoader.open();
        LOG.info("TableLoader opened successfully");

        // Step 5: Write to Iceberg table using UPSERT with CORRECT equality fields
        // ✅ FIXED: Include BOTH event_id AND event_hour (partition field)
        FlinkSink.forRowData(rowDataStream)
            .tableLoader(tableLoader)
            .equalityFieldColumns(java.util.Arrays.asList("event_id", "event_hour"))  // Must include partition field!
            .upsert(true)
            .append();

        LOG.info("Starting Flink job execution with deduplication (reading from earliest)...");
        env.execute("MSK to S3 Tables - DataStream API with Deduplication");
    }
}