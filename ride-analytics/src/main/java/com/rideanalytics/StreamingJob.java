package com.amazonaws.services.msf;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set parallelism to 1 for debugging (increase later for production)
        env.setParallelism(1);
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // checkpoint every 60 seconds

        // Generate unique consumer group to avoid offset issues
        String consumerGroup = "flink-consumer-" + System.currentTimeMillis();
        
        LOG.info("Starting Flink job with consumer group: {}", consumerGroup);

        // Build KafkaSource with IAM authentication
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(
                "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
                "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098," +
                "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098")
            .setTopics("user_events")
            .setGroupId(consumerGroup)
            // Use LATEST to read only NEW messages (change to earliest() to read all)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            
            // IAM Authentication properties
            .setProperty("security.protocol", "SASL_SSL")
            .setProperty("sasl.mechanism", "AWS_MSK_IAM")
            .setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
            .setProperty("sasl.client.callback.handler.class", 
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
            
            // Connection and timeout properties
            .setProperty("request.timeout.ms", "60000")
            .setProperty("session.timeout.ms", "30000")
            .setProperty("heartbeat.interval.ms", "3000")
            
            // Partition discovery for dynamic partition addition
            .setProperty("partition.discovery.interval.ms", "10000")
            
            // Disable auto-commit (Flink handles this)
            .setProperty("enable.auto.commit", "false")
            
            // Increase fetch size for better throughput
            .setProperty("fetch.min.bytes", "1")
            .setProperty("fetch.max.wait.ms", "500")
            
            .build();

        // Create data stream from Kafka
        DataStream<String> stream = env.fromSource(
            source, 
            WatermarkStrategy.noWatermarks(), 
            "Kafka Source"
        );

        // CRITICAL FIX: Use uid() and name() on map operator, then disable chaining
        // This breaks operator chaining and makes metrics visible in the Flink UI
        stream
            .map(msg -> {
                LOG.info("=== RECEIVED MESSAGE FROM KAFKA ===");
                LOG.info("Message: {}", msg);
                System.err.println(">>> KAFKA MESSAGE: " + msg);
                return "Processed: " + msg;
            })
            .name("Process Kafka Message")
            .uid("process-kafka-msg")
            .disableChaining()  // Disable chaining HERE (on the SingleOutputStreamOperator)
            .print()
            .name("Print to Output");

        // Execute the job
        LOG.info("Executing Flink job - waiting for messages from user_events topic...");
        env.execute("MSK to Flink POC Job");
    }
}