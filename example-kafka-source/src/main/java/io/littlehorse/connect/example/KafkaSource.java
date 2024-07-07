package io.littlehorse.connect.example;

import io.littlehorse.connect.source.FailedRecord;
import io.littlehorse.connect.source.JsonSourceRecord;
import io.littlehorse.connect.source.LHSourceConnector;
import io.littlehorse.connect.source.LHSourceConnectorContext;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class KafkaSource extends LHSourceConnector {

    public static final String TOPIC_KEY = "topic";
    public static final String FLUSH_INTERVAL_MS_KEY = "flush.interval.ms";

    private LHSourceConnectorContext context;
    private Consumer<String, String> consumer;
    private Duration flushInterval;
    private Instant lastFlushTime;

    @Override
    public void configure(Properties config, LHSourceConnectorContext context) {
        this.context = context;
        this.lastFlushTime = Instant.now();

        System.out.println("\n\n\nadsfasdf\n\n\n");
        System.out.println(config);

        Properties kafkaProps = new Properties();
        for (String prop : config.stringPropertyNames()) {
            if (prop.startsWith("kafka.")) {
                kafkaProps.put(prop.substring(6), config.get(prop));
            }
        }
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(kafkaProps);
        this.consumer.subscribe(Collections.singleton(String.valueOf(config.get(TOPIC_KEY))));

        long flushIntervalMs = Long.valueOf(String.valueOf(config.getOrDefault(FLUSH_INTERVAL_MS_KEY, "1000")));
        this.flushInterval = Duration.ofMillis(flushIntervalMs);
    }

    @Override
    public void start() {
        while (true) {
            // Poll Timeout could be another config...
            processRecords(consumer.poll(flushInterval));
            if (Instant.now().minus(flushInterval).isAfter(lastFlushTime)) {
                flush();
            }
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        records.forEach(record -> {
            try {
                Map<String, String> headers = new HashMap<>();
                for (Header header : record.headers().toArray()) {
                    headers.put(header.key(), new String(header.value()));
                }
                context.produce(new JsonSourceRecord(record.value(), headers));
                System.out.println("Produced record!");
            } catch (Exception exn) {
                log.error("Couldn't process record {}: {}", record.value(), exn);
            }
        });
    }

    private void flush() {
        lastFlushTime = Instant.now();
        List<FailedRecord> failedRecords = context.flushRecords();
        for (FailedRecord failedRecord : failedRecords) {
            System.out.println("Failed to process " + failedRecord.getRecord());
        }
        consumer.commitSync();
    }
}
