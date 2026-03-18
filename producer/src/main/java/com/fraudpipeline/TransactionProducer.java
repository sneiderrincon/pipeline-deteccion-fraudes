package com.fraudpipeline;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        // ── Config from environment ────────────────────────────────────────
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic            = getEnv("KAFKA_TOPIC",             "fraud-transactions");
        String dataPath         = getEnv("TEST_DATA_PATH",          "/data/fraudTest.csv");
        int    delayMin         = Integer.parseInt(getEnv("DELAY_MIN_MS", "50"));
        int    delayMax         = Integer.parseInt(getEnv("DELAY_MAX_MS", "300"));

        // ── Kafka producer properties ──────────────────────────────────────
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,              "all");   // strongest durability
        props.put(ProducerConfig.RETRIES_CONFIG,           "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG,         "5");     // small batching
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,  "snappy");

        AtomicLong sent = new AtomicLong(0);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = Files.newBufferedReader(Paths.get(dataPath))) {

            reader.readLine(); // skip CSV header

            String line;
            long   idx = 0;

            while ((line = reader.readLine()) != null) {
                String key = "tx-" + idx;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, line);

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send record: {}", ex.getMessage());
                    } else {
                        long count = sent.incrementAndGet();
                        if (count % 1000 == 0) {
                            log.info("Sent {} transactions (partition={}, offset={})",
                                     count, metadata.partition(), metadata.offset());
                        }
                    }
                });

                idx++;
                int delay = ThreadLocalRandom.current().nextInt(delayMin, delayMax + 1);
                Thread.sleep(delay);
            }

            producer.flush();
            log.info("Done. Total transactions sent: {}", sent.get());
        }
    }

    private static String getEnv(String key, String defaultVal) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultVal;
    }
}
