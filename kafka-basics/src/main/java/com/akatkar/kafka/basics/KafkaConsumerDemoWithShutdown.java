package com.akatkar.kafka.basics;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDemoWithShutdown.class.getName());

    private static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        log.info("Consumer with shutdown is running...");
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "group_id");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Thread mainThread = Thread.currentThread();

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down the consumer");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                }
            }));

            consumer.subscribe(List.of(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> data: records) {
                    log.info("consumed: {}", data);
                }

            }

        } catch (WakeupException wakeupException) {
            log.info("consumer gracefully shutdown");
        }
    }
}
