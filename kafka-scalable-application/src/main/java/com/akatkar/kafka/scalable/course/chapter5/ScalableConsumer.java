package com.akatkar.kafka.scalable.course.chapter5;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScalableConsumer {

    public static void main(String[] args) {

        //Setup Properties for consumer
        Properties kafkaProps = new Properties();

        //List of Kafka brokers to connect to
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        //Deserializer class to convert Keys from Byte Array to String
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Deserializer class to convert Messages from Byte Array to String
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        //Consumer Group ID for this consumer
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-scalable-consumer");

        //Set to consume from the earliest message, on start when no offset is
        //available in Kafka
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        //Set auto commit to false
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //Create a Consumer
        KafkaConsumer<String, String> simpleConsumer = new KafkaConsumer<>(kafkaProps);

        //Subscribe to the kafka.learning.orders topic
        simpleConsumer.subscribe(List.of("kafka.learning.orders"));

        //Create a Thread Pool of workers
        System.out.println("Starting worker threads for parallel processing");

        List<SampleWorker> workerList = new ArrayList<SampleWorker>();

        //create 5 worker threads
        for (int i=0 ;i< 5; i++) {
            SampleWorker worker = new SampleWorker("Worker_" + i);
            Thread newThread = new Thread(worker);
            newThread.start();
            workerList.add(worker);
        }

        //Continuously poll for new messages
        while(true) {

            //Poll with timeout of 1 second
            ConsumerRecords<String, String> messages =
                    simpleConsumer.poll(Duration.ofMillis(1000));

            //Print batch of records consumed
            for (ConsumerRecord<String, String> message : messages) {

                System.out.println("Message fetched : " + message);
                //Add to Queue
                SampleWorker.addToQueue(message.value());

            }

            //Wait for processing to complete across threads
            while( SampleWorker.getPendingCount() > 0) {
                try {
                    Thread.sleep(100);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }

            //Commit messages manually now.
            simpleConsumer.commitAsync();
            System.out.println("All messages successfully processed. Proceeding to poll more");
        }


    }
}
