package com.akatkar.kafka.opensearch.consumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
    private static final String INDEX_NAME = "wikimedia";
    private static final String TOPIC_NAME = "wikimedia.recentchange";


    private static final String URL = "http://localhost:9200";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient openSearchClient = createRestHighLevelClient();

        KafkaConsumer<String, String> kafkaConsumer = createConsumer();
        kafkaConsumer.subscribe(List.of(TOPIC_NAME));

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down the consumer");
            kafkaConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
            }
        }));

        try (openSearchClient; kafkaConsumer) {
            boolean isIndexExist  = openSearchClient.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
            if (!isIndexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);

                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("wikimedia index created");
            }

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                log.info("Received {} records", records.count());

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records){
                    try {
                        // There are two different way to generate ID to provide idempotent writing in opensearch
                        // First one is to use record information from Kafka
                        // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                        // Second one is to use metadata
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                                .id(id)
                                .source(record.value(), XContentType.JSON);
                        // To send one by one
                        // IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        // log.info("OpenSearch consumer : ID is {}", indexResponse.getId());

                        // To send bulk
                        bulkRequest.add(indexRequest);

                    } catch (Exception e) {
                        log.error("Unexpected error writing to OpenSearch", e);
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("OpenSearch consumer : inserted {} records", bulkResponse.getItems().length);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        } catch (WakeupException wakeupException) {
            log.info("consumer gracefully shutdown");
        } finally {
            kafkaConsumer.close();
            openSearchClient.close();
        }



    }

    private static String extractId(String jsonRecord) {
        return JsonParser.parseString(jsonRecord)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static RestHighLevelClient createRestHighLevelClient() {
        RestHighLevelClient restHighLevelClient;
        URI connURI = URI.create(URL);
        String userInfo = connURI.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort())));
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort()))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(cp)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy()))
            );
        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, "wikemedia-opensearch");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(properties);
    }
}
