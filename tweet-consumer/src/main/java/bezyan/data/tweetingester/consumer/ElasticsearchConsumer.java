package bezyan.data.tweetingester.consumer;

import bezyan.data.tweetingester.consumer.utils.TweetUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

public class ElasticsearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());


    public static RestHighLevelClient createClient() {
        String hostname = "kafka-sandbox1-2123547504.ap-southeast-2.bonsaisearch.net";
        String username = "lepcnpkrsv";
        String password = "9t0200zw26";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main (String[] args) throws IOException {
        final String consumerTopic = "weather_tweets_topic2";
        final String consumerGroupId = "weather_tweets_application2";

        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> kafkaConsumer = new TweetConsumer().createConsumer(consumerTopic, consumerGroupId);

        // Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            int recordCount = records.count();
            logger.info("Received " + recordCount + " records.");

            if (recordCount > 0) {
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    String tweetJson = record.value();

                    // Obtain ID
                    Optional<String> tweetId = TweetUtils.extractTweetId(tweetJson);
                    String genericRecordId = record.topic() + "-" + record.partition() + "-" + record.offset();
                    String id = tweetId.orElse(genericRecordId);

                    IndexRequest indexRequest = new IndexRequest( //TODO: replace with correct method
                            "twitter5",
                            "weather",
                            id
                    ).source(tweetJson, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                    logger.info("Record read from Kafka: " + record.topic() + "-" + record.partition() + "-" + record.offset());
                }

                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing the offsets.");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed.");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //client.close();
    }
}
