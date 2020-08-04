package bezyan.data.tweetingester.consumer;

import bezyan.data.tweetingester.consumer.utils.TweetUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

public class TweetConsumerIndexer {
    private static final Logger logger = LoggerFactory.getLogger(TweetConsumerIndexer.class.getName());

    private TweetConsumerIndexer() {}

    public static void main (String[] args) throws IOException {
        new TweetConsumerIndexer().run();
    }

    private void run() throws IOException {
        //TODO: allow user to specify Kafka topic and Consumer Group name
        final String consumerTopic = "weather_tweets_topic";
        final String consumerGroupId = "weather_tweets_application";

        RestHighLevelClient client = ElasticsearchClient.createClient();
        KafkaConsumer<String, String> kafkaConsumer = new TweetConsumer().createConsumer(consumerTopic, consumerGroupId);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            logger.info("Closing Elasticsearch client");
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Done!");
        }));

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

                    IndexRequest indexRequest = new IndexRequest("tweets");
                    indexRequest.id(id);
                    indexRequest.source(tweetJson, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                    logger.info("Record read from Kafka: " + record.topic() + "-" + record.partition() + "-" + record.offset());
                }

                client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing the offsets.");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed.");
            }

            // Wait one second before polling again
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
