package bezyan.data.tweetingester.producer;

import bezyan.data.tweetingester.common.PropertiesUtil;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetReaderProducer {
    Logger logger = LoggerFactory.getLogger(TweetReaderProducer.class.getName());

    //TODO: allow user to specify terms(s) to follow and name of Kafka topic
    private static final String kafkaTopic = "weather_tweets_topic";
    private static final List<String> termsToTrack = Lists.newArrayList("weather");
    
    private TweetReaderProducer() {}

    public static void main(String[] args) {
        new TweetReaderProducer().run();
    }

    public void run() {
        // Set up blocking queues ensuring to size properly based on expected TPS of the stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a Twitter client
        Client client = createTwitterClient(msgQueue, termsToTrack);
        client.connect();

        // Create a Kafka Producer
        KafkaProducerWrapper kafkaProducer = new KafkaProducerWrapper(kafkaTopic);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            logger.info("Shutting down Twitter client");
            client.stop();
            logger.info("Closing Kafka Producer");
            kafkaProducer.close();
            logger.info("Done!");
        }));
        
        // Loop to send Tweets to Kafka
        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    kafkaProducer.produce(null, msg);
                    logger.info(msg);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }
        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> termsToTrack) {
        Properties p = PropertiesUtil.getPropertiesFromFile("tweet-producer/src/main/resources/twitter.properties");

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Specify terms to track
        hosebirdEndpoint.trackTerms(termsToTrack);

        // These secrets should be read from a config file
        Authentication hosebirdAuth =
                new OAuth1(
                        p.getProperty("twitter.api.consumer.key"),
                        p.getProperty("twitter.api.consumer.secret"),
                        p.getProperty("twitter.api.token"),
                        p.getProperty("twitter.api.token.secret")
                );

        // Creating a client
        ClientBuilder builder = new ClientBuilder()
                .name("KafkaElasticsearch1")                              // Optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
