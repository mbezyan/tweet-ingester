package bezyan.data.tweetingester.producer;

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

    private String kafkaTopic = "weather_tweets_topic3";
    private List<String> termsToTrack = Lists.newArrayList("weather");
    
    public TweetReaderProducer() {}

    public static void main(String[] args) {
        new TweetReaderProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a Twitter client
        Client client = createTwitterClient(msgQueue, termsToTrack);
        client.connect();

        // Create a Kafka Producer
        KafkaProducerWrapper kafkaProducer = new KafkaProducerWrapper(kafkaTopic);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            logger.info("Shutting down client from Twitter");
            client.stop();
            logger.info("Closing Producer");
            kafkaProducer.close();
            logger.info("Done!");
        }));
        
        // Loop to send Tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS); // Alternative: msgQueue.take // TODO: why not take
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
        FileReader reader = null;
        try {
            reader = new FileReader("tweet-producer/src/main/resources/twitter.properties");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to read properties file", e);
        }

        Properties p = new Properties();
        try {
            p.load(reader);
        } catch (IOException e) {
            throw new RuntimeException("Unable to load properties from file", e);
        }

        // Declaring the connection information:
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
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

        // Creating a client:
        ClientBuilder builder = new ClientBuilder()
                .name("KafkaElasticsearch1")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
