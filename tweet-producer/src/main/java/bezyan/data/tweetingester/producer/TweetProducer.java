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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetProducer {
    public static final String CONSUMER_KEY = "GeF32Yf5ZgyGRL04NBXBRbV96";
    private static final String CONSUMER_SECRET = "Z9cJUVO2ypfPVlLTAD8ieg0KVFDLNDy9Htr4ifhX1oGa5wNOpL";
    private static final String TOKEN = "962786320073465856-ymzVqq12jkP2kPAYsbCKMkI9OFgglIm";
    private static final String TOKEN_SECRET = "E05juHcgAO3Ha12CcFWIDPZ4pv4eaaEyc67lnjS74ytYK";

    Logger logger = LoggerFactory.getLogger(TweetProducer.class.getName());

    private String kafkaTopic = "weather_tweets_topic2";
    private List<String> termsToTrack = Lists.newArrayList("weather");
    
    public TweetProducer() {}

    public static void main(String[] args) {
        new TweetProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a Twitter client
        Client client = createTwitterClient(msgQueue, termsToTrack);
        client.connect();

        // Create a Kafka Producer
        MyKafkaProducer kafkaProducer = new MyKafkaProducer(kafkaTopic);

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
        // Declaring the connection information:
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Specify terms to track
        hosebirdEndpoint.trackTerms(termsToTrack);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

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
