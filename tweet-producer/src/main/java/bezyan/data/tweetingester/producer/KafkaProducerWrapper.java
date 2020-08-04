package bezyan.data.tweetingester.producer;

import bezyan.data.tweetingester.common.PropertiesUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWrapper {
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String ALL = "all";
    private static final String COMPRESSION_TYPE_SNAPPY = "snappy";
    private static final int BYTES_PER_KILOBYTE = 1024;


    Logger logger = LoggerFactory.getLogger(KafkaProducerWrapper.class.getName());

    private String topic;

    private KafkaProducer producer;

    public KafkaProducerWrapper(String topic) {
        Properties p = PropertiesUtil.getPropertiesFromFile("src/main/resources/kafka.properties");

        // Create Produce properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, p.getProperty("kafka.bootstrap.servers"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Configure Producer for safety
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(true));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, ALL);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));

        // Configure Producer for high throughput at a cost of some latency and CPU usage
        // Chosen snappy as it's a good compromise for CPU time on Producer/Consumer side and message size for JSON
        // See: https://blog.cloudflare.com/squeezing-the-firehose/
        // properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE_SNAPPY); // TODO: uncomment
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * BYTES_PER_KILOBYTE));

        // Create Producer
        // Key (k) is String, Value (v) is String
        producer = new KafkaProducer<String, String>(properties);

        this.topic = topic;
    }

    public void produce(String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);

        // Send data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    logger.error("Something went wrong", e);
                }
            }
        });
    }

    public void close() {
        // Flush and close Producer
        producer.close();
    }
}
