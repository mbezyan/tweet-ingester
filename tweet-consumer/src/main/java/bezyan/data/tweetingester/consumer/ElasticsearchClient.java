package bezyan.data.tweetingester.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ElasticsearchClient {
    public static RestHighLevelClient createClient() {
        FileReader reader = null;
        try {
            reader = new FileReader("tweet-consumer/src/main/resources/elasticsearch.properties");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to read properties file", e);
        }

        Properties p = new Properties();
        try {
            p.load(reader);
        } catch (IOException e) {
            throw new RuntimeException("Unable to load properties from file", e);
        }

        String hostname = p.getProperty("elasticsearch.hostname");
        String username = p.getProperty("elasticsearch.username");
        String password = p.getProperty("elasticsearch.password");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(builder);
    }
}
