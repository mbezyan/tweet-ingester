package bezyan.data.tweetingester.consumer;

import bezyan.data.tweetingester.common.PropertiesUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Properties;

public class ElasticsearchClient {
    public static RestHighLevelClient createClient() {
        Properties p = PropertiesUtil.getPropertiesFromFile("tweet-consumer/src/main/resources/elasticsearch.properties");

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
