package bezyan.data.tweetingester.consumer.utils;

import com.google.gson.JsonParser;

import java.util.Optional;

public class TweetUtils {
    private static final String TWEET_ID_ATTRIBUTE_NAME = "id_str";
    private static final JsonParser jsonParser = new JsonParser(); // TODO: use new constructor

    public static Optional<String> extractTweetId(String tweetJson) {
        return Optional.ofNullable(
                jsonParser
                        .parse(tweetJson)
                        .getAsJsonObject()
                        .get(TWEET_ID_ATTRIBUTE_NAME)
                        .getAsString()
        );
    }
}
