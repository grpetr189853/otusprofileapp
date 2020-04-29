package twitter.tweeutils;

import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;

public class ClientSupplier {
    public static BasicClient prepareAndConnectDefaultClient(String consumerKey,
                                                   String  consumerSecret,
                                                   String  token,
                                                   String  secret,
                                                   BlockingQueue<String> queue){
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        BasicClient client = new com.twitter.hbc.ClientBuilder()
                .name("twitterClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        return client;
    }
}
