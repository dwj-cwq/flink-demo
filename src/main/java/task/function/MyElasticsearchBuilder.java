package task.function;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;

import java.util.List;

/**
 * @author dwj
 * @date 2020/12/14 10:52
 */
public class MyElasticsearchBuilder<T> extends ElasticsearchSink.Builder<T> {
    public MyElasticsearchBuilder(List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        super(httpHosts, elasticsearchSinkFunction);
        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        setBulkFlushMaxActions(1);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        Header[] headers = new Header[2];
        headers[0] = new BasicHeader("Connection", "Keep-Alive");
        headers[1] = new BasicHeader("Proxy-Connection", "Keep-Alive");
        setRestClientFactory(
                restClientFactory -> {
                    restClientFactory.setDefaultHeaders(headers);
                    restClientFactory.setRequestConfigCallback(
                            requestConfigCallback -> {
                                requestConfigCallback.setConnectTimeout(300000);
                                requestConfigCallback.setSocketTimeout(300000);
                                requestConfigCallback.setConnectionRequestTimeout(300000);
                                return requestConfigCallback;
                            }
                    );
                }
        );

        // set failure handler
        setFailureHandler(new RetryRejectedExecutionFailureHandler());
    }
}
