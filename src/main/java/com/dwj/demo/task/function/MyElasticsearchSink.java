package com.dwj.demo.task.function;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

/**
 * @author dwj
 * @date 2020/12/14 10:35
 */
public class MyElasticsearchSink<T> implements ElasticsearchSinkFunction<T> {
    public IndexRequest createIndexRequest(T element) {
        return Requests.indexRequest()
                .index("my-index")
                .source(element);
    }

    @Override
    public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}
