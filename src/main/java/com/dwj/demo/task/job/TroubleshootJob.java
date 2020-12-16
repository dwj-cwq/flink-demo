package com.dwj.demo.task.job;

import com.dwj.demo.task.config.KafkaConfig;
import com.dwj.demo.task.decoder.KafkaAlertCoder;
import com.dwj.demo.task.dto.Alert;
import com.dwj.demo.task.dto.TroubleshootResult;
import com.dwj.demo.task.function.MyElasticsearchBuilder;
import com.dwj.demo.task.function.MyElasticsearchSink;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dwj
 * @date 2020/11/21 18:12
 */
public class TroubleshootJob {

    private static final String TOPIC = "alert-test";

    public static void doExecute() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Alert> streamSource = environment.addSource(new FlinkKafkaConsumer<Alert>(
                TOPIC,
                new KafkaAlertCoder(),
                KafkaConfig.getKafkaConfig()));

        SingleOutputStreamOperator<TroubleshootResult> resultStream = streamSource
                .keyBy(alert -> getInstanceByTags(alert.getTags()))
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ProcessWindowFunction<Alert, TroubleshootResult, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Alert> elements, Collector<TroubleshootResult> out) throws Exception {
                        elements.forEach(alert -> {
                            if (monitorEnable(alert)) {
                                List<Alert> instancePool = Lists.newArrayList();
                                instancePool.add(alert);
                                out.collect(doCheckAction());
                            }
                        });
                    }
                });


        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.0.60.128", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        MyElasticsearchBuilder<TroubleshootResult> esSinkBuilder =
                new MyElasticsearchBuilder<>(httpHosts, new MyElasticsearchSink<>());

        // finally, build and add the sink to the job's pipeline
        resultStream.addSink(esSinkBuilder.build());

        environment.execute("troubleshootJob");
    }

    private static boolean monitorEnable(Alert alert) {
        return true;
    }

    private static String getInstanceByTags(Alert.Tags tags) {
        return "app";
    }

    private static TroubleshootResult doCheckAction() {
        return new TroubleshootResult();
    }

    public static void main(String[] args) throws Exception {
        doExecute();
    }

}
