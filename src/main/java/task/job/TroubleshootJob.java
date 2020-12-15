package task.job;

import com.bizseer.xts.task.dto.Alert;
import com.bizseer.xts.task.dto.TroubleshootResult;
import com.bizseer.xts.task.function.MyElasticsearchBuilder;
import com.bizseer.xts.task.function.MyElasticsearchSink;
import com.bizseer.xts.task.util.SerializableUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import java.util.Properties;

/**
 * @author dwj
 * @date 2020/11/21 18:12
 */
public class TroubleshootJob {

    private static final String TOPIC = "alert-test";

    public static void doExecute() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.addSource(new FlinkKafkaConsumer<String>(
                TOPIC,
                new SimpleStringSchema(),
                getKafkaConfig()));

        SingleOutputStreamOperator<TroubleshootResult> resultStream = streamSource
                .map(new MapFunction<String, Alert>() {
                    @Override
                    public Alert map(String s) throws Exception {
                        return SerializableUtil.json2obj(s, Alert.class);
                    }
                })
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

    private static Properties getKafkaConfig() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //偏移量自动重置
        properties.setProperty("auto.offset.reset", "latest");
        return properties;
    }

    public static void main(String[] args) throws Exception {
        doExecute();
    }

}
