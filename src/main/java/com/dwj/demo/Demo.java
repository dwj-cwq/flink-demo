package com.dwj.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author dwj
 * @date 2020/11/10 19:42
 */
public class Demo {

    public static void main(String[] args) throws Exception {
        // kafka stream
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.100.148:13021");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> stream = environment.addSource(
                new FlinkKafkaConsumer<>("alert-topic", new SimpleStringSchema(), properties)
        ).setParallelism(1);

        environment.execute();
    }

    private void dataSet() throws Exception {
        // dataSet
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple1<Integer>> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(new Tuple1<Integer>(i));
        }

        DataSet<Tuple1<Integer>> set = environment.fromCollection(data);
        set.aggregate(Aggregations.MAX, 0).print();
    }

}
