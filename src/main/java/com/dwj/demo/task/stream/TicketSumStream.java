package com.dwj.demo.task.stream;

import com.dwj.demo.task.decoder.KafkaTicketDecoder;
import com.dwj.demo.task.dto.TicketDTO;
import com.dwj.demo.task.function.PrintProcessFunction;
import com.dwj.demo.task.function.TicketProcessFunction;
import com.dwj.demo.task.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author Chen Wenqun
 * 计算实时车票的总数量，以及所有价格总数
 */
public class TicketSumStream {

    public static void main(String[] args) throws Exception {
//        processTimeStream(); // 基于 process time 计算 watermark 推动窗口的流
        eventTimeStream(); // 基于 event time 计算 watermark 推动窗口的流
    }

    public static void processTimeStream() throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        WatermarkStrategy<TicketDTO> strategy = WatermarkStrategy
            .forBoundedOutOfOrderness(Duration.ofSeconds(20));

        DataStream<TicketDTO> tickets = environment.addSource(new FlinkKafkaConsumer<>("ticket",
            new KafkaTicketDecoder(), KafkaUtil.kafkaConfigs()).assignTimestampsAndWatermarks(strategy));

        OutputTag<TicketDTO> sideOutput = new OutputTag<TicketDTO>("side") {
        };

        tickets
            .process(new PrintProcessFunction())
            .keyBy(TicketDTO::getUser)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .process(new TicketProcessFunction());

        environment.execute("ticket");
    }

    public static void eventTimeStream() throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // 基于 event time 计算 watermark 推动窗口时必需指定流使用 event time
        WatermarkStrategy<TicketDTO> strategy = WatermarkStrategy
            .<TicketDTO>forBoundedOutOfOrderness(Duration.ofSeconds(20))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<TicketDTO> tickets = environment.addSource(new FlinkKafkaConsumer<>("ticket",
            new KafkaTicketDecoder(), KafkaUtil.kafkaConfigs()).assignTimestampsAndWatermarks(strategy));

        OutputTag<TicketDTO> sideOutput = new OutputTag<TicketDTO>("side") {
        };

        tickets
            .process(new PrintProcessFunction())
            .keyBy(TicketDTO::getUser)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .process(new TicketProcessFunction());

        environment.execute("ticket");
    }

}
