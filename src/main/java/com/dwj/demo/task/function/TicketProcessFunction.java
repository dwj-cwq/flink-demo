package com.dwj.demo.task.function;

import com.dwj.demo.task.dto.TicketDTO;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Chen Wenqun
 */
public class TicketProcessFunction extends ProcessWindowFunction<TicketDTO, Tuple2<String, Double>, String, TimeWindow> {

    private transient ValueState<Tuple2<Long, Double>> sumState;

    @Override
    public void process(String s, Context context, Iterable<TicketDTO> elements, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<Long, Double> oldSumState = sumState.value();
        System.out.println(String.format("begin process window data. Now sum is: %f, count: %d", oldSumState.f1, oldSumState.f0));
        double sum = 0;
        for (TicketDTO element : elements) {
            sum = sum + element.getPrice();
            System.out.println(String.format("window:" + context.window() + "process ticket: id:%d, user:%s, price:%f, sum:%f",
                element.getId(), element.getUser(), element.getPrice(), sum));
            oldSumState.f0++;
        }
        oldSumState.f1 += sum;
        sumState.update(oldSumState);
        out.collect(new Tuple2<>(s, sum));
        System.out.println(String.format("success process window data. Now sum is: %f, count: %d", oldSumState.f1, oldSumState.f0));
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Double>> descriptor = new ValueStateDescriptor<Tuple2<Long, Double>>(
            "sum",
            TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
            }),
            Tuple2.of(0L, 0.0d)
        );
        sumState = getRuntimeContext().getState(descriptor);
    }
}
