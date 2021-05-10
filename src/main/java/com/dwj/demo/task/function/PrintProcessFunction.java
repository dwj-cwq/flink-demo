package com.dwj.demo.task.function;

import com.dwj.demo.task.dto.TicketDTO;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Chen Wenqun
 */
public class PrintProcessFunction extends ProcessFunction<TicketDTO, TicketDTO> {

    @Override
    public void processElement(TicketDTO value, Context ctx, Collector<TicketDTO> out) {
        System.out.println(String.format("process ticket: id:%d, user:%s, price:%f, time:%d", value.getId(), value.getUser(), value.getPrice(), value.getTimestamp()));
        out.collect(value);
    }
}
