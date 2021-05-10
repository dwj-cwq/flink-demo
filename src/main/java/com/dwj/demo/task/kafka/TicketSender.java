package com.dwj.demo.task.kafka;

import com.dwj.demo.task.dto.TicketDTO;
import com.dwj.demo.task.util.KafkaUtil;
import com.dwj.demo.task.util.SerializableUtil;

/**
 * @author Chen Wenqun
 */
public class TicketSender {

    private static final String TOPIC = "ticket";

    public static void main(String[] args) throws InterruptedException {
        KafkaUtil kafkaUtil = new KafkaUtil();
        long id = 0;
        while (true) {
            System.out.println(String.format("start send ticket: %d", id));
            kafkaUtil.send(TOPIC, productTicket(id));
            System.out.println(String.format("success send ticket: %d", id));
            id++;
            Thread.sleep(1000);
        }
    }

    private static String productTicket(long id) {
        TicketDTO ticket = new TicketDTO();
        ticket.setId(id);
        ticket.setTimestamp(System.currentTimeMillis());
        ticket.setPrice(Math.random() * 1000);
        ticket.setUser("user" + id % 2);
        return SerializableUtil.obj2json(ticket);
    }
}
