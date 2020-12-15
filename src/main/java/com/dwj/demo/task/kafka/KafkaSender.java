package com.dwj.demo.task.kafka;

import com.bizseer.xts.task.dto.Alert;
import com.bizseer.xts.task.util.KafkaUtil;
import com.bizseer.xts.task.util.SerializableUtil;

/**
 * @author dwj
 * @date 2020/12/2 16:54
 */
public class KafkaSender {

    private static final String TOPIC = "alert-out";

    public static void main(String[] args) {
        KafkaUtil kafkaUtil = new KafkaUtil();
        while (true) {
            kafkaUtil.send(TOPIC, productAlert());
        }
    }

    private static String productAlert() {
        Alert alert = new Alert().setId("test")
                .setName("test")
                .setTags(new Alert.Tags().setApp("sj")
                        .setService("zhifu").setProcess("ngnix"));
        return SerializableUtil.obj2json(alert);
    }
}
