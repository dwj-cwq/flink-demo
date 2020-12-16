package com.dwj.demo.task.config;

import java.util.Properties;

/**
 * @author dwj
 * @date 2020/12/16 10:08
 */
public class KafkaConfig {

    public static Properties getKafkaConfig() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //偏移量自动重置
        properties.setProperty("auto.offset.reset", "latest");
        return properties;
    }
}
