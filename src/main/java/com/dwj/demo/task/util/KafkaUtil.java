package com.dwj.demo.task.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

/**
 * @author dwj
 */
public class KafkaUtil {

    private final Producer<String, String> kafkaProducer;

    private static String group = "cwq-test";

    public KafkaUtil() {
        this.kafkaProducer = new KafkaProducer<String, String>(producerConfigs());
    }

    public void send(String topic, String value) {
        if (kafkaProducer != null) {
            this.kafkaProducer.send(new ProducerRecord<String, String>(topic, value));
//            List<PartitionInfo> partitions = kafkaProducer.partitionsFor(topic);
//            for (PartitionInfo p : partitions) {
//                System.out.println(p);
//            }
        }
    }

    private Properties producerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.90.86:9092,10.0.90.86:9093,10.0.90.86:9094");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-sec\";");
        return props;
    }

    public void close() {
        this.kafkaProducer.close();
    }

    public static Properties kafkaConfigs() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.90.86:9092,10.0.90.86:9093,10.0.90.86:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-sec\";");
        return props;
    }

}
