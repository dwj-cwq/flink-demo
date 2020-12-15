package task.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;

/**
 * @author dwj
 */
public class KafkaUtil {

    private KafkaProducer<String, String> kafkaProducer;

    public KafkaUtil() {
        this.kafkaProducer = new KafkaProducer<>(producerConfigs());
    }

    public void send(String topic, String value) {
        if (kafkaProducer != null) {
            this.kafkaProducer.send(new ProducerRecord<String, String>(topic, value));
            List<PartitionInfo> partitions = kafkaProducer.partitionsFor(topic);
            for (PartitionInfo p : partitions) {
                System.out.println(p);
            }
        }
    }

    private Properties producerConfigs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.90:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void close() {
        this.kafkaProducer.close();
    }

}
