package com.dwj.demo.task.decoder;

import com.dwj.demo.task.dto.Alert;
import com.dwj.demo.task.util.SerializableUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @author dwj
 * @date 2020/11/21 20:01
 */
public class KafkaAlertCoder implements DeserializationSchema<Alert>, SerializationSchema<Alert> {

    @Override
    public Alert deserialize(byte[] bytes) {
        return SerializableUtil.readFromByteArray(bytes, Alert.class);
    }

    @Override
    public boolean isEndOfStream(Alert alertDTO) {
        return false;
    }

    @Override
    public byte[] serialize(Alert alert) {
        return SerializableUtil.objectWriteToByteArray(alert);
    }

    @Override
    public TypeInformation<Alert> getProducedType() {
        return null;
    }
}
