package com.dwj.demo.task.decoder;

import com.dwj.demo.task.dto.TicketDTO;
import com.dwj.demo.task.util.SerializableUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * @author Chen Wenqun
 */
public class KafkaTicketDecoder implements DeserializationSchema<TicketDTO>, SerializationSchema<TicketDTO> {

    @Override
    public TicketDTO deserialize(byte[] bytes) {
        return SerializableUtil.readFromByteArray(bytes, TicketDTO.class);
    }

    @Override
    public boolean isEndOfStream(TicketDTO alertDTO) {
        return false;
    }

    @Override
    public byte[] serialize(TicketDTO alert) {
        return SerializableUtil.objectWriteToByteArray(alert);
    }

    @Override
    public TypeInformation<TicketDTO> getProducedType() {
        return TypeInformation.of(TicketDTO.class);
    }
}

