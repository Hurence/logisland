package com.hurence.logisland.serializer;

import com.hurence.logisland.record.Record;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;


public class KafkaRecordSerializer implements Serializer<Record> {

    private static Logger logger = LoggerFactory.getLogger(KafkaRecordSerializer.class);

    private ExtendedJsonSerializer recordSerializer = new ExtendedJsonSerializer();
    private ByteArrayOutputStream out;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        out = new ByteArrayOutputStream();
    }

    @Override
    public byte[] serialize(String s, Record record) {
        recordSerializer.serialize(out, record);
        return out.toByteArray();
    }

    @Override
    public void close() {
        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                logger.error("error while closing stream", e);
            }
        }
    }
}
