/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
