/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.util.validator;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.AvroSerializer;
import com.hurence.logisland.serializer.RecordSerializationException;
import com.hurence.logisland.util.runner.RecordValidator;
import org.apache.avro.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class AvroRecordValidator implements RecordValidator {

    private final AvroSerializer serializer;

    public AvroRecordValidator(Schema schema) {
        this.serializer = new AvroSerializer(schema);
    }

    public AvroRecordValidator(String strSchema) {
        this.serializer = new AvroSerializer(strSchema);
    }
    public AvroRecordValidator(InputStream inputStream) {
        this.serializer = new AvroSerializer(inputStream);
    }

    @Override
    public void assertRecord(Record record) {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(baos, record);
            baos.close();
            assertTrue(true);
        } catch (RecordSerializationException e) {
            e.printStackTrace();
            assertFalse(true);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
