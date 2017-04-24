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
/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.serializer;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;

public class BytesArraySerializer implements RecordSerializer {

    /* TODO */
    public void serialize(OutputStream objectDataOutput, Record record) {
        throw new RuntimeException("BytesArraySerializer serialize method not implemented yet");
    }

    public Record deserialize(InputStream objectDataInput) {
        try {
            Record record = new StandardRecord();
            byte[] bytes = IOUtils.toByteArray(objectDataInput);
            record.setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, bytes);
            return record;
        } catch (Throwable t) {
         //   t.printStackTrace();
            throw new RecordSerializationException(t.getMessage(), t.getCause());
        }
    }
}