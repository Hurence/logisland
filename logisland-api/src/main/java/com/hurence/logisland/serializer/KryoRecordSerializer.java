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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.Field;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class KryoRecordSerializer implements RecordSerializer {
    private final boolean compress;

    private static final ThreadLocal<Kryo> kryoThreadLocal
            = new ThreadLocal<Kryo>() {

        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.register(Record.class);
            kryo.register(Field.class);
            return kryo;
        }
    };

    public KryoRecordSerializer(boolean compress) {
        this.compress = compress;
    }

    public void serialize(OutputStream objectDataOutput, Record record) {
        try {
            Kryo kryo = kryoThreadLocal.get();

            if (compress) {
                Output output = null;
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(4096);

                try {
                    DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream);
                    output = new Output(deflaterOutputStream);
                    kryo.writeObject(output, record);

                } finally {
                    if (output != null) {
                        output.close();
                    }
                }

                byte[] bytes = byteArrayOutputStream.toByteArray();
                objectDataOutput.write(bytes);
            } else {
                Output output = null;
                try {
                    output = new Output(objectDataOutput);
                    kryo.writeObject(output, record);
                } finally {
                    if (output != null) {
                        output.flush();
                    }
                }
            }
        } catch (Throwable t) {
            throw new RecordSerializationException(t.getMessage(), t.getCause());
        }
    }

    public Record deserialize(InputStream objectDataInput) {
        try {
            InputStream in = objectDataInput;

            if (compress) {
                in = new InflaterInputStream(in);
            }

            Input input = new Input(in);
            Kryo kryo = kryoThreadLocal.get();

            return kryo.readObject(input, Record.class);
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RecordSerializationException(t.getMessage(), t.getCause());
        }
    }
}