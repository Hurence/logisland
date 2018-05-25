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
package com.hurence.logisland.processor.hbase.util;


import com.hurence.logisland.serializer.DeserializationException;
import com.hurence.logisland.serializer.Deserializer;
import com.hurence.logisland.serializer.SerializationException;
import com.hurence.logisland.serializer.Serializer;
import org.apache.commons.io.IOUtils;

import java.io.*;

public class ObjectSerDe implements Serializer<Object>, Deserializer<Object> {

    @Override
    public Object deserialize(InputStream is) throws DeserializationException, IOException {
        if (is == null) {
            return null;
        }

        byte[] input = IOUtils.toByteArray(is);
        if (input == null || input.length == 0) {
            return null;
        }

        try (final ByteArrayInputStream in = new ByteArrayInputStream(input);
             final ObjectInputStream objIn = new ObjectInputStream(in)) {
            return objIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new DeserializationException("Could not deserialize object due to ClassNotFoundException", e);
        }
    }

    @Override
    public void serialize(OutputStream output, Object value) throws SerializationException, IOException {
        try (final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
             final ObjectOutputStream objOut = new ObjectOutputStream(bOut)) {
            objOut.writeObject(value);
            output.write(bOut.toByteArray());
        }
    }

}
