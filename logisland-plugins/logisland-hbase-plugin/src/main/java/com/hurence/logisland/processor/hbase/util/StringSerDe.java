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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringSerDe implements Serializer<String>, Deserializer<String> {

    @Override
    public String deserialize(final byte[] value) throws DeserializationException, IOException {
        if ( value == null ) {
            return null;
        }

        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
    }

}
