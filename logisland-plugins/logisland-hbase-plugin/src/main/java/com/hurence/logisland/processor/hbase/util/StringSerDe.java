
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
