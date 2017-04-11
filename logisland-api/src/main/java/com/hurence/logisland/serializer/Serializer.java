
package com.hurence.logisland.serializer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Provides a mechanism by which a value can be serialized to a stream of bytes
 *
 * @param <T> type to serialize
 */
public interface Serializer<T> {

    /**
     * Serializes the given value to the {@link OutputStream}
     *
     * @param value value
     * @param output stream
     * @throws SerializationException If unable to serialize the given value
     * @throws IOException ex
     */
    void serialize(T value, OutputStream output) throws SerializationException, IOException;

}
