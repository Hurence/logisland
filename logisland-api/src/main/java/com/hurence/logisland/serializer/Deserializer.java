package com.hurence.logisland.serializer;


import java.io.IOException;

/**
 * Provides an interface for deserializing an array of bytes into an Object
 *
 * @param <T> type
 */
public interface Deserializer<T> {

    /**
     * Deserializes the given byte array input an Object and returns that value.
     *
     * @param input input
     * @return returns deserialized value
     * @throws DeserializationException if a valid object cannot be deserialized
     * from the given byte array
     * @throws IOException ex
     */
    T deserialize(byte[] input) throws DeserializationException, IOException;

}
