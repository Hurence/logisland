package com.hurence.logisland.serializer;

public class SerializationException extends RuntimeException {

    public SerializationException(final Throwable cause) {
        super(cause);
    }

    public SerializationException(final String message) {
        super(message);
    }

    public SerializationException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
