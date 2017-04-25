package com.hurence.logisland.serializer;

public class DeserializationException extends RuntimeException {

    public DeserializationException(final Throwable cause) {
        super(cause);
    }

    public DeserializationException(final String message) {
        super(message);
    }

    public DeserializationException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
