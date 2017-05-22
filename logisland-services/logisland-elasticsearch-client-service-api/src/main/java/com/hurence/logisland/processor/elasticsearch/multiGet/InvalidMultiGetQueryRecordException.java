package com.hurence.logisland.processor.elasticsearch.multiGet;

/**
 * Indicates an invalid MultiGetQueryRecord
 */
public class InvalidMultiGetQueryRecordException extends Exception {

    public InvalidMultiGetQueryRecordException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMultiGetQueryRecordException(String message) {
        super(message);
    }

    public InvalidMultiGetQueryRecordException(Throwable cause) {
        super(cause);
    }

    public InvalidMultiGetQueryRecordException() {
        super();
    }
}
