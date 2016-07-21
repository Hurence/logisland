package com.hurence.logisland.processor;


/**
 * Exception indicating that a failure or early completion condition was
 * detected in an event processing.
 *
 * @author Tom Bailet
 *
 */
public class ProcessException extends java.lang.RuntimeException {

    public ProcessException() {
    }

    public ProcessException(Throwable cause) {
        super(cause);
    }

    /**
     * Create a new {@link ProcessException} based on a message and
     * another exception.
     *
     * @param message
     *            the message for this exception
     * @param cause
     *            the other exception
     */
    public ProcessException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a new {@link ProcessException} based on a message.
     *
     * @param message
     *            the message for this exception
     */
    public ProcessException(String message) {
        super(message);
    }
}
