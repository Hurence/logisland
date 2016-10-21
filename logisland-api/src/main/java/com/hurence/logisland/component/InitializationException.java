package com.hurence.logisland.component;

public class InitializationException extends Exception {

    public InitializationException(String explanation) {
        super(explanation);
    }

    public InitializationException(Throwable cause) {
        super(cause);
    }

    public InitializationException(String explanation, Throwable cause) {
        super(explanation, cause);
    }
}
