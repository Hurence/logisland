package com.hurence.logisland.processor.networkpacket;

/**
 * Indicates an invalid pcap file format
 */
public class InvalidPCapFileException extends Exception {

    public InvalidPCapFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidPCapFileException(String message) {
        super(message);
    }

    public InvalidPCapFileException(Throwable cause) {
        super(cause);
    }

    public InvalidPCapFileException() {
        super();
    }
}
