package com.hurence.logisland.processor;


import com.hurence.logisland.record.StandardRecord;

import java.util.Collection;

/**
 * Exception indicating that a failure or early completion condition was
 * detected in an event processing.
 *
 * @author Tom Bailet
 *
 */
public class ProcessException extends java.lang.RuntimeException {

    private final Collection<StandardRecord> errorRecords;

    public Collection<StandardRecord> getErrorRecords() {
        return errorRecords;
    }

    /**
     * Create a new {@link ProcessException} based on a collection of records
     *
     * @param errorRecords the error records for this exception
     */
    public ProcessException(Collection<StandardRecord> errorRecords) {
        this.errorRecords = errorRecords;
    }

}
