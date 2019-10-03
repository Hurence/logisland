package com.hurence.logisland.error;

import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.record.Record;

public class ErrorUtils {
    static public void handleError(final ComponentLog logger, final Throwable ex, final Record record, String errorType) {
        StringBuilder stb = new StringBuilder(ex.toString());
        for (Throwable exception = ex.getCause(); exception != null; exception = exception.getCause()) {
            stb.append(System.getProperty("line.separator"));
            stb.append(exception.toString());
        }
        record.addError(errorType, logger, stb.toString());
    }
}
