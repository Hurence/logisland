package com.hurence.logisland.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class LogMessage {

    private final String message;
    private final LogLevel level;
    private final Throwable throwable;
    private final long time;

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String TO_STRING_FORMAT = "%1$s %2$s - %3$s";

    public LogMessage(final long millisSinceEpoch, final LogLevel level, final String message, final Throwable throwable) {
        this.level = level;
        this.throwable = throwable;
        this.message = message;
        this.time = millisSinceEpoch;
    }

    public long getMillisSinceEpoch() {
        return time;
    }

    public String getMessage() {
        return message;
    }

    public LogLevel getLevel() {
        return level;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public String toString() {
        final DateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT, Locale.US);
        final String formattedTime = dateFormat.format(new Date(time));

        String formattedMsg = String.format(TO_STRING_FORMAT, formattedTime, level.toString(), message);
        if (throwable != null) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            formattedMsg += "\n" + sw.toString();
        }

        return formattedMsg;
    }
}
