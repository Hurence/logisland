package com.hurence.logisland.connect.spooldir.modele;

import java.io.File;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedFile implements Delayed {

    private final File failedFile;
    private final long timeDelayOk;

    public DelayedFile(File failedFile, long timestampOfError, long delay, TimeUnit unit) {
        this.failedFile = failedFile;
        this.timeDelayOk = timestampOfError + unit.toMillis(delay);
    }

    public DelayedFile(File failedFile, long delay, TimeUnit unit) {
        this(failedFile, System.currentTimeMillis(), delay, unit);
    }

    public DelayedFile(File failedFile, long delay) {
        this(failedFile, System.currentTimeMillis(), delay, TimeUnit.SECONDS);
    }

    public DelayedFile(File failedFile) {
        this(failedFile, System.currentTimeMillis(), 10, TimeUnit.SECONDS);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long currentDate = System.currentTimeMillis();
        long delay = timeDelayOk - currentDate;
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.timeDelayOk, ((DelayedFile) o).getTimeDelayOk());
    }

    public File getFailedFile() {
        return failedFile;
    }

    public long getTimeDelayOk() {
        return timeDelayOk;
    }
}
