package com.hurence.logisland.logging;

public interface LogRepository {

    void addLogMessage(LogLevel level, String message);

    void addLogMessage(LogLevel level, String message, Throwable t);

    void addLogMessage(LogLevel level, String messageFormat, Object[] params);

    void addLogMessage(LogLevel level, String messageFormat, Object[] params, Throwable t);

    /**
     * Registers an observer so that it will be notified of all Log Messages
     * whose levels are at least equal to the given level.
     *
     * @param observerIdentifier identifier of observer
     * @param level of logs the observer wants
     * @param observer the observer
     */
    void addObserver(String observerIdentifier, LogLevel level, LogObserver observer);

    /**
     * Sets the observation level of the specified observer.
     *
     * @param observerIdentifier identifier of observer
     * @param level of logs the observer wants
     */
    void setObservationLevel(String observerIdentifier, LogLevel level);

    /**
     * Gets the observation level for the specified observer.
     *
     * @param observerIdentifier identifier of observer
     * @return level
     */
    LogLevel getObservationLevel(String observerIdentifier);

    /**
     * Removes the given LogObserver from this Repository.
     *
     * @param observerIdentifier identifier of observer
     * @return old log observer
     */
    LogObserver removeObserver(String observerIdentifier);

    /**
     * Removes all LogObservers from this Repository
     */
    void removeAllObservers();

    /**
     * Sets the current logger for the component
     *
     * @param logger the logger to use
     */
    void setLogger(ComponentLog logger);

    /**
     * @return the current logger for the component
     */
    ComponentLog getLogger();

    boolean isDebugEnabled();

    boolean isInfoEnabled();

    boolean isWarnEnabled();

    boolean isErrorEnabled();
}
