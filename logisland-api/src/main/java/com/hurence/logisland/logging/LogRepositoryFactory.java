package com.hurence.logisland.logging;

import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
public class LogRepositoryFactory {

    public static final String LOG_REPOSITORY_CLASS_NAME = "com.hurence.logisland.logging.StandardLogRepository";

    private static final ConcurrentMap<String, LogRepository> repositoryMap = new ConcurrentHashMap<>();
    private static final Class<LogRepository> logRepositoryClass;

    static {
        Class<LogRepository> clazz = null;
        try {
            clazz = (Class<LogRepository>) Class.forName(LOG_REPOSITORY_CLASS_NAME, true, LogRepositoryFactory.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            LoggerFactory.getLogger(LogRepositoryFactory.class).error("Unable to find class {}; logging may not work properly", LOG_REPOSITORY_CLASS_NAME);
        }
        logRepositoryClass = clazz;
    }

    public static LogRepository getRepository(final String componentId) {
        LogRepository repository = repositoryMap.get(requireNonNull(componentId));
        if (repository == null) {
            try {
                repository = logRepositoryClass.newInstance();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }

            final LogRepository oldRepository = repositoryMap.putIfAbsent(componentId, repository);
            if (oldRepository != null) {
                repository = oldRepository;
            }
        }

        return repository;
    }
}
