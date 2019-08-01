/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hurence.logisland.timeseries;


import com.hurence.logisland.timeseries.converter.TimeSeriesConverter;
import com.hurence.logisland.timeseries.streaming.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * The Chronix client to stream and add time series
 *
 * @param <T> the time series type
 * @param <C> the connection class
 * @param <Q> the query class
 * @author f.lautenschlager
 */
public class ChronixClient<T, C, Q> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronixClient.class);

    private final TimeSeriesConverter<T> converter;
    private final StorageService<T, C, Q> service;


    /**
     * Creates a Chronix client.
     *
     * @param converter - the converter to handle the time series
     * @param service   - the service for accessing the storage
     */
    public ChronixClient(TimeSeriesConverter<T> converter, StorageService<T, C, Q> service) {
        this.converter = converter;
        this.service = service;
        LOGGER.debug("Creating ChronixClient with Converter {} for Storage {}", converter, service);
    }

    /**
     * Creates a stream of time series for the given query context and the connection
     *
     * @param connection - the connection to the storage
     * @param query      - the query used by the connection
     * @return a stream of time series
     */
    public Stream<T> stream(C connection, Q query) {
        LOGGER.debug("Streaming time series from {} with query {}", connection, query);
        return service.stream(converter, connection, query);
    }

    /**
     * Adds the given time series to the given connection.
     * Note that the connection is responsible for the commit.
     *
     * @param timeSeries - the time series of type <T> that should stored
     * @param connection - the connection to the server
     * @return the server status of the add command.
     */
    public boolean add(Collection<T> timeSeries, C connection) {
        LOGGER.debug("Adding {} to {}", timeSeries, connection);
        return service.add(converter, timeSeries, connection);
    }
}
