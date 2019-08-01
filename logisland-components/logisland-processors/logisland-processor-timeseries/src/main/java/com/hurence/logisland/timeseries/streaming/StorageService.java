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
package com.hurence.logisland.timeseries.streaming;

import com.hurence.logisland.timeseries.converter.TimeSeriesConverter;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * The storage access to stream and add time series
 *
 * @param <T> - the type of the time series returned or added
 * @param <C> - the connection type
 * @param <Q> - the query type used by the connection
 * @author f.lautenschlager
 */
public interface StorageService<T, C, Q> {
    /**
     * Streams time series of type <T> from the given connection using the given query.
     *
     * @param converter  - defines how the time series of type <T> are created
     * @param connection - the connection to the storage
     * @param query      - the query that describe the result
     * @return an iterator on the result set
     */
    Stream<T> stream(TimeSeriesConverter<T> converter, C connection, Q query);

    /**
     * Adds the given collection of time series to the storage
     *
     * @param converter  - the converter for the time series
     * @param documents  - the time series added to the storage
     * @param connection - the connection to the storage
     * @return true if the time series are added correctly, otherwise false
     */
    boolean add(TimeSeriesConverter<T> converter, Collection<T> documents, C connection);


}
