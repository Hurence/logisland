/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.timeseries.converter;

import java.io.Serializable;

/**
 * Defines how a custom time series of type <T> is converted from a binary time series and back
 *
 * @param <T> type of the document returned by the converter
 * @author f.lautenschlager
 */
public interface TimeSeriesConverter<T> extends Serializable {

    /**
     * Shall create an object of type T from the given time series document.
     * <p>
     * The time series contains all fields.
     * This method is executed in worker thread and should handle the transformation into
     * a user custom time series object.
     *
     * @param binaryTimeSeries - the time series document containing all stored fields and values
     * @param queryStart       - the start of the query
     * @param queryEnd         - the end of the query
     * @return a concrete object of type T
     */
    T from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd);

    /**
     * Shall do the conversation of the custom time series T into the binary time series that is stored.
     *
     * @param document - the custom time series with all fields
     * @return the time series document that is stored
     */
    BinaryTimeSeries to(T document);

}
