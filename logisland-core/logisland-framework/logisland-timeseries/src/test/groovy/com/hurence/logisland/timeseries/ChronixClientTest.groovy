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
package com.hurence.logisland.timeseries

import com.hurence.logisland.timeseries.converter.BinaryTimeSeries
import com.hurence.logisland.timeseries.converter.TimeSeriesConverter
import com.hurence.logisland.timeseries.streaming.StorageService
import spock.lang.Specification

/**
 * Unit test for the Chronix Client
 * @author f.lautenschlager
 */
class ChronixClientTest extends Specification {
    def "test stream"() {
        given:
        def converter = Mock(TimeSeriesConverter.class)
        def service = Mock(StorageService.class)

        def connection = Mock(Object.class)
        def query = Mock(Object.class)

        ChronixClient<BinaryTimeSeries, Object, Object> client = new ChronixClient<>(converter, service)

        when:
        client.stream(connection, query)

        then:
        1 * service.stream(_, _, _)

    }

    def "test add"() {
        given:
        def converter = Mock(TimeSeriesConverter.class)
        def service = Mock(StorageService.class)
        def connection = Mock(Object.class)

        ChronixClient client = new ChronixClient(converter, service)

        when:
        def someTimeSeries = new ArrayList()
        client.add(someTimeSeries, connection)

        then:
        1 * service.add(_, _, _)
    }
}
