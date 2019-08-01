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
package com.hurence.logisland.timeseries.converter;

import com.hurence.logisland.timeseries.GenericTimeSeries;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import com.hurence.logisland.timeseries.dts.Pair;
import com.hurence.logisland.record.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Generic time series converter to convert our time series into a binary storage time series and back
 *
 * @author f.lautenschlager
 */
public class GenericTimeSeriesConverter implements TimeSeriesConverter<GenericTimeSeries<Long, Double>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericTimeSeriesConverter.class);

    @Override
    public GenericTimeSeries<Long, Double> from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {

        //This is a hack
        MetricTimeSeries metricTimeSeries = new MetricTimeSeriesConverter().from(binaryTimeSeries, queryStart, queryEnd);
        GenericTimeSeries<Long, Double> genericTimeSeries = new GenericTimeSeries<>(map(metricTimeSeries.points()));
        metricTimeSeries.getAttributesReference().forEach(genericTimeSeries::addAttribute);

        return genericTimeSeries;
    }

    private Iterator<Pair<Long, Double>> map(Stream<Point> points) {
        return points.map(point -> Pair.pairOf(point.getTimestamp(), point.getValue())).iterator();
    }


    @Override
    public BinaryTimeSeries to(GenericTimeSeries<Long, Double> genericTimeSeries) {

        //-oo is represented through the first element that is null, hence if the size is one the time series is empty
        if (genericTimeSeries.size() == 1) {
            LOGGER.info("Empty time series detected. {}", genericTimeSeries);
            //Create a builder with the minimal required fields
            BinaryTimeSeries.Builder builder = new BinaryTimeSeries.Builder()
                    .data(new byte[]{})
                    .start(0)
                    .end(0);
            return builder.build();
        } else {
            return new MetricTimeSeriesConverter().to(map(genericTimeSeries));
        }
    }

    private MetricTimeSeries map(GenericTimeSeries<Long, Double> genericTimeSeries) {
        MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(
                genericTimeSeries.getAttribute("name").toString(),
                genericTimeSeries.getAttribute("type").toString());


        //add points
        Iterator<Pair<Long, Double>> it = genericTimeSeries.iterator();

        //ignore the first element
        if (it.hasNext()) {
            it.next();
        }

        while (it.hasNext()) {
            Pair<Long, Double> pair = it.next();
            builder.point(pair.getFirst(), pair.getSecond());
        }

        //add attributes
        genericTimeSeries.getAttributes().forEachRemaining(attribute -> builder.attribute(attribute.getKey(), attribute.getValue()));

        return builder.build();
    }
}
