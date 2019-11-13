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
package com.hurence.logisland.timeseries.sampling;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.record.*;

public class SamplerFactory {

    /**
     * Instanciates a sampler.
     *
     * @param algorithm the sampling algorithm
     * @param valueFieldName the name of the field containing the point value (Y)
     * @param timeFieldName the name of the field containing the point time (X)
     * @param parameter an int parameter
     * @return the sampler
     */
    public static RecordSampler getRecordSampler(SamplingAlgorithm algorithm,
                                                 String valueFieldName,
                                                 String timeFieldName,
                                                 int parameter) {

        switch (algorithm) {
            case LTTB:
                return new LTTBRecordSampler(valueFieldName, timeFieldName, parameter);
            case FIRST_ITEM:
                return new FirstItemRecordSampler(valueFieldName, timeFieldName, parameter);
            case AVERAGE:
                return new AverageRecordSampler(valueFieldName, timeFieldName, parameter);
            case MIN_MAX:
                return new MinMaxRecordSampler(valueFieldName, timeFieldName, parameter);
            case MODE_MEDIAN:
                return new ModeMedianRecordSampler(valueFieldName, timeFieldName, parameter);
            case NONE:
                return new IsoRecordSampler(valueFieldName, timeFieldName);
            default:
                throw new UnsupportedOperationException("algorithm " + algorithm.name() + " is not yet supported !");
        }
    }

    //TODO make this method generic
    //TODO Find a way to generate TimeSerieHandler with a factory, taking the class as param ?
    /**
     * Instanciates a sampler.
     *
     * @param algorithm the sampling algorithm
     * @param bucketSize an int parameter
     * @return the sampler
     */
    public static Sampler<Point> getPointSampler(SamplingAlgorithm algorithm, int bucketSize) {
        switch (algorithm) {
            case FIRST_ITEM:
                return new FirstItemSampler<Point>(bucketSize);
            case AVERAGE:
                return new AverageSampler<Point>(getPointTimeSerieHandler(), bucketSize);
            case NONE:
                return new IsoSampler<Point>();
            case MIN:
                return new MinSampler<Point>(getPointTimeSerieHandler(), bucketSize);
            case MAX:
                return new MaxSampler<Point>(getPointTimeSerieHandler(), bucketSize);
            case MIN_MAX:
            case LTTB:
            case MODE_MEDIAN:
            default:
                throw new UnsupportedOperationException("algorithm " + algorithm.name() + " is not yet supported !");

        }
    }

    public static TimeSerieHandler<Point> getPointTimeSerieHandler() {
        return new TimeSerieHandler<Point>() {
                @Override
                public Point createTimeserie(long timestamp, double value) {
                    return new Point(0, timestamp, value);
                }

                @Override
                public long getTimeserieTimestamp(Point point) {
                    return point.getTimestamp();
                }

                @Override
                public Double getTimeserieValue(Point point) {
                    return point.getValue();
                }
            };
    }
}
