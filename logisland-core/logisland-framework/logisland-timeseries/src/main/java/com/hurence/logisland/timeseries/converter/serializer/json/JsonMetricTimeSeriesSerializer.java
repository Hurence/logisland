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
package com.hurence.logisland.timeseries.converter.serializer.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.hurence.logisland.timeseries.MetricTimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * The json serializer for the json metric simple time series
 *
 * @author f.lautenschlager
 */
public class JsonMetricTimeSeriesSerializer {

    public static final String UTF_8 = "UTF-8";

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMetricTimeSeriesSerializer.class);
    private static final byte[] EMPTY_JSON = "[[],[]]".getBytes(Charset.forName(UTF_8));
    private final Gson gson;

    /**
     * Constructs a new JsonMetricTimeSeriesSerializer.
     */
    public JsonMetricTimeSeriesSerializer() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gson = gsonBuilder.create();
    }

    /**
     * Serializes the collection of metric data points to json
     *
     * @param timeSeries -  the time series whose points should be serialized.
     * @return a json serialized collection of metric data points
     */
    public byte[] toJson(MetricTimeSeries timeSeries) {

        if (!timeSeries.isEmpty()) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                JsonWriter writer = new JsonWriter(new OutputStreamWriter(baos, UTF_8));

                List[] data = new List[]{new ArrayList<>(timeSeries.size()), new ArrayList<>(timeSeries.size())};

                for (int i = 0; i < timeSeries.size(); i++) {
                    data[0].add(timeSeries.getTime(i));
                    data[1].add(timeSeries.getValue(i));
                }
                gson.toJson(data, List[].class, writer);
                writer.close();
                baos.flush();

                return baos.toByteArray();
            } catch (IOException e) {
                LOGGER.error("Could not serialize data to json", e);

            }
        }
        return EMPTY_JSON;
    }

    /**
     * Deserialize the given json to a collection of metric data points
     *
     * @param json       the json representation of collection holding metric data points
     * @param queryStart the start of the query
     * @param queryEnd   the end of the query
     * @param builder    the builder for the time series
     */
    public void fromJson(byte[] json, final long queryStart, final long queryEnd, MetricTimeSeries.Builder builder) {
        if (queryStart <= 0 && queryEnd <= 0) {
            return;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(json);
            JsonReader reader = new JsonReader(new InputStreamReader(bais, UTF_8));
            List[] timestampsValues = gson.fromJson(reader, List[].class);
            reader.close();

            List<Double> times = (List<Double>) timestampsValues[0];
            List<Double> values = (List<Double>) timestampsValues[1];


            for (int i = 0; i < times.size(); i++) {
                if (times.get(i) > queryEnd) {
                    break;
                }

                if (times.get(i) >= queryStart && times.get(i) <= queryEnd) {
                    builder.point(times.get(i).longValue(), values.get(i));
                }
            }

        } catch (IOException | JsonSyntaxException | JsonIOException e) {
            LOGGER.error("Could not deserialize json data. Returning empty lists.", e);
        }

    }

}
