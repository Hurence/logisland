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
package com.caseystella.analytics.extractor;

import com.caseystella.analytics.DataPoint;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;

public class ExtractorsTest {

    /**
     * {
     * "keyConverter" : "NOOP"
     * , "valueConverter" : "CSVConverter"
     * , "valueConverterConfig" : {
     * "columnMap" : {
     * "sensor1_ts" : 0
     * ,"sensor1_value" : 1
     * ,"sensor2_ts" : 4
     * ,"sensor2_value" : 5
     * ,"plant_id" : 7
     * }
     * }
     * , "measurements" : [
     * {
     * "source" : "sensor_1"
     * ,"timestampField" : "sensor1_ts"
     * ,"measurementField" : "sensor1_value"
     * ,"metadataFields" : [ "plant_id"]
     * }
     * ,{
     * "source" : "sensor_2"
     * ,"timestampField" : "sensor2_ts"
     * ,"measurementField" : "sensor2_value"
     * ,"metadataFields" : [ "plant_id"]
     * }
     * ]
     * }
     */
    public static String extractorConfig = "{\n" +
            "      \"keyConverter\" : \"NOOP\"\n" +
            "      , \"valueConverter\" : \"CSVConverter\"\n" +
            " , \"valueConverterConfig\" : {\n" +
            " \"columnMap\" : {\n" +
            " \"sensor1_ts\" : 0\n" +
            " ,\"sensor1_value\" : 1\n" +
            " ,\"sensor2_ts\" : 4\n" +
            " ,\"sensor2_value\" : 5\n" +
            " ,\"plant_id\" : 7\n" +
            " }\n" +
            " }\n" +
            " , \"measurements\" : [\n" +
            " {\n" +
            " \"source\" : \"sensor_1\"\n" +
            " ,\"timestampField\" : \"sensor1_ts\"\n" +
            " ,\"measurementField\" : \"sensor1_value\"\n" +
            " ,\"metadataFields\" : [ \"plant_id\"]\n" +
            " }\n" +
            " ,{\n" +
            " \"source\" : \"sensor_2\"\n" +
            " ,\"timestampField\" : \"sensor2_ts\"\n" +
            " ,\"measurementField\" : \"sensor2_value\"\n" +
            " ,\"metadataFields\" : [ \"plant_id\"]\n" +
            " }\n" +
            " ]\n" +
            " }";

    @Test
    public void testExtractor() throws Exception {
        Assert.assertNotNull(extractorConfig);
        DataPointExtractorConfig config = DataPointExtractorConfig.load(extractorConfig);
        DataPointExtractor extractor = new DataPointExtractor().withConfig(config);
        {
            Iterable<DataPoint> dataPoints = extractor.extract(Longs.toByteArray(0L), "   #0,100,foo,bar,50,7,grok,plant_1,baz".getBytes(), true);
            Assert.assertEquals(0, Iterables.size(dataPoints));
        }
        {
            Iterable<DataPoint> dataPoints = extractor.extract(Longs.toByteArray(0L), "0,100,foo,bar,50,7,grok,plant_1,baz".getBytes(), true);
            Assert.assertEquals(2, Iterables.size(dataPoints));
            {
                DataPoint dp = Iterables.getFirst(dataPoints, null);
                Assert.assertNotNull(dp);
                Assert.assertEquals(dp.getSource(), "sensor_1");
                Assert.assertEquals(dp.getTimestamp(), 0L);
                Assert.assertEquals(dp.getValue(), 100d, 0.000001d);
                Assert.assertEquals(dp.getMetadata().size(), 1);
                Assert.assertEquals(dp.getMetadata().get("plant_id"), "plant_1");
            }
            {
                DataPoint dp = Iterables.getLast(dataPoints, null);
                Assert.assertNotNull(dp);
                Assert.assertEquals(dp.getSource(), "sensor_2");
                Assert.assertEquals(dp.getTimestamp(), 50L);
                Assert.assertEquals(dp.getValue(), 7d, 0.000001d);
                Assert.assertEquals(dp.getMetadata().size(), 1);
                Assert.assertEquals(dp.getMetadata().get("plant_id"), "plant_1");
            }
        }
    }

    /**
     * 
     */
    public static String fraudExtractorConfig = "{\n" +
            " \"valueConverter\" : \"CSVConverter\"\n" +
            " , \"valueConverterConfig\" : {\n" +
            " \"columnMap\" : {\n" +
            " \"physician_specialty\" : 1\n" +
            " ,\"transaction_date\" : 2\n" +
            " ,\"transaction_amount\" : 3\n" +
            " ,\"transaction_reason\" : 4\n" +
            " }\n" +
            " }\n" +
            " , \"measurements\" : [\n" +
            " {\n" +
            " \"sourceFields\" : [ \"physician_specialty\", \"transaction_reason\" ]\n" +
            " ,\"timestampField\" : \"transaction_date\"\n" +
            " ,\"timestampConverter\" : \"DateConverter\"\n" +
            " ,\"timestampConverterConfig\" : {\n" +
            " \"format\" : \"yyyy-MM-dd\"\n" +
            " }\n" +
            " ,\"measurementField\" : \"transaction_amount\"\n" +
            " }\n" +
            " ]\n" +
            " }";

    @Test
    public void testFraudExtractor() throws Exception {
        Assert.assertNotNull(extractorConfig);
        DataPointExtractorConfig config = DataPointExtractorConfig.load(fraudExtractorConfig);
        DataPointExtractor extractor = new DataPointExtractor().withConfig(config);
        {
            Iterable<DataPoint> dataPoints = extractor.extract(Longs.toByteArray(0L), "\"id_1\",\"optometrist\",\"2016-02-16\",\"75.00\",\"Food\"".getBytes(), true);
            Assert.assertEquals(1, Iterables.size(dataPoints));
            DataPoint dp = Iterables.get(dataPoints, 0);
            Assert.assertEquals("optometrist.Food", dp.getSource());
            Assert.assertEquals(2, dp.getMetadata().size());
            Assert.assertEquals("Food", dp.getMetadata().get("transaction_reason"));
            Assert.assertEquals("optometrist", dp.getMetadata().get("physician_specialty"));
            Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2016-02-16").getTime(), dp.getTimestamp());
            Assert.assertEquals(75, dp.getValue(), 1e-5);
        }
    }
}
