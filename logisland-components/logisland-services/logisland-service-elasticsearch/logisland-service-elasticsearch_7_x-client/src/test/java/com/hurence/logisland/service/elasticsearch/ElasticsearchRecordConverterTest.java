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
package com.hurence.logisland.service.elasticsearch;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ElasticsearchRecordConverterTest {

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchRecordConverterTest.class);

    @Test
    public void testBasics() throws Exception {
        Record record = new StandardRecord("factory")
                .setId("Modane")
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 41.4f);

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);

        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);

        // Verify the index does not exist
        Assert.assertEquals(true, convertedRecord.contains("location"));
    }

    @Test
    public void testArraySimple() throws Exception {
        Record record = new StandardRecord("test")
                .setId("simple array")
                .setStringField("address", "rue du Frejus")
                .setField("stringArray", FieldType.ARRAY, new String[]{"hi", "han"})
                .setField("intArray", FieldType.ARRAY, new int[]{1, 2})
                .setField("longArray", FieldType.ARRAY, new long[]{1, 2})
                .setField("doubleArray", FieldType.ARRAY, new double[]{1.12, 2.12})
                .setField("floatArray", FieldType.ARRAY, new float[]{1.12f, 2.12f})
                .setField("byteArray", FieldType.ARRAY, new byte[]{45, 35});

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);
        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);
    }


    @Test
    public void testMap() throws Exception {
        Map<String, Object> map = new HashedMap();
        map.put("key1", "value1");
        map.put("key2", "value2");
        Map<String, Object> nestedMap = new HashedMap();
        nestedMap.put("key1", "value1");
        nestedMap.put("key2", map);

        Record record = new StandardRecord("test")
                .setId("simple array")
                .setStringField("address", "rue du Frejus")
                .setField("simpleMap", FieldType.MAP, map)
                .setField("nestedMap", FieldType.MAP, nestedMap);

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);
        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);
    }

    @Test
    public void testObject() throws Exception {
        //TODO currently not working but not really necessary is it ?
        Object object = new ElasticsearchRecordConverter();

        Record record = new StandardRecord("test")
                .setId("simple array")
                .setStringField("address", "rue du Frejus")
                .setField("Object", FieldType.OBJECT, object);

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);
        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);
    }

    @Test
    public void testRecord() throws Exception {
        Record nestedRecord = new StandardRecord("factory")
                .setId("Modane")
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 41.4f);

        Record nestedNestedRecord = new StandardRecord("factory")
                .setId("Modane")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.RECORD, nestedRecord);

        Record record = new StandardRecord("test")
                .setId("simple array")
                .setStringField("address", "rue du Frejus")
                .setField("Record", FieldType.RECORD, nestedRecord)
                .setField("RecordNested", FieldType.RECORD, nestedNestedRecord);

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);
        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);
    }

    @Test
    public void testMapArray() throws Exception {
        Map<String, Object> map = new HashedMap();
        map.put("key1", "value1");
        map.put("key2", "value2");
        Map<String, Object> nestedMap = new HashedMap();
        nestedMap.put("key1", "value1");
        nestedMap.put("key2", map);
        Map<String, Object> nestedMap2 = new HashedMap();
        nestedMap.put("key12", "value2");
        nestedMap.put("key2", map);
        Record record = new StandardRecord("test")
                .setId("simple array")
                .setStringField("address", "rue du Frejus")
                .setField("MapArray", FieldType.ARRAY, new Map[]{nestedMap, nestedMap2});

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);
        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);
    }

    @Test
    public void testRecordArray() throws Exception {
        Record nestedRecord = new StandardRecord("factory")
                .setId("Modane")
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 41.4f);

        Record nestedNestedRecord = new StandardRecord("factory")
                .setId("Modane")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("nestedRecord", FieldType.RECORD, nestedRecord);

        Record nestedNestedRecord2 = new StandardRecord("factory")
                .setId("Modane")
                .setField("latitude2", FieldType.FLOAT, 45.4f)
                .setField("nestedRecord2", FieldType.RECORD, nestedRecord);

        Record record = new StandardRecord("test")
                .setId("simple array")
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 41.4f)
                .setField("RecordArray", FieldType.ARRAY, new Record[]{nestedNestedRecord, nestedNestedRecord2});

        String  convertedRecord = ElasticsearchRecordConverter.convertToString(record);
        logger.info("record is '{}'", record);
        logger.info("convertedRecord is '{}'", convertedRecord);
    }

//    @Test
//    public void aaa() throws IOException {
//        Record record = new StandardRecord("test")
//                .setId("simple array")
//                .setStringField("address", "rue du Frejus")
//                .setField("MapArray", FieldType.ARRAY, new String[]{"hi", "han"})
//                .setField("RecordArray", FieldType.ARRAY, new String[]{"hi", "han"});
//
//        final XContentBuilder document = jsonBuilder();
//        document.startObject();
//        document.field("hi", "han");
//        document.field("record", record);
//        document.endObject();
//        String result = Strings.toString(document);
//        document.flush();
//        logger.info("convertedRecord is '{}'", result);
//    }
}

