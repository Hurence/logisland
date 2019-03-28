/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ExpandMapFieldsTest {

    private Record createTestRecord() {
        Map<String, Object> map = new HashMap<>();
        map.put("f1", new Date());
        map.put("f2", 30L);
        map.put("f3", Arrays.asList(1, 2, 3, 4));
        map.put("f4", new HashMap<>());
        map.put("f5", "Hello world");
        Record ret = new StandardRecord("test");
        ret.setField("f2", FieldType.STRING, "I will survive");
        ret.setField("map", FieldType.MAP, map);
        return ret;
    }

    @Test
    public void testOverwriteValues() {
        TestRunner runner = TestRunners.newTestRunner(new ExpandMapFields());
        runner.setProperty(ExpandMapFields.CONFLICT_RESOLUTION_POLICY, ExpandMapFields.OVERWRITE_EXISTING);
        runner.setProperty(ExpandMapFields.FIELDS_TO_EXPAND, "map");
        runner.assertValid();
        runner.enqueue(createTestRecord());
        runner.run();
        runner.assertOutputRecordsCount(1);
        runner.assertAllOutputRecords(record -> {
            Assert.assertFalse(record.hasField("map"));
            Assert.assertTrue(record.hasField("f1"));
            Assert.assertTrue(record.hasField("f2"));
            Assert.assertTrue(record.hasField("f3"));
            Assert.assertTrue(record.hasField("f4"));
            Assert.assertTrue(record.hasField("f5"));
            Assert.assertEquals(record.getField("f1").getType(), FieldType.DATETIME);
            Assert.assertEquals(record.getField("f2").getType(), FieldType.LONG);
            Assert.assertEquals(record.getField("f3").getType(), FieldType.ARRAY);
            Assert.assertEquals(record.getField("f4").getType(), FieldType.MAP);
            Assert.assertEquals(record.getField("f5").getType(), FieldType.STRING);
            Assert.assertEquals(record.getField("f5").getRawValue(), "Hello world");
        });
    }


    @Test
    public void testKeepOldValues() {
        TestRunner runner = TestRunners.newTestRunner(new ExpandMapFields());
        runner.setProperty(ExpandMapFields.CONFLICT_RESOLUTION_POLICY, ExpandMapFields.KEEP_OLD_FIELD);
        runner.setProperty(ExpandMapFields.FIELDS_TO_EXPAND, "map");
        runner.assertValid();
        runner.enqueue(createTestRecord());
        runner.run();
        runner.assertOutputRecordsCount(1);
        runner.assertAllOutputRecords(record -> {
            Assert.assertFalse(record.hasField("map"));
            Assert.assertTrue(record.hasField("f1"));
            Assert.assertTrue(record.hasField("f2"));
            Assert.assertTrue(record.hasField("f3"));
            Assert.assertTrue(record.hasField("f4"));
            Assert.assertTrue(record.hasField("f5"));
            Assert.assertEquals(record.getField("f2").getRawValue(), "I will survive");
        });
    }

}