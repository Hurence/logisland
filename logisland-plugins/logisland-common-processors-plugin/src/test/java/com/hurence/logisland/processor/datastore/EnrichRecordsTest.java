/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.datastore;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EnrichRecordsTest {

    @Test
    public void testSimpleEnrichment() throws InitializationException {


        // create the controller service and link it to the test processor
        final DatastoreClientService service = new MockDatastoreService();


        final TestRunner runner = TestRunners.newTestRunner(EnrichRecords.class);
        runner.setProperty(EnrichRecords.RECORD_KEY_FIELD, "record_id");
        runner.setProperty(EnrichRecords.COLLECTION_NAME, "test");
        runner.setProperty(EnrichRecords.DATASTORE_CLIENT_SERVICE, service.getIdentifier());
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);

        getLookupRecords().forEach(r -> service.put("test", r, false));

        final DatastoreClientService lookupService = runner.getProcessContext()
                .getPropertyValue(EnrichRecords.DATASTORE_CLIENT_SERVICE)
                .asControllerService(MockDatastoreService.class);


        Collection<Record> recordsToEnrich = getRecords();

        runner.assertValid();
        runner.enqueue(recordsToEnrich);
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(3);
        runner.assertOutputErrorCount(0);


        Record expected = getJoinedRecords().get("id1");
        Record enriched0 = runner.getOutputRecords().get(0);
        assertEquals(expected.getField("a"), enriched0.getField("a"));
        assertEquals(expected.getField("b"), enriched0.getField("b"));
        assertEquals(expected.getField("c"), enriched0.getField("c"));
        assertEquals(expected.getField("f1"), enriched0.getField("f1"));
        assertEquals(expected.getField("f2"), enriched0.getField("f2"));

    }

    private Collection<Record> getRecords() {
        Collection<Record> recordsToEnrich = new ArrayList<>();

        recordsToEnrich.add(new StandardRecord()
                .setId("id1")
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        recordsToEnrich.add(new StandardRecord()
                .setId("id2")
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        recordsToEnrich.add(new StandardRecord()
                .setId("id3")
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("c", FieldType.LONG, 3));
        return recordsToEnrich;
    }

    private Map<String, Record> getJoinedRecords() {
        Map<String, Record> lookupRecords = new HashMap<>();

        lookupRecords.put("id1", new StandardRecord()
                .setId("id1")
                .setField("f1", FieldType.STRING, "value1")
                .setField("f2", FieldType.STRING, "falue2")
                .setField("a", FieldType.STRING, "a1")
                .setField("b", FieldType.STRING, "b1")
                .setField("c", FieldType.LONG, 1));

        lookupRecords.put("id2", new StandardRecord()
                .setId("id2")
                .setField("f1", FieldType.STRING, "value3")
                .setField("f2", FieldType.STRING, "falue4")
                .setField("a", FieldType.STRING, "a2")
                .setField("b", FieldType.STRING, "b2")
                .setField("c", FieldType.LONG, 2));

        lookupRecords.put("id3", new StandardRecord()
                .setId("id3")
                .setField("f1", FieldType.STRING, "value5")
                .setField("f2", FieldType.STRING, "falue6")
                .setField("a", FieldType.STRING, "a3")
                .setField("b", FieldType.STRING, "b3")
                .setField("c", FieldType.LONG, 3));

        return lookupRecords;
    }

    private Collection<Record> getLookupRecords() {
        Collection<Record> lookupRecords = new ArrayList<>();

        lookupRecords.add(new StandardRecord()
                .setId("id1")
                .setField("f1", FieldType.STRING, "value1")
                .setField("f2", FieldType.STRING, "falue2"));

        lookupRecords.add(new StandardRecord()
                .setId("id2")
                .setField("f1", FieldType.STRING, "value3")
                .setField("f2", FieldType.STRING, "falue4"));

        lookupRecords.add(new StandardRecord()
                .setId("id3")
                .setField("f1", FieldType.STRING, "value5")
                .setField("f2", FieldType.STRING, "falue6"));

        return lookupRecords;
    }
}
