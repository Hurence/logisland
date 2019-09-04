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
package com.hurence.logisland.processor.alerting;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.datastore.MockDatastoreService;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class CheckThresholdsTest {

    @Test
    public void testMultipleRules() throws InitializationException {

        // create the controller service and link it to the test processor
        final DatastoreClientService service = new MockDatastoreService();
        getCacheRecords().forEach(r -> service.put("test", r, false));

        final TestRunner runner = TestRunners.newTestRunner(new CheckThresholds());
        runner.setProperty(ComputeTags.MAX_CPU_TIME, "100");
        runner.setProperty(ComputeTags.MAX_MEMORY, "12800000");
        runner.setProperty(ComputeTags.MAX_PREPARED_STATEMENTS, "100");
        runner.setProperty(ComputeTags.ALLOW_NO_BRACE, "false");
        runner.setProperty("tvib1","cache(\"cached_id1\").value > 10.0");
        runner.setProperty("tvib2", "cache(\"cached_id2\").value >= 0");
        runner.setProperty(ComputeTags.DATASTORE_CLIENT_SERVICE, service.getIdentifier());
        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);

        final DatastoreClientService lookupService = PluginProxy.unwrap(runner.getProcessContext()
                .getPropertyValue(ComputeTags.DATASTORE_CLIENT_SERVICE)
                .asControllerService());

        Collection<Record> recordsToEnrich = getRecords();
        runner.assertValid();
        runner.enqueue(recordsToEnrich);
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(2+3);
        runner.assertOutputErrorCount(0);

        for (Record enriched : runner.getOutputRecords()) {
            if (enriched.getId().equals("tvib1")) {
                assertEquals(enriched.getField(FieldDictionary.RECORD_VALUE).asString(), "cache(\"cached_id1\").value > 10.0");
            } else if (enriched.getId().equals("tvib2")) {
                assertEquals(enriched.getField(FieldDictionary.RECORD_VALUE).asString(), "cache(\"cached_id2\").value >= 0");
            }
        }
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


    private Collection<Record> getCacheRecords() {
        Collection<Record> lookupRecords = new ArrayList<>();

        lookupRecords.add(new StandardRecord()
                .setId("cached_id1")
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 12.45));

        lookupRecords.add(new StandardRecord()
                .setId("cached_id2")
                .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 2.5));

        return lookupRecords;
    }
}
