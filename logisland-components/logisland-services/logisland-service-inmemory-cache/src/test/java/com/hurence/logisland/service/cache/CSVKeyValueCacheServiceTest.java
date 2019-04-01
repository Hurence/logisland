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
package com.hurence.logisland.service.cache;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CSVKeyValueCacheServiceTest {


    @Test
    public void testLookup() throws InitializationException {

        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());
        runner.setProperty(TestProcessor.CACHE_SERVICE, "lookup_service");


        File file = new File(getClass().getClassLoader().getResource("lookup.csv").getFile());
        String dbPath = file.getAbsolutePath();

        // create the controller service and link it to the test processor
        final CSVKeyValueCacheService service = new CSVKeyValueCacheService();
        runner.addControllerService("lookup_service", service);
        runner.setProperty(service, CSVKeyValueCacheService.DATABASE_FILE_PATH, dbPath);
        runner.setProperty(service, CSVKeyValueCacheService.CSV_FORMAT, "excel_fr");
        runner.setProperty(service, CSVKeyValueCacheService.FIRST_LINE_HEADER, "true");
        runner.setProperty(service, CSVKeyValueCacheService.ROW_KEY, "tagname");
        runner.setProperty(service, CSVKeyValueCacheService.ENCODING_CHARSET, "ISO-8859-1");
        runner.setProperty(service, CacheService.CACHE_SIZE, "20000");

        runner.enableControllerService(service);

        final CSVKeyValueCacheService lookupService = PluginProxy.unwrap(
                runner.getProcessContext().getPropertyValue(TestProcessor.CACHE_SERVICE).asControllerService()
        );

        Record result = lookupService.get("D112.M_TI41.F_CV");


        // Compare maps
        assertEquals("D112.M_TI41.F_CV", result.getField("tagname").asString());
        assertEquals("Température amont Valco (F_CV)", result.getField("description").asString());
        assertEquals("DEG", result.getField("engunits").asString());
        assertEquals("03/06/2014 10:32:18", result.getField("lastmodified").asString());


        result = lookupService.get("D112.M_TI15.F_CV");
        assertEquals("D112.M_TI15.F_CV", result.getField("tagname").asString());
        assertEquals("Traçage (F_CV)", result.getField("description").asString());
        assertEquals("DEG", result.getField("engunits").asString());
        assertEquals("03/06/2014 10:32:18", result.getField("lastmodified").asString());


    }


}
