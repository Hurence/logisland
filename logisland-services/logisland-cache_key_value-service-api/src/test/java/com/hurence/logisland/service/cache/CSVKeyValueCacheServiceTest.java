/**
 * Copyright (C) 2017 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.cache;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CSVKeyValueCacheServiceTest {


    @Test
    public void testLookup() throws InitializationException, IOException {

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.setProperty(TestProcessor.CACHE_SERVICE, "lookup_service");


        File file = new File(getClass().getClassLoader().getResource("lookup.csv").getFile());
        String dbPath = file.getAbsolutePath();

        runner.setProperty(CSVKeyValueCacheService.DATABASE_FILE_PATH, dbPath);
        runner.setProperty(CSVKeyValueCacheService.CSV_FORMAT, "excel_fr");
        runner.setProperty(CSVKeyValueCacheService.FIRST_LINE_HEADER, "true");
        runner.setProperty(CSVKeyValueCacheService.ROW_KEY, "tagname");
        runner.setProperty(CSVKeyValueCacheService.ENCODING_CHARSET, "UTF-8");
        runner.setProperty(CacheService.CACHE_SIZE, "20000");

        // create the controller service and link it to the test processor
        final CSVKeyValueCacheService service = new CSVKeyValueCacheService();
        runner.addControllerService("lookup_service", service);
        runner.enableControllerService(service);

        final CSVKeyValueCacheService lookupService = runner.getProcessContext()
                .getPropertyValue(TestProcessor.CACHE_SERVICE)
                .asControllerService(CSVKeyValueCacheService.class);

        Record result = lookupService.get("D112.M_TI41.F_CV");


        // Compare maps
        assertEquals("D112.M_TI41.F_CV", result.getField("tagname").asString());
        assertEquals("Température amont Valco (F_CV)", result.getField("description").asString());
        assertEquals("DEG", result.getField("engunits").asString());
        assertEquals("03/06/2014 10:32:18", result.getField("lastmodified").asString());


        result = lookupService.get("D112.M_TI15.F_CV");
        assertEquals("D112.M_TI15.F_CV", result.getField("tagname").asString());
        assertEquals("Tra�age (F_CV)", result.getField("description").asString());
        assertEquals("DEG", result.getField("engunits").asString());
        assertEquals("03/06/2014 10:32:18", result.getField("lastmodified").asString());





    }


}
