/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import de.qaware.chronix.converter.MetricTimeSeriesConverter;
import de.qaware.chronix.solr.client.ChronixSolrStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrClient;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ChronixClientServiceTest {


    private static final String CHRONIX_COLLECTION = "chronix";

    private static Logger logger = LoggerFactory.getLogger(ChronixClientServiceTest.class);

    @Rule
    public final SolrRule solrRule = new SolrRule();



    private class MockSolrClientService extends Solr_6_4_2_ChronixClientService {

        @Override
        protected void createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
            if (solr != null) {
                return;
            }
            solr = solrRule.getClient();
        }

        @Override
        protected void createChronixStorage(ControllerServiceInitializationContext context) throws ProcessException {
            if (storage != null) {
                return;
            }
            try {

                converter = new MetricTimeSeriesConverter();
                storage = new ChronixSolrStorage<>(20, groupBy, reduce);


            } catch (Exception ex) {
                logger.error(ex.toString());
            }
        }


        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> props = new ArrayList<>();

            return Collections.unmodifiableList(props);
        }

    }

    private DatastoreClientService configureClientService(final TestRunner runner) throws InitializationException {
        final Solr_6_4_2_ChronixClientService service = new MockSolrClientService();


        runner.setProperty(TestProcessor.SOLR_CLIENT_SERVICE, "service");
        runner.addControllerService("service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        return service;
    }


    private Collection<Record> getRandomMetrics(int size) throws InterruptedException {

        List<Record> records = new ArrayList<>();
        Random rnd = new Random();

        String[] metricsType = {"disk.io", "cpu.wait", "io.wait"};
        String[] hosts = {"host1", "host2", "host3"};
        for (int i = 0; i < size; i++) {
            records.add(new StandardRecord(RecordDictionary.METRIC)
                    .setStringField(FieldDictionary.RECORD_NAME, metricsType[rnd.nextInt(3)])
                    .setStringField("host", hosts[rnd.nextInt(3)])
                    .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, new Date().getTime())
                    .setField(FieldDictionary.RECORD_VALUE, FieldType.FLOAT, 100.0 * Math.random())
            );
            Thread.sleep(rnd.nextInt(500));
        }

        return records;
    }


    @Test
    public void testConvertion() throws InterruptedException {

        final Date now = new Date();
        final Record record =  new StandardRecord(RecordDictionary.METRIC)
                .setStringField(FieldDictionary.RECORD_NAME, "cpu.wait")
                .setTime(now)
                .setField(FieldDictionary.RECORD_VALUE, FieldType.FLOAT, 12.345);

        final Solr_6_4_2_ChronixClientService service = new Solr_6_4_2_ChronixClientService();
        MetricTimeSeries metric = service.convertToMetric(record);

        assertTrue(metric.getName().equals("cpu.wait"));
        assertTrue(metric.getType().equals("metric"));
        assertTrue(metric.getTime(0) == now.getTime());
        assertTrue(metric.getValue(0) == 12.345);
    }


    @Test
    public void testBasics() {
        try {

            Collection<Record> records = null;

            records = getRandomMetrics(1000);

            boolean result;

            final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
            final DatastoreClientService service = configureClientService(runner);

            records.forEach(record -> service.put(CHRONIX_COLLECTION, record, false));

            // Verify the index does not exist
            assertFalse(service.existsCollection("foo"));
            //  Assert.assertEquals(true, service.existsCollection("chronix"));
        } catch (InterruptedException | InitializationException e) {
            e.printStackTrace();
        }

    }

}
