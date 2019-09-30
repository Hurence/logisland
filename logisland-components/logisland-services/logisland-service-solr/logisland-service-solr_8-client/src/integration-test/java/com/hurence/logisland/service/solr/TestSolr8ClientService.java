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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.service.datastore.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.solr.api.SolrClientService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSolr8ClientService extends TestSolrClientService {
    private class MockSolrClientService extends Solr8ClientService {

        public SolrClient getClient() {
            return solrClient;
        }

        @Override
        protected SolrClient createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
            return solrRule.getClient();
        }

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> props = new ArrayList<>();

            return Collections.unmodifiableList(props);
        }
    }

    public String getVersion() {
        return "8.0.0";
    }

    @Override
    protected SolrClientService getMockClientService() {
        return new MockSolrClientService();
    }

    @Override
    @Test
    public void testMultiGet() throws InitializationException, IOException, InterruptedException, InvalidMultiGetQueryRecordException {
        super.testMultiGet();
    }
}
