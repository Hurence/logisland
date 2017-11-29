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

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSolr_5_5_5_ClientService extends TestSolrClientService {
    private class MockSolrClientService extends Solr_5_5_5_ClientService {

        public SolrClient getClient() {
            return solrClient;
        }

        @Override
        protected void createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
            setClient(solrRule.getClient());
        }

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> props = new ArrayList<>();

            return Collections.unmodifiableList(props);
        }
    }

    public String getVersion() {
        return "5.5.5";
    }

    @Override
    protected SolrClientService getMockClientService() {
        return new MockSolrClientService();
    }

    @Test
    @Override
    public void testBasics() throws Exception {
        super.testBasics();
    }
}
