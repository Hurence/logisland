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
package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;


public abstract class AbstractElasticsearchProcessor extends AbstractProcessor {

    public static final PropertyDescriptor ELASTICSEARCH_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("elasticsearch.client.service")
            .description("The instance of the Controller Service to use for accessing Elasticsearch.")
            .required(true)
            .identifiesControllerService(ElasticsearchClientService.class)
            .build();


    protected ElasticsearchClientService elasticsearchClientService;

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        elasticsearchClientService = PluginProxy.rewrap(context.getPropertyValue(ELASTICSEARCH_CLIENT_SERVICE).asControllerService());
        if (elasticsearchClientService == null) {
            getLogger().error("Elasticsearch client service is not initialized!");
        }
    }


}
