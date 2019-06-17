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
package com.hurence.logisland.processor.datastore;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractDatastoreProcessor extends AbstractProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractDatastoreProcessor.class);

    public static final PropertyDescriptor DATASTORE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("datastore.client.service")
            .description("The instance of the Controller Service to use for accessing datastore.")
            .required(true)
            .identifiesControllerService(DatastoreClientService.class)
            .build();


    protected DatastoreClientService datastoreClientService;

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        datastoreClientService = PluginProxy.rewrap(context.getPropertyValue(DATASTORE_CLIENT_SERVICE).asControllerService());
        if (datastoreClientService == null) {
            logger.error("Datastore client service is not initialized!");
        }
    }


}