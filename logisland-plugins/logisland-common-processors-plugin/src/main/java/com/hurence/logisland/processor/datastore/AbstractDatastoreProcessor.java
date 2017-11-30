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

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.logging.StandardComponentLogger;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.service.datastore.DatastoreClientService;


public abstract class AbstractDatastoreProcessor extends AbstractProcessor {

    private ComponentLog logger = new StandardComponentLogger(this.getIdentifier(), AbstractDatastoreProcessor.class);

    static final PropertyDescriptor DATASTORE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
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
    public void init(final ProcessContext context) {
        logger.info("Datastore client service initialization");
        datastoreClientService = context.getPropertyValue(DATASTORE_CLIENT_SERVICE).asControllerService(DatastoreClientService.class);
        if(datastoreClientService == null) {
            logger.error("Datastore client service is not initialized!");
        }
    }




}