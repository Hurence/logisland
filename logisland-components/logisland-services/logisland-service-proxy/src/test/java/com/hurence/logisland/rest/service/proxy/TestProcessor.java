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
package com.hurence.logisland.rest.service.proxy;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.proxy.ProxyConfigurationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class TestProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(TestProcessor.class);

    PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .name("proxy.configuration.service")
            .displayName("Proxy Configuration Service")
            .description("Specifies the Proxy Configuration Controller Service to proxy network requests." +
                    " If set, it supersedes proxy settings configured per component.")
            .identifiesControllerService(ProxyConfigurationService.class)
            .required(false)
            .build();

    // Ip to Geo service to use to perform the translation requests
    private ProxyConfigurationService proxyConfService = null;


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(PROXY_CONFIGURATION_SERVICE);
    }

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        proxyConfService = PluginProxy.rewrap(context.getPropertyValue(PROXY_CONFIGURATION_SERVICE).asControllerService());
        if (proxyConfService == null) {
            logger.error("ProxyConf service is not initialized!");
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        return Collections.emptyList();
    }

}
