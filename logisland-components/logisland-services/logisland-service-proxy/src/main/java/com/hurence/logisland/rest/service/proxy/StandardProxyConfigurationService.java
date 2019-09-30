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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Category;
import com.hurence.logisland.annotation.documentation.ComponentCategory;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.service.proxy.ProxyConfiguration;
import com.hurence.logisland.service.proxy.ProxyConfigurationService;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 * @see <a href="https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-standard-services/nifi-proxy-configuration-bundle/nifi-proxy-configuration/src/main/java/org/apache/nifi/proxy/StandardProxyConfigurationService.java">
 *      Processor inspired from StandardProxyConfigurationService nifi processor
 *     </a>
 */
@Category(ComponentCategory.UTILS)
@CapabilityDescription("Provides a set of configurations for different NiFi components to use a proxy server.")
@Tags({"Proxy"})
public class StandardProxyConfigurationService extends AbstractControllerService implements ProxyConfigurationService {

    static final PropertyDescriptor PROXY_TYPE = new PropertyDescriptor.Builder()
            .name("proxy.type")
            .displayName("Proxy Type")
            .description("Proxy type.")
            .allowableValues(Proxy.Type.values())
            .defaultValue(Proxy.Type.DIRECT.name())
            .required(true)
            .build();

    static final PropertyDescriptor PROXY_SERVER_HOST = new PropertyDescriptor.Builder()
            .name("proxy.server.host")
            .displayName("Proxy Server Host")
            .description("Proxy server hostname or ip-address.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor PROXY_SERVER_PORT = new PropertyDescriptor.Builder()
            .name("proxy.server.port")
            .displayName("Proxy Server Port")
            .description("Proxy server port number.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor PROXY_USER_NAME = new PropertyDescriptor.Builder()
            .name("proxy.user.name")
            .displayName("Proxy User Name")
            .description("The name of the proxy client for user authentication.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    static final PropertyDescriptor PROXY_USER_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy.user.password")
            .displayName("Proxy User Password")
            .description("The password of the proxy client for user authentication.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .sensitive(true)
            .build();

    private volatile ProxyConfiguration configuration = ProxyConfiguration.DIRECT_CONFIGURATION;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROXY_TYPE);
        properties.add(PROXY_SERVER_HOST);
        properties.add(PROXY_SERVER_PORT);
        properties.add(PROXY_USER_NAME);
        properties.add(PROXY_USER_PASSWORD);
        return properties;
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        super.init(context);
        try {
            configuration = new ProxyConfiguration();
            configuration.setProxyType(Proxy.Type.valueOf(context.getProperty(PROXY_TYPE)));
            if (context.getPropertyValue(PROXY_SERVER_HOST).isSet()) {
                configuration.setProxyServerHost(context.getProperty(PROXY_SERVER_HOST));
            }
            if (context.getPropertyValue(PROXY_SERVER_PORT).isSet()) {
                configuration.setProxyServerPort(context.getPropertyValue(PROXY_SERVER_PORT).asInteger());
            }
            if (context.getPropertyValue(PROXY_USER_NAME).isSet()) {
                configuration.setProxyUserName(context.getProperty(PROXY_USER_NAME));
            }
            if (context.getPropertyValue(PROXY_USER_PASSWORD).isSet()) {
                configuration.setProxyUserPassword(context.getProperty(PROXY_USER_PASSWORD));
            }
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Proxy.Type proxyType = Proxy.Type.valueOf(validationContext.getPropertyValue(PROXY_TYPE).asString());
        if (Proxy.Type.DIRECT.equals(proxyType)) {
            return Collections.emptyList();
        }

        final List<ValidationResult> results = new ArrayList<>();
        if (!validationContext.getPropertyValue(PROXY_SERVER_HOST).isSet()) {
            results.add(new ValidationResult.Builder().subject(PROXY_SERVER_HOST.getDisplayName())
                    .explanation("required").valid(false).build());
        }
        if (!validationContext.getPropertyValue(PROXY_SERVER_PORT).isSet()) {
            results.add(new ValidationResult.Builder().subject(PROXY_SERVER_PORT.getDisplayName())
                    .explanation("required").valid(false).build());
        }
        return results;
    }

    @Override
    public ProxyConfiguration getConfiguration() {
        return configuration;
    }
}
