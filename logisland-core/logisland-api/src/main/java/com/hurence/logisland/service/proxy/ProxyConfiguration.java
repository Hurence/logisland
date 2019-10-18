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
package com.hurence.logisland.service.proxy;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;


import static com.hurence.logisland.service.proxy.ProxySpec.*;

/**
 *
 * @see <a href="https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-standard-services/nifi-proxy-configuration-api/src/main/java/org/apache/nifi/proxy/ProxyConfiguration.java">
 *      class inspired from ProxyConfiguration nifi
 *     </a>
 */
public class ProxyConfiguration {

    public static final ProxyConfiguration DIRECT_CONFIGURATION = new ProxyConfiguration();

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .name("proxy.configuration.service")
            .displayName("Proxy Configuration Service")
            .description("Specifies the Proxy Configuration Controller Service to proxy network requests." +
                    " If set, it supersedes proxy settings configured per component.")
            .identifiesControllerService(ProxyConfigurationService.class)
            .required(false)
            .build();

    public static PropertyDescriptor createProxyConfigPropertyDescriptor(final boolean hasComponentProxyConfigs, final ProxySpec ... _specs) {

        final Set<ProxySpec> specs = getUniqueProxySpecs(_specs);

        final StringBuilder description = new StringBuilder("Specifies the Proxy Configuration Controller Service to proxy network requests.");
        if (hasComponentProxyConfigs) {
            description.append(" If set, it supersedes proxy settings configured per component.");
        }
        description.append(" Supported proxies: ");
        description.append(specs.stream().map(ProxySpec::getDisplayName).collect(Collectors.joining(", ")));

        return new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(PROXY_CONFIGURATION_SERVICE)
                .description(description.toString())
                .build();
    }

    /**
     * Remove redundancy. If X_AUTH is supported, then X should be supported, too.
     * @param _specs original specs
     * @return sorted unique specs
     */
    private static Set<ProxySpec> getUniqueProxySpecs(ProxySpec ... _specs) {
        final Set<ProxySpec> specs = Arrays.stream(_specs).sorted().collect(Collectors.toSet());
        if (specs.contains(HTTP_AUTH)) {
            specs.remove(HTTP);
        }
        if (specs.contains(SOCKS_AUTH)) {
            specs.remove(SOCKS);
        }
        return specs;
    }

    public static void validateProxySpec(ValidationContext context, Collection<ValidationResult> results, final ProxySpec ... _specs) {

        final Set<ProxySpec> specs = getUniqueProxySpecs(_specs);
        final Set<Proxy.Type> supportedProxyTypes = specs.stream().map(ProxySpec::getProxyType).collect(Collectors.toSet());

        if (!context.getPropertyValue(PROXY_CONFIGURATION_SERVICE).isSet()) {
            return;
        }

        final ProxyConfigurationService proxyService = context.getPropertyValue(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
        final ProxyConfiguration proxyConfiguration = proxyService.getConfiguration();
        final Proxy.Type proxyType = proxyConfiguration.getProxyType();

        if (proxyType.equals(Proxy.Type.DIRECT)) {
            return;
        }

        if (!supportedProxyTypes.contains(proxyType)) {
            results.add(new ValidationResult.Builder()
                    .explanation(String.format("Proxy type %s is not supported.", proxyType))
                    .valid(false)
                    .subject(PROXY_CONFIGURATION_SERVICE.getDisplayName())
                    .build());

            // If the proxy type is not supported, no need to do further validation.
            return;
        }

        if (proxyConfiguration.hasCredential()) {
            // If credential is set, check whether the component is capable to use it.
            if (!specs.contains(Proxy.Type.HTTP.equals(proxyType) ? HTTP_AUTH : SOCKS_AUTH)) {
                results.add(new ValidationResult.Builder()
                        .explanation(String.format("Proxy type %s with Authentication is not supported.", proxyType))
                        .valid(false)
                        .subject(PROXY_CONFIGURATION_SERVICE.getDisplayName())
                        .build());
            }
        }


    }
    public static ProxyConfiguration getConfiguration(ProcessContext context, Supplier<ProxyConfiguration> perComponentSetting) {
        if (context.getPropertyValue(PROXY_CONFIGURATION_SERVICE).isSet()) {
            final ProxyConfigurationService proxyService = context.getPropertyValue(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
            return proxyService.getConfiguration();
        } else {
            return perComponentSetting.get();
        }
    }

    private Proxy.Type proxyType = Proxy.Type.DIRECT;
    private String proxyServerHost;
    private Integer proxyServerPort;
    private String proxyUserName;
    private String proxyUserPassword;

    public Proxy.Type getProxyType() {
        return proxyType;
    }

    public void setProxyType(Proxy.Type proxyType) {
        this.proxyType = proxyType;
    }

    public String getProxyServerHost() {
        return proxyServerHost;
    }

    public void setProxyServerHost(String proxyServerHost) {
        this.proxyServerHost = proxyServerHost;
    }

    public Integer getProxyServerPort() {
        return proxyServerPort;
    }

    public void setProxyServerPort(Integer proxyServerPort) {
        this.proxyServerPort = proxyServerPort;
    }

    public boolean hasCredential() {
        return proxyUserName != null && !proxyUserName.isEmpty();
    }

    public String getProxyUserName() {
        return proxyUserName;
    }

    public void setProxyUserName(String proxyUserName) {
        this.proxyUserName = proxyUserName;
    }

    public String getProxyUserPassword() {
        return proxyUserPassword;
    }

    public void setProxyUserPassword(String proxyUserPassword) {
        this.proxyUserPassword = proxyUserPassword;
    }

    /**
     * Create a Proxy instance based on proxy type, proxy server host and port.
     */
    public Proxy createProxy() {
        return Proxy.Type.DIRECT.equals(proxyType) ? Proxy.NO_PROXY : new Proxy(proxyType, new InetSocketAddress(proxyServerHost, proxyServerPort));
    }

}
