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


import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.service.proxy.ProxyConfiguration;
import com.hurence.logisland.service.proxy.ProxyConfigurationService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;

import java.net.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestProxyConfiguration {

    private void testProxyConfService(
            final String host,
            final String userName,
            final String password,
            final String port,
            final String type,
            final boolean valid) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());
        ProxyConfigurationService service = new StandardProxyConfigurationService();

        Map<String, String> props = new HashMap<>();
        if (host != null) props.put(StandardProxyConfigurationService.PROXY_SERVER_HOST.getName(), host);
        if (userName != null) props.put(StandardProxyConfigurationService.PROXY_USER_NAME.getName(), userName);
        if (password != null) props.put(StandardProxyConfigurationService.PROXY_USER_PASSWORD.getName(), password);
        if (port != null) props.put(StandardProxyConfigurationService.PROXY_SERVER_PORT.getName(), port);
        if (type != null) props.put(StandardProxyConfigurationService.PROXY_TYPE.getName(), type);
        runner.addControllerService("proxyConfService", service, props);

        if (!valid) {
            runner.assertNotValid(service);
        } else {
            runner.assertValid(service);
            runner.enableControllerService(service);

            final ProxyConfigurationService proxyConfService = (ProxyConfigurationService) runner.getControllerService("proxyConfService");
            assertThat(proxyConfService, instanceOf(StandardProxyConfigurationService.class));

            ProxyConfiguration conf1 = proxyConfService.getConfiguration();
            assertEquals(host, conf1.getProxyServerHost());
            assertEquals(userName, conf1.getProxyUserName());
            assertEquals(password, conf1.getProxyUserPassword());
            assertEquals(Integer.valueOf(port), conf1.getProxyServerPort());
            assertEquals(type == null ? Proxy.Type.DIRECT : Proxy.Type.valueOf(type), conf1.getProxyType());

            Proxy proxy1 = conf1.createProxy();
            assertEquals(type == null ? Proxy.Type.DIRECT : Proxy.Type.valueOf(type), proxy1.type());
        }
    }

    @Test
    public void testValidateProxyConf() throws InitializationException {
        testProxyConfService("host", "userName", "password", "14", "HTTP", true);
    }

    @Test
    public void testValidateProxyConfBadType() throws InitializationException {
        testProxyConfService("host", "userName", "password", "14", "http", false);
    }


    @Test
    public void testValidateProxyConfBadPort() throws InitializationException {
        testProxyConfService("host", "userName", "password", "aa", "HTTP", false);
    }

    @Test
    public void testValidateProxyConfNoHost() throws InitializationException {
        testProxyConfService(null, "userName", "password", "14", "HTTP", false);
    }

    @Test
    public void testValidateProxyConfNoUsername() throws InitializationException {
        testProxyConfService("host", null, "password", "14", "HTTP", true);
    }

    @Test
    public void testValidateProxyConfNoPassword() throws InitializationException {
        testProxyConfService("host", "userName", null, "14", "HTTP", true);
    }

    @Test
    public void testValidateProxyConfNoPort() throws InitializationException {
        testProxyConfService("host", "userName", "password", null, "HTTP", false);
    }

    @Test
    public void testValidateProxyConfNoType() throws InitializationException {
        testProxyConfService("host", "userName", "password", "14", null, true);
    }
}
