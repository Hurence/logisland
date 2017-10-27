/**
 * Copyright (C) 2017 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.iptogeo.maxmind;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class MaxmindIpToGeoServiceTest {

    @Before
    public void setup() {
        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "logisland.com");
        System.setProperty("java.security.krb5.kdc", "logisland.kdc");
    }

    @Test
    public void testIpToGeoService() throws InitializationException, IOException {

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final IpToGeoService service = (IpToGeoService)new MaxmindIpToGeoService();
        runner.addControllerService("ipToGeoService", service);
        runner.enableControllerService(service);
        runner.setProperty(TestProcessor.IP_TO_GEO_SERVICE, "ipToGeoService");
//        runner.assertValid(service);

        final IpToGeoService ipToGeoService = runner.getProcessContext().getPropertyValue(TestProcessor.IP_TO_GEO_SERVICE)
                .asControllerService(IpToGeoService.class);

        Map<String, String> result = ipToGeoService.getGeoInfo("207.97.227.239");
        System.out.println(result);
    }
}
