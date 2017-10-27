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
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import static com.hurence.logisland.service.iptogeo.IpToGeoService.*;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MaxmindIpToGeoServiceTest {

    @Test
    public void testIpToGeoService() throws InitializationException, IOException {

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final IpToGeoService service = (IpToGeoService)new MockMaxmindIpToGeoService();
        runner.addControllerService("ipToGeoService", service);
        runner.enableControllerService(service);
        runner.setProperty(TestProcessor.IP_TO_GEO_SERVICE, "ipToGeoService");
        runner.assertValid(service);

        final IpToGeoService ipToGeoService = runner.getProcessContext().getPropertyValue(TestProcessor.IP_TO_GEO_SERVICE)
                .asControllerService(IpToGeoService.class);

        Map<String, String> result = ipToGeoService.getGeoInfo("207.97.227.239"); // Github IP

        assertEquals("San Antonio", result.get(GEO_FIELD_CITY));
        assertEquals("29.4889", result.get(GEO_FIELD_LATITUDE));
        assertEquals("-98.3987", result.get(GEO_FIELD_LONGITUDE));
        assertEquals("Texas", result.get(GEO_FIELD_SUBDIVISION + "0"));
        assertEquals("TX", result.get(GEO_FIELD_SUBDIVISION_ISOCODE + "0"));
        assertEquals("United States", result.get(GEO_FIELD_COUNTRY));
        assertEquals("US", result.get(GEO_FIELD_COUNTRY_ISOCODE));
        assertEquals("78218", result.get(GEO_FIELD_POSTALCODE));
    }

    /**
     * Just because
     * runner.setProperty(service, MaxmindIpToGeoService.MAXMIND_DATABASE_FILE_PATH, "ipToGeoService");
     * does not work if called after
     * runner.addControllerService("ipToGeoService", service);
     * and vice versa (runner controller service not implemented, so workaround for the moment)
     */
    private class MockMaxmindIpToGeoService extends MaxmindIpToGeoService
    {

        public void init(ControllerServiceInitializationContext context) throws InitializationException {
            dbPath = "/local/cybersecu/maxmind/GeoLite2-City_20171003/GeoLite2-City.mmdb";
            super.init(context);
        }
    }
}
