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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

        Map<String, String> result = ipToGeoService.getGeoInfo("81.2.69.142");

        assertEquals("London", result.get(GEO_FIELD_CITY));
        assertEquals("51.5142", result.get(GEO_FIELD_LATITUDE));
        assertEquals("-0.0931", result.get(GEO_FIELD_LONGITUDE));
        assertEquals("England", result.get(GEO_FIELD_SUBDIVISION + "0"));
        assertEquals("ENG", result.get(GEO_FIELD_SUBDIVISION_ISOCODE + "0"));
        assertEquals("United Kingdom", result.get(GEO_FIELD_COUNTRY));
        assertEquals("GB", result.get(GEO_FIELD_COUNTRY_ISOCODE));
        assertEquals(null, result.get(GEO_FIELD_POSTALCODE));
    }

    /**
     * Just because
     * runner.setProperty(service, MaxmindIpToGeoService.MAXMIND_DATABASE_FILE_PATH, "pathToDbFile");
     * does not work if called after
     * runner.addControllerService("ipToGeoService", service);
     * and vice versa (runner controller service not implemented, so workaround for the moment)
     */
    public class MockMaxmindIpToGeoService extends MaxmindIpToGeoService
    {
        // Use a small test DB file we got from https://github.com/maxmind/MaxMind-DB/tree/master/test-data
        // to avoid embedding a big maxmind db in our workspace
        public void init(ControllerServiceInitializationContext context) throws InitializationException {

            File file = new File(getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getFile());
            dbPath = file.getAbsolutePath();
            super.init(context);
        }
    }
}
