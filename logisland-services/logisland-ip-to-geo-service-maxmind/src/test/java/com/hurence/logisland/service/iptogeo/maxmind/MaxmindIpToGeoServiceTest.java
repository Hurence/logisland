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

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DataProviderRunner.class)
public class MaxmindIpToGeoServiceTest {

    @DataProvider
    public static Object[][] testIpToGeoServiceProvider() {

        Map<String, Object> enResult = new HashMap<String, Object>();
        enResult.put(GEO_FIELD_CONTINENT, "Europe");
        enResult.put(GEO_FIELD_CONTINENT_CODE, "EU");
        enResult.put(GEO_FIELD_CITY, "Boxford");
        enResult.put(GEO_FIELD_LATITUDE, new Double("51.75"));
        enResult.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        enResult.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        enResult.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        enResult.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        enResult.put(GEO_FIELD_SUBDIVISION + "0", "England");
        enResult.put(GEO_FIELD_SUBDIVISION_ISOCODE + "0", "ENG");
        enResult.put(GEO_FIELD_SUBDIVISION + "1", "West Berkshire");
        enResult.put(GEO_FIELD_SUBDIVISION_ISOCODE + "1", "WBK");
        enResult.put(GEO_FIELD_COUNTRY, "United Kingdom");
        enResult.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        enResult.put(GEO_FIELD_POSTALCODE, "OX1");

        Map<String, Object> frResult = new HashMap<String, Object>();
        frResult.put(GEO_FIELD_CONTINENT, "Europe");
        frResult.put(GEO_FIELD_CONTINENT_CODE, "EU");
        frResult.put(GEO_FIELD_CITY, null);
        frResult.put(GEO_FIELD_LATITUDE, new Double("51.75"));
        frResult.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        frResult.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        frResult.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        frResult.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        frResult.put(GEO_FIELD_SUBDIVISION + "0", "Angleterre");
        frResult.put(GEO_FIELD_SUBDIVISION_ISOCODE + "0", "ENG");
        frResult.put(GEO_FIELD_SUBDIVISION + "1", null);
        frResult.put(GEO_FIELD_SUBDIVISION_ISOCODE + "1", "WBK");
        frResult.put(GEO_FIELD_COUNTRY, "Royaume-Uni");
        frResult.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        frResult.put(GEO_FIELD_POSTALCODE, "OX1");

        Map<String, Object> enResult2 = new HashMap<String, Object>();
        enResult2.put(GEO_FIELD_CONTINENT, "Europe");
        enResult2.put(GEO_FIELD_CONTINENT_CODE, "EU");
        enResult2.put(GEO_FIELD_CITY, "London");
        enResult2.put(GEO_FIELD_LATITUDE, new Double("51.5142"));
        enResult2.put(GEO_FIELD_LONGITUDE, new Double("-0.0931"));
        enResult2.put(GEO_FIELD_LOCATION, "51.5142,-0.0931");
        enResult2.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        enResult2.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        enResult2.put(GEO_FIELD_SUBDIVISION + "0", "England");
        enResult2.put(GEO_FIELD_SUBDIVISION_ISOCODE + "0", "ENG");
        enResult2.put(GEO_FIELD_COUNTRY, "United Kingdom");
        enResult2.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        enResult2.put(GEO_FIELD_POSTALCODE, null);

        Map<String, Object> frResult2 = new HashMap<String, Object>();
        frResult2.put(GEO_FIELD_CONTINENT, "Europe");
        frResult2.put(GEO_FIELD_CONTINENT_CODE, "EU");
        frResult2.put(GEO_FIELD_CITY, "Londres");
        frResult2.put(GEO_FIELD_LATITUDE, new Double("51.5142"));
        frResult2.put(GEO_FIELD_LONGITUDE, new Double("-0.0931"));
        frResult2.put(GEO_FIELD_LOCATION, "51.5142,-0.0931");
        frResult2.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        frResult2.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        frResult2.put(GEO_FIELD_SUBDIVISION + "0", "Angleterre");
        frResult2.put(GEO_FIELD_SUBDIVISION_ISOCODE + "0", "ENG");
        frResult2.put(GEO_FIELD_COUNTRY, "Royaume-Uni");
        frResult2.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        frResult2.put(GEO_FIELD_POSTALCODE, null);

        Map<String, Object> deResult2 = new HashMap<String, Object>();
        deResult2.put(GEO_FIELD_CONTINENT, "Europa");
        deResult2.put(GEO_FIELD_CONTINENT_CODE, "EU");
        deResult2.put(GEO_FIELD_CITY, "London");
        deResult2.put(GEO_FIELD_LATITUDE, new Double("51.5142"));
        deResult2.put(GEO_FIELD_LONGITUDE, new Double("-0.0931"));
        deResult2.put(GEO_FIELD_LOCATION, "51.5142,-0.0931");
        deResult2.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        deResult2.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        deResult2.put(GEO_FIELD_SUBDIVISION + "0", null);
        deResult2.put(GEO_FIELD_SUBDIVISION_ISOCODE + "0", "ENG");
        deResult2.put(GEO_FIELD_COUNTRY, "Vereinigtes Königreich");
        deResult2.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        deResult2.put(GEO_FIELD_POSTALCODE, null);

        Object[][] inputs = {
                {"2.125.160.216", "en", enResult},
                {"2.125.160.216", "fr", frResult},
                {"81.2.69.160", "en", enResult2},
                {"81.2.69.160", "fr", frResult2},
                {"81.2.69.160", "de", deResult2}
        };

        return inputs;
    }


    @Test
    @UseDataProvider("testIpToGeoServiceProvider")
    public void testIpToGeoService(String ip, String locale, Map<String, Object> expectedResult) throws InitializationException, IOException {

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor
        final IpToGeoService service = (IpToGeoService)new MockMaxmindIpToGeoService(locale);
        runner.addControllerService("ipToGeoService", service);
        runner.enableControllerService(service);
        runner.setProperty(TestProcessor.IP_TO_GEO_SERVICE, "ipToGeoService");
        runner.assertValid(service);

        final IpToGeoService ipToGeoService = runner.getProcessContext().getPropertyValue(TestProcessor.IP_TO_GEO_SERVICE)
                .asControllerService(IpToGeoService.class);

        Map<String, Object> result = ipToGeoService.getGeoInfo(ip);

        // Check that a time has been added
        int searchTimeMicros = (int)result.get(GEO_FIELD_LOOKUP_TIME_MICROS);
        assertTrue("Should return non strictly positive search time but was " + searchTimeMicros + " micros", searchTimeMicros >= 0);

        // Of course, remove time to be able to compare maps
        result.remove(GEO_FIELD_LOOKUP_TIME_MICROS);

        // Compare maps
        assertEquals("Expected and result maps should be identical", expectedResult, result);
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
        public MockMaxmindIpToGeoService(String locale)
        {
            super.locale = locale;
        }

        // Use a small test DB file we got from https://github.com/maxmind/MaxMind-DB/tree/master/test-data
        // to avoid embedding a big maxmind db in our workspace
        public void init(ControllerServiceInitializationContext context) throws InitializationException {

            File file = new File(getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getFile());
            dbPath = file.getAbsolutePath();
            super.init(context);
        }
    }
}
