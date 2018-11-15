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
package com.hurence.logisland.processor.enrichment;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hurence.logisland.service.iptogeo.IpToGeoService.*;
import static org.junit.Assert.*;

@RunWith(DataProviderRunner.class)
public class IpToGeoTest {

    private static Logger logger = LoggerFactory.getLogger(IpToGeoTest.class);
    private static final String IP_ADDRESS_FIELD_NAME = "src_ip";

    public static final String KEY = "some key";
    public static final String VALUE = "some content";

    private Record getRecordWithStringIp(String ip) {
        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord(KEY, VALUE));
        inputRecord.setStringField(IP_ADDRESS_FIELD_NAME, ip);
        return inputRecord;
    }

    @DataProvider
    public static Object[][] testHierarchicalProvider() {

        Map<String, Object> fullFields = new HashMap<String, Object>();
        fullFields.put(GEO_FIELD_CONTINENT, "Europe");
        fullFields.put(GEO_FIELD_CONTINENT_CODE, "EU");
        fullFields.put(GEO_FIELD_CITY, "Boxford");
        fullFields.put(GEO_FIELD_LATITUDE, new Double("51.75"));
        fullFields.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        fullFields.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        fullFields.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        fullFields.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        fullFields.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        fullFields.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        fullFields.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        fullFields.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        fullFields.put(GEO_FIELD_COUNTRY, "United Kingdom");
        fullFields.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        fullFields.put(GEO_FIELD_POSTALCODE, "OX1");

        String fullFieldsList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CONTINENT,
                GEO_FIELD_CONTINENT_CODE,
                GEO_FIELD_CITY,
                GEO_FIELD_LATITUDE,
                GEO_FIELD_LONGITUDE,
                GEO_FIELD_LOCATION,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_TIME_ZONE,
                GEO_FIELD_SUBDIVISION,
                GEO_FIELD_SUBDIVISION_ISOCODE,
                GEO_FIELD_COUNTRY,
                GEO_FIELD_COUNTRY_ISOCODE,
                GEO_FIELD_POSTALCODE
        ));

        Map<String, Object> subFieldsIsocode = new HashMap<String, Object>();
        subFieldsIsocode.put(GEO_FIELD_CITY, "Boxford");
        subFieldsIsocode.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        subFieldsIsocode.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        subFieldsIsocode.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        subFieldsIsocode.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        subFieldsIsocode.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        subFieldsIsocode.put(GEO_FIELD_COUNTRY, "United Kingdom");
        subFieldsIsocode.put(GEO_FIELD_POSTALCODE, "OX1");

        String subFieldsIsocodeList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CITY,
                GEO_FIELD_LONGITUDE,
                GEO_FIELD_LOCATION,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_SUBDIVISION_ISOCODE,
                GEO_FIELD_COUNTRY,
                GEO_FIELD_POSTALCODE
        ));

        Map<String, Object> subFieldsSubdiv = new HashMap<String, Object>();
        subFieldsSubdiv.put(GEO_FIELD_CITY, "Boxford");
        subFieldsSubdiv.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        subFieldsSubdiv.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        subFieldsSubdiv.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        subFieldsSubdiv.put(GEO_FIELD_COUNTRY, "United Kingdom");
        subFieldsSubdiv.put(GEO_FIELD_POSTALCODE, "OX1");

        String subFieldsSubdivList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CITY,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_SUBDIVISION,
                GEO_FIELD_COUNTRY,
                GEO_FIELD_POSTALCODE
        ));

        Map<String, Object> subFieldsBoth = new HashMap<String, Object>();
        subFieldsBoth.put(GEO_FIELD_CITY, "Boxford");
        subFieldsBoth.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        subFieldsBoth.put(GEO_FIELD_POSTALCODE, "OX1");

        String subFieldsBothList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CITY,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_SUBDIVISION,
                GEO_FIELD_SUBDIVISION_ISOCODE,
                GEO_FIELD_POSTALCODE
        ));

        Object[][] inputs = {
                {"2.125.160.216", null, null, fullFields},
                {"2.125.160.216", "*", null, fullFields},
                {"2.125.160.216", null, "_suffix", fullFields},
                {"2.125.160.216", "*", "_suffix", fullFields},
                {"2.125.160.216", fullFieldsList, null, fullFields},
                {"2.125.160.216", fullFieldsList, "_suffix", fullFields},
                {"2.125.160.216", subFieldsIsocodeList, null, subFieldsIsocode},
                {"2.125.160.216", subFieldsIsocodeList, "_suffix", subFieldsIsocode},
                {"2.125.160.216", subFieldsSubdivList, null, subFieldsSubdiv},
                {"2.125.160.216", subFieldsSubdivList, "_suffix", subFieldsSubdiv},
                {"2.125.160.216", subFieldsBothList, null, subFieldsBoth},
                {"2.125.160.216", subFieldsBothList, "_suffix", subFieldsBoth}
        };

        return inputs;
    }

    /**
     * Facility to get a comma separated list of geo fields
     *
     * @param fields Fields
     * @return A usable list of fields for the processor configuration
     */
    private static String fieldsToCsv(List<String> fields) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String field : fields) {
            if (first) {
                sb.append(field);
                first = false;
            } else {
                sb.append(",").append(field);
            }
        }
        return sb.toString();
    }

    @Test
    @UseDataProvider("testHierarchicalProvider")
    public void testHierarchical(String ip, String geoFields, String hierarchicalSuffix, Map<String, Object> expectedResult) throws Exception {
        final TestRunner runner = getTestRunner();

        runner.setProperty(IpToGeo.HIERARCHICAL, "true");
        if (geoFields != null) {
            runner.setProperty(IpToGeo.GEO_FIELDS, geoFields);
        }
        if (hierarchicalSuffix != null) {
            runner.setProperty(IpToGeo.HIERARCHICAL_SUFFIX, hierarchicalSuffix);
        }

        final Record inputRecord = getRecordWithStringIp(ip);
        runner.enqueue(inputRecord);
        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord = runner.getOutputRecords().get(0);

        // Check the hierarchical field is present (field holding geo fields)
        final String fatherField = IP_ADDRESS_FIELD_NAME + ((hierarchicalSuffix != null) ? hierarchicalSuffix : "_geo");
        outputRecord.assertFieldExists(fatherField);

        Field fatherFieldValue = outputRecord.getField(fatherField);
        Map<String, Object> geoFieldsFromFather = (Map<String, Object>) fatherFieldValue.getRawValue();

        // Check that a time has been added
        int searchTimeMicros = (int) geoFieldsFromFather.get(GEO_FIELD_LOOKUP_TIME_MICROS);
        assertTrue("Should return non strictly positive search time but was " + searchTimeMicros + " micros", searchTimeMicros >= 0);
        // Of course, remove time to be able to compare maps
        geoFieldsFromFather.remove(GEO_FIELD_LOOKUP_TIME_MICROS);

        // Compare maps
        assertEquals("Hierarchical and expected maps should be identical", expectedResult, geoFieldsFromFather);

        outputRecord.assertFieldNotExists(ProcessError.RUNTIME_ERROR.toString());
    }

    @Test
    public void testHierarchicalWithCache() throws Exception {
        final TestRunner runner = getTestRunner();
        runner.setProperty("cache.size", "5");
        runner.setProperty("debug", "true");

        final Record inputRecord = getRecordWithStringIp("2.125.160.216");
        final Record inputRecord2 = getRecordWithStringIp("2.125.160.216");

        String hierarchicalSuffix = null;

        Map<String, Object> expectedResult = new HashMap<String, Object>();
        expectedResult.put(GEO_FIELD_CONTINENT, "Europe");
        expectedResult.put(GEO_FIELD_CONTINENT_CODE, "EU");
        expectedResult.put(GEO_FIELD_CITY, "Boxford");
        expectedResult.put(GEO_FIELD_LATITUDE, new Double("51.75"));
        expectedResult.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        expectedResult.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        expectedResult.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        expectedResult.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        expectedResult.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        expectedResult.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        expectedResult.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        expectedResult.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        expectedResult.put(GEO_FIELD_COUNTRY, "United Kingdom");
        expectedResult.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        expectedResult.put(GEO_FIELD_POSTALCODE, "OX1");

        runner.setProperty(IpToGeo.HIERARCHICAL, "true");

        runner.enqueue(inputRecord, inputRecord2);
        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord = runner.getOutputRecords().get(0);

        // Check the hierarchical field is present (field holding geo fields)
        final String fatherField = IP_ADDRESS_FIELD_NAME + ((hierarchicalSuffix != null) ? hierarchicalSuffix : "_geo");
        outputRecord.assertFieldExists(fatherField);

        Field fatherFieldValue = outputRecord.getField(fatherField);
        Map<String, Object> geoFieldsFromFather = (Map<String, Object>) fatherFieldValue.getRawValue();

        // Of course, remove time to be able to compare maps
        geoFieldsFromFather.remove(GEO_FIELD_LOOKUP_TIME_MICROS);

        // Compare maps
        assertEquals("Hierarchical and expected maps should be identical", expectedResult, geoFieldsFromFather);

        // Check the geoInfo has not been retrieved from the cache.
        String fromCache = fatherField + IpToGeo.DEBUG_FROM_CACHE_SUFFIX;
        outputRecord.assertFieldEquals(fromCache, false);

        final MockRecord outputRecord2 = runner.getOutputRecords().get(1);

        // Check the geoInfo has been retrieved from cache this time.
        outputRecord2.assertFieldEquals(fromCache, true);
    }

    @DataProvider
    public static Object[][] testFlatProvider() {

        Map<String, Object> fullFields = new HashMap<String, Object>();
        fullFields.put(GEO_FIELD_CONTINENT, "Europe");
        fullFields.put(GEO_FIELD_CONTINENT_CODE, "EU");
        fullFields.put(GEO_FIELD_CITY, "Boxford");
        fullFields.put(GEO_FIELD_LATITUDE, new Double("51.75"));
        fullFields.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        fullFields.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        fullFields.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        fullFields.put(GEO_FIELD_TIME_ZONE, "Europe/London");
        fullFields.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        fullFields.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        fullFields.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        fullFields.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        fullFields.put(GEO_FIELD_COUNTRY, "United Kingdom");
        fullFields.put(GEO_FIELD_COUNTRY_ISOCODE, "GB");
        fullFields.put(GEO_FIELD_POSTALCODE, "OX1");

        String fullFieldsList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CONTINENT,
                GEO_FIELD_CONTINENT_CODE,
                GEO_FIELD_CITY,
                GEO_FIELD_LATITUDE,
                GEO_FIELD_LONGITUDE,
                GEO_FIELD_LOCATION,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_TIME_ZONE,
                GEO_FIELD_SUBDIVISION,
                GEO_FIELD_SUBDIVISION_ISOCODE,
                GEO_FIELD_COUNTRY,
                GEO_FIELD_COUNTRY_ISOCODE,
                GEO_FIELD_POSTALCODE
        ));

        Map<String, Object> subFieldsIsocode = new HashMap<String, Object>();
        subFieldsIsocode.put(GEO_FIELD_CITY, "Boxford");
        subFieldsIsocode.put(GEO_FIELD_LONGITUDE, new Double("-1.25"));
        subFieldsIsocode.put(GEO_FIELD_LOCATION, "51.75,-1.25");
        subFieldsIsocode.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        subFieldsIsocode.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        subFieldsIsocode.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        subFieldsIsocode.put(GEO_FIELD_COUNTRY, "United Kingdom");
        subFieldsIsocode.put(GEO_FIELD_POSTALCODE, "OX1");

        String subFieldsIsocodeList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CITY,
                GEO_FIELD_LONGITUDE,
                GEO_FIELD_LOCATION,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_SUBDIVISION_ISOCODE,
                GEO_FIELD_COUNTRY,
                GEO_FIELD_POSTALCODE
        ));

        Map<String, Object> subFieldsSubdiv = new HashMap<String, Object>();
        subFieldsSubdiv.put(GEO_FIELD_CITY, "Boxford");
        subFieldsSubdiv.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        subFieldsSubdiv.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        subFieldsSubdiv.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        subFieldsSubdiv.put(GEO_FIELD_COUNTRY, "United Kingdom");
        subFieldsSubdiv.put(GEO_FIELD_POSTALCODE, "OX1");

        String subFieldsSubdivList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CITY,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_SUBDIVISION,
                GEO_FIELD_COUNTRY,
                GEO_FIELD_POSTALCODE
        ));

        Map<String, Object> subFieldsBoth = new HashMap<String, Object>();
        subFieldsBoth.put(GEO_FIELD_CITY, "Boxford");
        subFieldsBoth.put(GEO_FIELD_ACCURACY_RADIUS, new Integer(100));
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "0", "England");
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION + SEPARATOR + "1", "West Berkshire");
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "0", "ENG");
        subFieldsBoth.put(GEO_FIELD_SUBDIVISION_ISOCODE + SEPARATOR + "1", "WBK");
        subFieldsBoth.put(GEO_FIELD_POSTALCODE, "OX1");

        String subFieldsBothList = fieldsToCsv(Arrays.asList(
                GEO_FIELD_LOOKUP_TIME_MICROS,
                GEO_FIELD_CITY,
                GEO_FIELD_ACCURACY_RADIUS,
                GEO_FIELD_SUBDIVISION,
                GEO_FIELD_SUBDIVISION_ISOCODE,
                GEO_FIELD_POSTALCODE
        ));

        Object[][] inputs = {
                {"2.125.160.216", null, null, fullFields},
                {"2.125.160.216", "*", null, fullFields},
                {"2.125.160.216", null, "_suffix_", fullFields},
                {"2.125.160.216", "*", "_suffix_", fullFields},
                {"2.125.160.216", fullFieldsList, null, fullFields},
                {"2.125.160.216", fullFieldsList, "_suffix_", fullFields},
                {"2.125.160.216", subFieldsIsocodeList, null, subFieldsIsocode},
                {"2.125.160.216", subFieldsIsocodeList, "_suffix_", subFieldsIsocode},
                {"2.125.160.216", subFieldsSubdivList, null, subFieldsSubdiv},
                {"2.125.160.216", subFieldsSubdivList, "_suffix_", subFieldsSubdiv},
                {"2.125.160.216", subFieldsBothList, null, subFieldsBoth},
                {"2.125.160.216", subFieldsBothList, "_suffix_", subFieldsBoth}
        };

        return inputs;
    }

    @Test
    @UseDataProvider("testFlatProvider")
    public void testFlat(String ip, String geoFields, String flatSuffix, Map<String, Object> expectedResult) throws Exception {
        final TestRunner runner = getTestRunner();

        runner.setProperty(IpToGeo.HIERARCHICAL, "false");
        if (geoFields != null) {
            runner.setProperty(IpToGeo.GEO_FIELDS, geoFields);
        }
        if (flatSuffix != null) {
            runner.setProperty(IpToGeo.FLAT_SUFFIX, flatSuffix);
        }

        final Record inputRecord = getRecordWithStringIp(ip);
        runner.enqueue(inputRecord);
        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord = runner.getOutputRecords().get(0);

        // Compute prefix for flat geo field names
        final String fieldPrefix = IP_ADDRESS_FIELD_NAME + ((flatSuffix != null) ? flatSuffix : "_geo_");


        // Check that a time has been added
        String lookupTimeFieldName = fieldPrefix + GEO_FIELD_LOOKUP_TIME_MICROS;
        outputRecord.assertFieldExists(lookupTimeFieldName);
        int searchTimeMicros = outputRecord.getField(lookupTimeFieldName).asInteger();
        assertTrue("Should return non strictly positive search time but was " + searchTimeMicros + " micros", searchTimeMicros >= 0);
        // Remove time and any other useless field to be able to compare maps
        outputRecord.removeField(lookupTimeFieldName);
        outputRecord.removeField(IP_ADDRESS_FIELD_NAME);
        outputRecord.removeField(FieldDictionary.RECORD_KEY);
        outputRecord.removeField(FieldDictionary.RECORD_VALUE);

        // Compare maps
        compareMaps(outputRecord, expectedResult, fieldPrefix);

        outputRecord.assertFieldNotExists(ProcessError.RUNTIME_ERROR.toString());
    }

    /**
     * Compares maps of geo fields with the expected ones
     *
     * @param outputRecord
     * @param expectedResult
     */
    private void compareMaps(Record outputRecord, Map<String, Object> expectedResult, String prefix) {
        assertEquals("Both maps should have the same number of fields\nExpected: " +
                        expectedResult + "\nGot: " + outputRecord
                , expectedResult.size(), outputRecord.size());
        // outputRecord.size() does not count dictionnary fields time, type, record_id and record_type fields

        for (Map.Entry<String, Object> entry : expectedResult.entrySet()) {
            String expectedFieldName = entry.getKey();
            FieldType expectedFieldType = IpToGeo.supportedGeoFieldNames.get(expectedFieldName);
            if (expectedFieldType == null) {
                // Handle subdivision and subdivision_isocode fields (geo_subdivision_0 is not geo_subdivision)
                expectedFieldType = FieldType.STRING;
            }

            // Check field name
            Field actualField = outputRecord.getField(prefix + expectedFieldName);
            assertNotNull("Record map should contain a field named: " + expectedFieldName, actualField);

            // Check field type
            FieldType actualFieldType = actualField.getType();
            assertEquals("Geo field " + expectedFieldName + " should be of type: " + expectedFieldType,
                    expectedFieldType, actualFieldType);

            // Check field value
            Object expectedFieldValue = entry.getValue();
            Object actualFieldValue = actualField.getRawValue();
            assertEquals("Geo field " + expectedFieldName + " should have value: " + expectedFieldValue,
                    expectedFieldValue, actualFieldValue);
        }
    }

    private TestRunner getTestRunner() throws Exception {

        final TestRunner runner = TestRunners.newTestRunner("com.hurence.logisland.processor.enrichment.IpToGeo");
        runner.setProperty(IpToGeo.IP_ADDRESS_FIELD, IP_ADDRESS_FIELD_NAME);
        runner.setProperty(IpToGeo.IP_TO_GEO_SERVICE, "ipToGeoService");
        runner.setProperty("lookup.time", "true");

        runner.setProperty("maxmind.database.uri", new File(getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getFile()).toURI().toASCIIString());

        // create the controller service and link it to the test processor
        final IpToGeoService service = ComponentFactory.loadComponent("com.hurence.logisland.service.iptogeo.maxmind.MaxmindIpToGeoService");
        runner.addControllerService("ipToGeoService", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final MockCacheService<String, String> cacheService = new MockCacheService();
        runner.addControllerService("cacheService", cacheService);
        runner.enableControllerService(cacheService);
        runner.setProperty(IpToGeo.CONFIG_CACHE_SERVICE, "cacheService");

        return runner;
    }

    /**
     * Just because
     * runner.setProperty(service, MaxmindIpToGeoService.MAXMIND_DATABASE_FILE_PATH, "ipToGeoService");
     * does not work if called after
     * runner.addControllerService("ipToGeoService", service);
     * and vice versa (runner controller service not implemented, so workaround for the moment)
     */
    /*
    private class MockMaxmindIpToGeoService extends MaxmindIpToGeoService {

        // Use a small test DB file we got from https://github.com/maxmind/MaxMind-DB/tree/master/test-data
        // to avoid embedding a big maxmind db in our workspace
        public void init(ControllerServiceInitializationContext context) throws InitializationException {

            File file = new File(getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getFile());
            dbPath = file.getAbsolutePath();
            lookupTime = true;
            super.init(context);
        }
    }
    */


}
