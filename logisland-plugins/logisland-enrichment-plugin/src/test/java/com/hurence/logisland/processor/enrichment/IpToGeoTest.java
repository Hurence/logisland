
package com.hurence.logisland.processor.enrichment;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.service.iptogeo.IpToGeoService;
import com.hurence.logisland.service.iptogeo.maxmind.MaxmindIpToGeoService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.hurence.logisland.service.iptogeo.IpToGeoService.*;

public class IpToGeoTest {

    private static Logger logger = LoggerFactory.getLogger(IpToGeoTest.class);
    private static final String IP_ADDRESS_FIELD_NAME = "ip";

    public static final String KEY = "some key";
    public static final String VALUE = "some content";

    private Record getRecordWithStringIp(String ip) {
        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord(KEY, VALUE));
        inputRecord.setStringField(IP_ADDRESS_FIELD_NAME, ip);
        return inputRecord;
    }

    @Test
    public void testValidIp() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("81.2.69.142");
        runner.enqueue(inputRecord);
        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord = runner.getOutputRecords().get(0);

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_CITY);
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_CITY, "London");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_LATITUDE);
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_LATITUDE, "51.5142");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_LONGITUDE);
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_LONGITUDE, "-0.0931");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_LOCATION);
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_LOCATION, "51.5142,-0.0931");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_SUBDIVISION + "0");
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_SUBDIVISION + "0", "England");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_SUBDIVISION_ISOCODE + "0");
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_SUBDIVISION_ISOCODE + "0", "ENG");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_COUNTRY);
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_COUNTRY, "United Kingdom");

        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_COUNTRY_ISOCODE);
        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_COUNTRY_ISOCODE, "GB");

//        outputRecord.assertFieldExists(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_POSTALCODE);
//        outputRecord.assertFieldEquals(IP_ADDRESS_FIELD_NAME + SEPARATOR + GEO_FIELD_POSTALCODE, "78218");

        outputRecord.assertFieldNotExists(ProcessError.RUNTIME_ERROR.toString());
    }

    private TestRunner getTestRunner() throws InitializationException {

        final TestRunner runner = TestRunners.newTestRunner(IpToGeo.class);
        runner.setProperty(IpToGeo.IP_ADDRESS_FIELD, IP_ADDRESS_FIELD_NAME);
        runner.setProperty(IpToGeo.IP_TO_GEO_SERVICE, "ipToGeoService");

        // create the controller service and link it to the test processor
        final IpToGeoService service = (IpToGeoService)new MockMaxmindIpToGeoService();
        runner.addControllerService("ipToGeoService", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        return runner;
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

        // Use a small test DB file we got from https://github.com/maxmind/MaxMind-DB/tree/master/test-data
        // to avoid embedding a big maxmind db in our workspace
        public void init(ControllerServiceInitializationContext context) throws InitializationException {

            File file = new File(getClass().getClassLoader().getResource("GeoIP2-City-Test.mmdb").getFile());
            dbPath = file.getAbsolutePath();
            super.init(context);
        }
    }
}
