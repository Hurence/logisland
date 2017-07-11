
package com.hurence.logisland.processor.enrichment;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.service.cache.LRUKeyValueCacheService;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

import java.io.IOException;

public class IpToFqdnTest {

    private static Logger logger = LoggerFactory.getLogger(IpToFqdnTest.class);
    public static final String FQDN_FIELD_NAME = "domain";
    public static final String IP_ADDRESS_FIELD_NAME = "ip";
    public static final String OVERRIDE_FQDN = "true";

    public static final long TIME_PROCESSING_FOR_TEN_RECORDS_WITH_SAME_IP_MAX_IN_MILLISECOND = 4000;
    public static final long TIME_PROCESSING_FOR_TEN_RECORDS_MAX_IN_MILLISECOND_FIRST_TIME = 7*3000;
    public static final long TIME_PROCESSING_FOR_TEN_RECORDS_MAX_IN_MILLISECOND_SECOND_TIME = 100;

    public static final long TIME_PROCESSING_INVALID_IP_MAX_IN_MILLISECOND = 100;
    public static final long TIME_PROCESSING_UNAUTHORIZED_RESOLUTION_MAX_IN_MILLISECOND = 4000;


    public static final String KEY = "some key";
    public static final String VALUE = "some content";

    @Test
    public void testOneIp() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("216.58.209.238");
        runner.enqueue(inputRecord);
        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord = runner.getOutputRecords().get(0);
        outputRecord.assertFieldExists(FQDN_FIELD_NAME);
        outputRecord.assertFieldNotEquals(FQDN_FIELD_NAME, "216.58.209.238");
        outputRecord.assertStringFieldEndWith(FQDN_FIELD_NAME, ".1e100.net");//several machine can be linked to
        outputRecord.assertFieldNotExists(ProcessError.RUNTIME_ERROR.toString());
    }

    @Test
    public void test10IpWithSomeNoMatch() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("216.58.209.238");
        final Record inputRecord2 = getRecordWithStringIp("216.58.209.48");
        final Record inputRecord3 = getRecordWithStringIp("216.58.209.48");
        final Record inputRecord4 = getRecordWithStringIp("216.59.209.238");
        final Record inputRecord5 = getRecordWithStringIp("18.44.209.238");
        final Record inputRecord6 = getRecordWithStringIp("158.58.209.238");
        final Record inputRecord7 = getRecordWithStringIp("222.58.209.238");
        final Record inputRecord8 = getRecordWithStringIp("222.58.209.238");
        final Record inputRecord9 = getRecordWithStringIp("222.58.209.238");
        final Record inputRecord10 = getRecordWithStringIp("135.60.209.238");
        final Record inputRecord11 = getRecordWithStringIp("2001:db8:1:1a0::");
        final Record inputRecord12 = getRecordWithStringIp("2001:db8:1:1a0::/59");


        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4,
                inputRecord5, inputRecord6, inputRecord7, inputRecord8 ,inputRecord9, inputRecord10,
                inputRecord11, inputRecord12);

        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();

        runner.assertAllInputRecordsProcessed();

        long lastedInMilliseconds = end - start;
        String msg = "processing should take less than '" + TIME_PROCESSING_FOR_TEN_RECORDS_MAX_IN_MILLISECOND_FIRST_TIME +
                "' millisecond. It lasted '" + lastedInMilliseconds + "' millisecond.";
        Assert.assertTrue( msg, lastedInMilliseconds < TIME_PROCESSING_FOR_TEN_RECORDS_MAX_IN_MILLISECOND_FIRST_TIME);


        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4,
                inputRecord5, inputRecord6, inputRecord7, inputRecord8 ,inputRecord9, inputRecord10,
                inputRecord11, inputRecord12);

        start = System.currentTimeMillis();

        runner.run();

        end = System.currentTimeMillis();

        runner.assertAllInputRecordsProcessed();

        lastedInMilliseconds = end - start;
        msg = "processing should take less than '" + TIME_PROCESSING_FOR_TEN_RECORDS_MAX_IN_MILLISECOND_SECOND_TIME +
                "' millisecond. It lasted '" + lastedInMilliseconds + "' millisecond.";
        Assert.assertTrue( msg, lastedInMilliseconds < TIME_PROCESSING_FOR_TEN_RECORDS_MAX_IN_MILLISECOND_SECOND_TIME);
    }

    @Test
    public void test10TimeSameIpThatDoesNotMatch() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord2 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord3 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord4 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord5 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord6 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord7 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord8 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord9 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord10 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord11 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord12 = getRecordWithStringIp("1.2.4.6");


        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4,
                inputRecord5, inputRecord6, inputRecord7, inputRecord8 ,inputRecord9, inputRecord10,
                inputRecord11, inputRecord12);

        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();

        runner.assertAllInputRecordsProcessed();

        //less than 1 second to treat those ten records
        long lastedInMilliseconds = end - start;
        String msg = "processing should take less than '" + TIME_PROCESSING_FOR_TEN_RECORDS_WITH_SAME_IP_MAX_IN_MILLISECOND +
                "' millisecond. It lasted '" + lastedInMilliseconds + "' millisecond.";
        Assert.assertTrue( msg, lastedInMilliseconds < TIME_PROCESSING_FOR_TEN_RECORDS_WITH_SAME_IP_MAX_IN_MILLISECOND);
    }

    @Test
    public void testSameIpSeveralTime() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("216.58.209.238");

        long firstRun = processRecordIn(inputRecord, runner);
        String msg = "process of the ip should not be superior to 2 seconds";
        Assert.assertTrue(msg, firstRun < 2000);

        msg = "process of the same ip should not differ more than 1 second";
        for (int i=0; i < 10; i++) {
            long diff = firstRun - processRecordIn(inputRecord, runner);
            Assert.assertTrue(msg, diff < 1000);
        }
    }

    @Test
    public void testFakeIp() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("2106.058.209.238");// par10s29-in-f238.1e100.net
        runner.enqueue(inputRecord);
        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();
        runner.assertAllInputRecordsProcessed();

        String msg = "processing a string that is not a valid ip should be faster than '" + TIME_PROCESSING_INVALID_IP_MAX_IN_MILLISECOND + "' ms";
        Assert.assertTrue(msg, end - start < TIME_PROCESSING_INVALID_IP_MAX_IN_MILLISECOND);

        final MockRecord outputRecord = runner.getOutputRecords().get(0);
        outputRecord.assertFieldNotExists(FQDN_FIELD_NAME);
        outputRecord.assertFieldNotExists(ProcessError.RUNTIME_ERROR.toString());
    }

    /*
    It may be because the dns resolution is not authorized for us for this ip.
     */
    @Test
    public void testIpThatDoesNotMatch() throws InitializationException {
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("1.2.4.6");// par10s29-in-f238.1e100.net
        runner.enqueue(inputRecord);
        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord = runner.getOutputRecords().get(0);
        outputRecord.assertFieldNotExists(FQDN_FIELD_NAME);
        outputRecord.assertFieldNotExists(ProcessError.RUNTIME_ERROR.toString());

        String msg = "processing an ip which we do not have permission to resolve domain name should be faster than '" + TIME_PROCESSING_UNAUTHORIZED_RESOLUTION_MAX_IN_MILLISECOND + "' ms";
        Assert.assertTrue(msg, end - start < TIME_PROCESSING_UNAUTHORIZED_RESOLUTION_MAX_IN_MILLISECOND);
    }


    @Test
    public void testCache() throws InitializationException  {
        final long TIME_MAX_PROCESSING_FIRST_DATA = 5*3000;
        final long TIME_MAX_PROCESSING_SECOND_DATA = 10;
        final TestRunner runner = getTestRunner();
        runner.setProperty("cache.size", "5");

        final Record inputRecord = getRecordWithStringIp("1.2.4.6");// par10s29-in-f238.1e100.net
        final Record inputRecord2 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord3 = getRecordWithStringIp("216.58.209.48");
        final Record inputRecord4 = getRecordWithStringIp("216.59.209.238");
        final Record inputRecord5 = getRecordWithStringIp("18.44.209.238");

        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4, inputRecord5);
        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();
        runner.assertAllInputRecordsProcessed();

        String msg = "processing an ip which we do not have permission to resolve domain name should be faster than '" + TIME_MAX_PROCESSING_FIRST_DATA + "' ms";
        Assert.assertTrue(msg, end - start < TIME_MAX_PROCESSING_FIRST_DATA);

        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4, inputRecord5);
        start = System.currentTimeMillis();

        runner.run();

        end = System.currentTimeMillis();
        runner.assertAllInputRecordsProcessed();

        msg = "processing an ip which we do not have permission to resolve domain name should be faster than '" + TIME_MAX_PROCESSING_SECOND_DATA + "' ms";
        Assert.assertTrue(msg, end - start < TIME_MAX_PROCESSING_SECOND_DATA);
    }

    @Test
    public void testNoCacheCache() throws InitializationException {
        final long TIME_MAX_PROCESSING_FIRST_DATA = 5*3000;
        final TestRunner runner = getTestRunner();

        final Record inputRecord = getRecordWithStringIp("1.2.4.6");// par10s29-in-f238.1e100.net
        final Record inputRecord2 = getRecordWithStringIp("1.2.4.6");
        final Record inputRecord3 = getRecordWithStringIp("216.58.209.48");
        final Record inputRecord4 = getRecordWithStringIp("216.59.209.238");
        final Record inputRecord5 = getRecordWithStringIp("18.44.209.238");

        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4, inputRecord5);
        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();
        runner.assertAllInputRecordsProcessed();

        String msg = "processing an ip which we do not have permission to resolve domain name should be faster than '" + TIME_MAX_PROCESSING_FIRST_DATA + "' ms";
        Assert.assertTrue(msg, end - start < TIME_MAX_PROCESSING_FIRST_DATA);

        runner.enqueue(inputRecord, inputRecord2, inputRecord3, inputRecord4, inputRecord5);
        start = System.currentTimeMillis();

        runner.run();

        end = System.currentTimeMillis();
        runner.assertAllInputRecordsProcessed();

        msg = "processing an ip which we do not have permission to resolve domain name should be faster than '" + TIME_MAX_PROCESSING_FIRST_DATA + "' ms";
        Assert.assertTrue(msg, end - start < TIME_MAX_PROCESSING_FIRST_DATA);
    }
    /**
     * Process a record and return the time that it took
     * @param record
     * @return
     */
    private long processRecordIn(Record record, TestRunner runner) {
        runner.enqueue(record);

        long start = System.currentTimeMillis();

        runner.run();

        long end = System.currentTimeMillis();

        runner.assertAllInputRecordsProcessed();

        return end-start;
    }

    private TestRunner getTestRunner() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(IpToFqdn.class);
        runner.setProperty(IpToFqdn.FQDN_FIELD, FQDN_FIELD_NAME);
        runner.setProperty(IpToFqdn.IP_ADDRESS_FIELD, IP_ADDRESS_FIELD_NAME);
        runner.setProperty(IpToFqdn.OVERRIDE_FQDN, OVERRIDE_FQDN);

        final MockCacheService<String, String> cacheService = new MockCacheService(20);
        runner.addControllerService("cacheService", cacheService);
        runner.enableControllerService(cacheService);
        runner.setProperty(IpToFqdn.CACHE_SERVICE, "cacheService");

        return runner;
    }

    private Record getRecordWithStringIp(String ip) {
        final Record inputRecord = new MockRecord(RecordUtils.getKeyValueRecord(KEY, VALUE));
        inputRecord.setStringField(IP_ADDRESS_FIELD_NAME, ip);
        return inputRecord;
    }


    @Test
    public void testValidatorIpV4() {
        //VALID
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("2"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("255"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("024.003"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("024.3.78"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("255.255.255.255"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("1.3.78.159"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("024.003.78.159"));

        //VALID, BUT VALIDITY IS JUSTIFIED OR NOT ?
        //It seems that it supports greater integer than 255 for most right part. It convert it in several bytes for completing address
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("125.1288"));
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("256"));//valid , is it okay ? probably converting integer into bytes
        Assert.assertTrue(IPAddressUtil.isIPv4LiteralAddress("257"));//valid , is it okay ? probably converting integer into bytes

        //NOT VALID
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress("a"));
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress("256.1"));
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress("125.1288.123"));
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress("125.128.123.255.1"));
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress("125.128.123.2551"));
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress("1288.128"));
        Assert.assertFalse(IPAddressUtil.isIPv4LiteralAddress(""));
    }

    @Test
    public void testValidatorIpV6() {
        //VALID
        Assert.assertTrue(IPAddressUtil.isIPv6LiteralAddress("2::"));
        Assert.assertTrue(IPAddressUtil.isIPv6LiteralAddress("::255"));
        Assert.assertTrue(IPAddressUtil.isIPv6LiteralAddress("02f4::0a03"));
        Assert.assertTrue(IPAddressUtil.isIPv6LiteralAddress("024F:a3:B78::"));
        Assert.assertTrue(IPAddressUtil.isIPv6LiteralAddress("1234:45af:1234:1234:1234:1234:1234:1234"));

        //NOT VALID
        Assert.assertFalse(IPAddressUtil.isIPv6LiteralAddress("g485::"));
        Assert.assertFalse(IPAddressUtil.isIPv6LiteralAddress("z12::"));
        Assert.assertFalse(IPAddressUtil.isIPv6LiteralAddress("z12::1::"));
        Assert.assertFalse(IPAddressUtil.isIPv6LiteralAddress("1234:45af:1234:1234:1234:1234:1234:1234:abcd"));
        Assert.assertFalse(IPAddressUtil.isIPv6LiteralAddress(""));
    }

    private class MockCacheService<K,V> extends LRUKeyValueCacheService<K,V> {

        private int cacheSize;

        public MockCacheService(final int cacheSize) {
            this.cacheSize = cacheSize;
        }

        @Override
        protected Cache<K, V> createCache(ControllerServiceInitializationContext context) throws IOException, InterruptedException {
            return new LRUCache<K,V>(cacheSize);
        }
    }

}
