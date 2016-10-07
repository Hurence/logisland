package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.util.record.RecordSchemaUtil;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.RecordValidator;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.hurence.logisland.util.validator.AvroRecordValidator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SplitTextTest {


    private static final String DATA_USR_BACKEND_LOG = "/data/usr_backend_application.log";
    private static final String DATA_USR_GATEWAY_LOG = "/data/usr_gateway_application.log";
    private static final String DATA_USR_BACKEND_LOG2 = "/data/USR-fail2.log";


    private static final String USR_BACKEND_REGEX = "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:([^:]*):([^\\]]*)\\]\\s*\\[ACC:([^\\]]*)\\]\\[SRV:([^\\]]*)\\]\\s+(\\S*)\\s+(\\S*)\\s+(\\S*)\\s+(.*)\\s*";
    private static final String USR_GATEWAY_REGEX = "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:(\\S*)\\]\\s+(\\S*)\\s+(\\S*) - (.*)";


    private static final String USR_BACKEND_FIELDS = "component,record_time,player_type,session,user_id,srv,log_level,logger,none,trace";
    private static final String USR_GATEWAY_FIELDS = "component,record_time,session,log_level,logger,trace";


    private static Logger logger = LoggerFactory.getLogger(SplitTextTest.class);


    @Test
    public void testUsrBackend() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, USR_BACKEND_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, USR_BACKEND_FIELDS);
        testRunner.setProperty(SplitText.KEY_REGEX, "(\\S*):(\\S*)");
        testRunner.setProperty(SplitText.KEY_FIELDS, "es_index,host_name");
        testRunner.assertValid();

        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(DATA_USR_BACKEND_LOG));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(DATA_USR_BACKEND_LOG2));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(244);

        testRunner.setProperty(SplitText.VALUE_REGEX, USR_GATEWAY_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, USR_GATEWAY_FIELDS);
        testRunner.assertValid();
        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(DATA_USR_GATEWAY_LOG));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(201);
    }


    private static final String APACHE_LOG = "/data/localhost_access.log";
    private static final String APACHE_LOG_FIELDS = "src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out";
    private static final String APACHE_LOG_REGEX = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s+(\\S+)\"\\s+(\\S+)\\s+(\\S+)";
    private static final String APACHE_LOG_SCHEMA = "/schemas/apache_log.avsc";

    @Test
    public void testApacheLog() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, APACHE_LOG_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
        testRunner.assertValid();
        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(APACHE_LOG));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(200);


        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertRecordSizeEquals(9);
    }

    @Test
    public void testApacheLogWithoutRawContent() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        final RecordValidator avroValidator = new AvroRecordValidator(SplitTextTest.class.getResourceAsStream(APACHE_LOG_SCHEMA));
        testRunner.setProperty(SplitText.VALUE_REGEX, APACHE_LOG_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
        testRunner.setProperty(SplitText.KEEP_RAW_CONTENT, "false");
        testRunner.setProperty(SplitText.EVENT_TYPE, "apache_log");
        testRunner.assertValid();
        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(APACHE_LOG));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(200);
        testRunner.assertOutputErrorCount(0);

        MockRecord out = testRunner.getOutputRecords().get(0);

        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertFieldEquals("http_method", "GET");
        out.assertFieldEquals("bytes_out", "51");
        out.assertFieldEquals("http_query", "/usr/rest/account/email");
        out.assertFieldEquals("http_version", "HTTP/1.1");
        out.assertFieldEquals("identd", "-");
        out.assertFieldEquals("user", "-");
        out.assertFieldEquals(FieldDictionary.RECORD_TYPE, "apache_log");
        out.assertFieldEquals(FieldDictionary.RECORD_TIME, 1469342728000L);


        System.out.println(RecordSchemaUtil.generateTestCase(out));

        out.assertRecordSizeEquals(8);
        testRunner.assertAllRecords(avroValidator);
    }

    @Test
    public void testApacheLogWithBadRegex() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, "bad_regex.*");
        testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
        testRunner.setProperty(SplitText.KEEP_RAW_CONTENT, "true");
        testRunner.setProperty(SplitText.EVENT_TYPE, "apache_log");
        testRunner.assertValid();
        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(APACHE_LOG));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(200);
        testRunner.assertOutputErrorCount(200);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldNotExists("src_ip");
        out.assertFieldEquals(FieldDictionary.RECORD_RAW_VALUE, "10.3.10.134 - - [24/Jul/2016:08:45:28 +0200] \"GET /usr/rest/account/email HTTP/1.1\" 200 51");
        out.assertFieldEquals(FieldDictionary.RECORD_ERROR, ProcessError.REGEX_PARSING_ERROR.toString());
        out.assertRecordSizeEquals(2);
    }


}
