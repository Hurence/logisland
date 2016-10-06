package com.hurence.logisland.processor;

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
    private static final String DATA_TRAKER1_LOG = "/data/traker1_with_key.log";
    private static final String SCHEMA_TRAKER1_LOG = "/schemas/traker.avsc";

    private static final String USR_BACKEND_REGEX = "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:([^:]*):([^\\]]*)\\]\\s*\\[ACC:([^\\]]*)\\]\\[SRV:([^\\]]*)\\]\\s+(\\S*)\\s+(\\S*)\\s+(\\S*)\\s+(.*)\\s*";
    private static final String USR_GATEWAY_REGEX = "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:(\\S*)\\]\\s+(\\S*)\\s+(\\S*) - (.*)";
    private static final String SYSLOG_TRAKER_REGEX = "(\\D{3}\\s+\\d{1,2} \\d{2}:\\d{2}:\\d{2})\\s+(\\S*)\\s+(?:date=(\\S*)\\s+)?(?:time=(\\S*)\\s+)?(?:devname=(\\S*)\\s+)?(?:devid=(\\S*)\\s+)?(?:logid=(\\S*)\\s+)?(?:type=(\\S*)\\s+)?(?:subtype=(\\S*)\\s+)?(?:level=(\\S*)\\s+)?(?:vd=(\\S*)\\s+)?(?:srcip=(\\S*)\\s+)?(?:srcport=(\\S*)\\s+)?(?:srcintf=\"(\\S*)\"\\s+)?(?:dstip=(\\S*)\\s+)?(?:dstport=(\\S*)\\s+)?(?:dstintf=\"(\\S*)\"\\s+)?(?:poluuid=(\\S*)\\s+)?(?:sessionid=(\\S*)\\s+)?(?:proto=(\\S*)\\s+)?(?:action=(\\S*)\\s+)?(?:policyid=(\\S*)\\s+)?(?:dstcountry=\"(\\S*)\"\\s+)?(?:srccountry=\"(\\S*\\s*\\S*)\"\\s+)?(?:trans*disp=(\\S*)\\s+)?(?:trans*ip=(\\S*)\\s+)?(?:trans*port=(\\S*)\\s+)?(?:service=\"(\\S*)\"\\s+)?(?:duration=(\\S*)\\s+)?(?:sentbyte=(\\S*)\\s+)?(?:rcvdbyte=(\\S*)\\s+)?(?:sentpkt=(\\S*)\\s+)?(?:rcvdpkt=(\\S*)\\s+)?(?:appcat=\"(\\S*)\"\\s+)?(?:crscore=(\\S*)\\s+)?(?:craction=(\\S*)\\s+)?(?:crlevel=(\\S*)\\s+)?";


    private static final String USR_BACKEND_FIELDS = "component,event_time,player_type,session,user_id,srv,log_level,logger,none,trace";
    private static final String USR_GATEWAY_FIELDS = "component,event_time,session,log_level,logger,trace";
    private static final String SYSLOG_TRAKER_FIELDS = "line_date,host,date,time,devname,devid,logid,type,subtype,level,vd,src_ip,src_port,src_inf,dest_ip,dest_port,dest_inf,pol_uuid,session_id,proto,action,policy_id,dest_country,src_country,tran_isp,tran_ip,tran_port,service,duration,bytes_out,bytes_in,packets_out,packets_in,app_cat,cr_score,cr_action,cr_level";

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
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(DATA_USR_BACKEND_LOG2));
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(8);

        testRunner.setProperty(SplitText.VALUE_REGEX, USR_GATEWAY_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, USR_GATEWAY_FIELDS);
        testRunner.assertValid();
        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(DATA_USR_GATEWAY_LOG));
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(73);
    }

    @Test
    public void testSyslogTraker() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, SYSLOG_TRAKER_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, SYSLOG_TRAKER_FIELDS);
        testRunner.setProperty(SplitText.KEY_REGEX, "(\\S*):(\\S*)");
        testRunner.setProperty(SplitText.KEY_FIELDS, "es_index,host_name");
        testRunner.assertValid();
        testRunner.enqueue("@", SplitTextTest.class.getResourceAsStream(DATA_TRAKER1_LOG));
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1000);


        MockRecord out = testRunner.getOutpuRecords().get(0);
        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.41.100");
        out.assertRecordSizeEquals(37);

      /*  testRunner.assertAllRecords(
                new AvroRecordValidator(SplitTextTest.class.getResourceAsStream(SCHEMA_TRAKER1_LOG))
        );*/

        testRunner.setProperty("bad_prop", "nasty man");
        testRunner.assertNotValid();
    }


    private static final String APACHE_LOG = "/data/localhost_access.log";
    private static final String APACHE_LOG_FIELDS = "src_ip,identd,user,event_time,http_method,http_query,http_version,http_status,bytes_out";
    private static final String APACHE_LOG_REGEX = "(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s+(\\S+)\"\\s+(\\S+)\\s+(\\S+)";
    private static final String APACHE_LOG_SCHEMA = "/schemas/apache_log.avsc";

    @Test
    public void testApacheLog() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, APACHE_LOG_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
        testRunner.assertValid();
        testRunner.enqueue(SplitTextTest.class.getResourceAsStream(APACHE_LOG));
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(200);


        MockRecord out = testRunner.getOutpuRecords().get(0);
        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertRecordSizeEquals(10);
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
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(200);


        MockRecord out = testRunner.getOutpuRecords().get(0);
        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertRecordSizeEquals(9);
        testRunner.assertAllRecords(avroValidator);

    }


}
