package com.hurence.logisland.processor;

import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SplitTextTest {

    private static final String DATA_USR_BACKEND_LOG = "/data/usr_backend_application.log";
    private static final String DATA_USR_GATEWAY_LOG = "/data/usr_gateway_application.log";
    private static final String DATA_USR_BACKEND_LOG2 = "/data/USR-fail2.log";
    private static final String DATA_TRAKER1_LOG = "/data/traker1_with_key.log";


    private static final String USR_BACKEND_REGEX = "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:([^:]*):([^\\]]*)\\]\\s*\\[ACC:([^\\]]*)\\]\\[SRV:([^\\]]*)\\]\\s+(\\S*)\\s+(\\S*)\\s+(\\S*)\\s+(.*)\\s*";
    private static final String USR_GATEWAY_REGEX = "\\[(\\S*)\\]\\s+(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})\\s+\\[SES:(\\S*)\\]\\s+(\\S*)\\s+(\\S*) - (.*)";
    private static final String SYSLOG_TRAKER_REGEX = "(\\D{3}\\s+\\d{1,2} \\d{2}:\\d{2}:\\d{2})\\s+(\\S*)\\s+(?:date=(\\S*)\\s+)?(?:time=(\\S*)\\s+)?(?:devname=(\\S*)\\s+)?(?:devid=(\\S*)\\s+)?(?:logid=(\\S*)\\s+)?(?:type=(\\S*)\\s+)?(?:subtype=(\\S*)\\s+)?(?:level=(\\S*)\\s+)?(?:vd=(\\S*)\\s+)?(?:srcip=(\\S*)\\s+)?(?:srcport=(\\S*)\\s+)?(?:srcintf=(\\S*)\\s+)?(?:dstip=(\\S*)\\s+)?(?:dstport=(\\S*)\\s+)?(?:dstintf=(\\S*)\\s+)?(?:poluuid=(\\S*)\\s+)?(?:sessionid=(\\S*)\\s+)?(?:proto=(\\S*)\\s+)?(?:action=(\\S*)\\s+)?(?:policyid=(\\S*)\\s+)?(?:dstcountry=(\\S*)\\s+)?(?:srccountry=(\\S* \\S*)\\s+)?(?:trans*disp=(\\S*)\\s+)?(?:trans*ip=(\\S*)\\s+)?(?:trans*port=(\\S*)\\s+)?(?:service=(\\S*)\\s+)?(?:duration=(\\S*)\\s+)?(?:sentbyte=(\\S*)\\s+)?(?:rcvdbyte=(\\S*)\\s+)?(?:sentpkt=(\\S*)\\s+)?(?:rcvdpkt=(\\S*)\\s+)?(?:appcat=(\\S*)\\s+)?(?:crscore=(\\S*)\\s+)?(?:craction=(\\S*)\\s+)?(?:crlevel=(\\S*)\\s+)?";

    private static final String USR_BACKEND_FIELDS = "raw_content,component,event_time,player_type,session,user_id,srv,log_level,logger,none,trace";
    private static final String USR_GATEWAY_FIELDS = "raw_content,component,event_time,session,log_level,logger,trace";
    private static final String SYSLOG_TRAKER_FIELDS = "raw_content,line_date,host,date,time,devname,devid,logid,type,subtype,level,vd,src_ip,src_port,src_inf,dest_ip,dest_port,dest_inf,pol_uuid,session_id,proto,action,policy_id,dest_country,src_country,tran_isp,tran_ip,tran_port,service,duration,bytes_out,bytes_in,packets_out,packets_in,app_cat,cr_score,cr_action,cr_level";

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


        testRunner.setProperty(SplitText.VALUE_REGEX, SYSLOG_TRAKER_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, SYSLOG_TRAKER_FIELDS);
        testRunner.assertValid();
        testRunner.enqueue("@", SplitTextTest.class.getResourceAsStream(DATA_TRAKER1_LOG));
        testRunner.clearOutpuRecords();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1000);


        testRunner.setProperty("bad_prop", "nasty man");
        testRunner.assertNotValid();
    }


}
