/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.util.record.RecordSchemaUtil;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.RecordValidator;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.hurence.logisland.validator.AvroRecordValidator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


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
        testRunner.setProperty(SplitText.RECORD_TYPE, "apache_log");
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
        out.assertFieldEquals("bytes_out", 51);
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
        testRunner.setProperty(SplitText.RECORD_TYPE, "apache_log");
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
        //out.assertFieldEquals(FieldDictionary.RECORD_ERRORS, ProcessError.REGEX_MATCHING_ERROR.toString());
        out.assertRecordSizeEquals(2);
    }


    @Test
    public void testAlternativeSingleMatch() {
        List<String> logs = new ArrayList<>();
        logs.add("10.3.10.134 - - [24/Jul/2016:08:45:29 +0200] \"GET /usr/rest/bank/purses?activeOnly=true HTTP/1.1\" 200 239");
        logs.add("10.3.10.134 - - [24/Jul/2016:08:45:29 +0200] \"GET /usr/rest/limits/moderato?siteCode=LOGISLAND_WEB HTTP/1.1\" 200 52");
        logs.add("10.3.10.134 200 52");

        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, APACHE_LOG_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
        testRunner.setProperty(SplitText.KEEP_RAW_CONTENT, "true");
        testRunner.setProperty(SplitText.RECORD_TYPE, "apache_log");
        testRunner.setProperty("alt.value.regex.1", "(\\S+)\\s*(\\S+)\\s*(\\S+)");
        testRunner.setProperty("alt.value.fields.1", "src_ip,http_status,bytes_out");
        testRunner.assertValid();
        logs.forEach(l -> testRunner.enqueue(null, l));
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertFieldEquals("http_method", "GET");
        out.assertFieldEquals("bytes_out", 239);
        out.assertFieldEquals("http_query", "/usr/rest/bank/purses?activeOnly=true");
        out.assertFieldEquals("http_version", "HTTP/1.1");
        out.assertFieldEquals("identd", "-");
        out.assertFieldEquals("user", "-");
        out.assertFieldEquals(FieldDictionary.RECORD_RAW_VALUE, "10.3.10.134 - - [24/Jul/2016:08:45:29 +0200] \"GET /usr/rest/bank/purses?activeOnly=true HTTP/1.1\" 200 239");
        //out.assertFieldEquals(FieldDictionary.RECORD_ERRORS, ProcessError.REGEX_MATCHING_ERROR.toString());
        out.assertRecordSizeEquals(9);

        out = testRunner.getOutputRecords().get(2);
        out.assertFieldNotExists("http_method");
        out.assertFieldNotExists("http_method");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertFieldEquals("http_status", 200);
        out.assertFieldEquals("bytes_out", 52);
        out.assertFieldEquals(FieldDictionary.RECORD_RAW_VALUE, "10.3.10.134 200 52");
        //out.assertFieldEquals(FieldDictionary.RECORD_ERRORS, ProcessError.REGEX_MATCHING_ERROR.toString());
        out.assertRecordSizeEquals(4);
    }

    @Test
    public void testAlternativeMultiMatch() {
        List<String> logs = new ArrayList<>();
        logs.add("10.3.10.134 - - [24/Jul/2016:08:45:29 +0200] \"GET /usr/rest/bank/purses?activeOnly=true HTTP/1.1\" 200 239");
        logs.add("[24/Jul/2016:08:45:29 +0200] 52");
        logs.add("10.3.10.134 200 52");

        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, APACHE_LOG_REGEX);
        testRunner.setProperty(SplitText.VALUE_FIELDS, APACHE_LOG_FIELDS);
        testRunner.setProperty(SplitText.KEEP_RAW_CONTENT, "true");
        testRunner.setProperty(SplitText.RECORD_TYPE, "apache_log");
        testRunner.setProperty("alt.value.regex.1", "(\\S+)\\s*(\\S+)\\s*(\\S+)");
        testRunner.setProperty("alt.value.fields.1", "src_ip,http_status,bytes_out");
        testRunner.setProperty("alt.value.regex.2", "\\[([\\w:/]+\\s[+\\-]\\d{4})\\]\\s+(\\S*)");
        testRunner.setProperty("alt.value.fields.2", "record_time,bytes_out");
        testRunner.assertValid();
        testRunner.enqueue(logs);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(3);
        testRunner.assertOutputErrorCount(0);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldExists("src_ip");
        out.assertFieldNotExists("src_ip2");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertFieldEquals("http_method", "GET");
        out.assertFieldEquals("bytes_out", 239);
        out.assertFieldEquals("http_query", "/usr/rest/bank/purses?activeOnly=true");
        out.assertFieldEquals("http_version", "HTTP/1.1");
        out.assertFieldEquals("identd", "-");
        out.assertFieldEquals("user", "-");
        out.assertFieldEquals(FieldDictionary.RECORD_RAW_VALUE, "10.3.10.134 - - [24/Jul/2016:08:45:29 +0200] \"GET /usr/rest/bank/purses?activeOnly=true HTTP/1.1\" 200 239");
        //out.assertFieldEquals(FieldDictionary.RECORD_ERRORS, ProcessError.REGEX_MATCHING_ERROR.toString());
        out.assertRecordSizeEquals(9);

        out = testRunner.getOutputRecords().get(1);
        out.assertFieldNotExists("http_query");
        out.assertFieldNotExists("http_method");
        out.assertFieldEquals(FieldDictionary.RECORD_TIME, 1469342729000L);
        out.assertFieldNotExists("http_status");
        out.assertFieldEquals("bytes_out", 52);
        out.assertFieldEquals(FieldDictionary.RECORD_RAW_VALUE, "[24/Jul/2016:08:45:29 +0200] 52");
        //out.assertFieldEquals(FieldDictionary.RECORD_ERRORS, ProcessError.REGEX_MATCHING_ERROR.toString());
        out.assertRecordSizeEquals(2);

        out = testRunner.getOutputRecords().get(2);
        out.assertFieldNotExists("http_method");
        out.assertFieldNotExists("http_query");
        out.assertFieldEquals("src_ip", "10.3.10.134");
        out.assertFieldEquals("http_status", 200);
        out.assertFieldEquals("bytes_out", 52);
        out.assertFieldEquals(FieldDictionary.RECORD_RAW_VALUE, "10.3.10.134 200 52");
        //out.assertFieldEquals(FieldDictionary.RECORD_ERRORS, ProcessError.REGEX_MATCHING_ERROR.toString());
        out.assertRecordSizeEquals(4);
    }

    @Test
    public void testSpecificAlternativematch() {
        List<String> logs = new ArrayList<>();
        logs.add("Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 77.154.202.48");
        logs.add("Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        logs.add("Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        logs.add("Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        logs.add("Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        logs.add("Jan 24 19:59:56 EagleP12.prod.hurence.fr/EagleP12.prod.hurence.fr 2017 Jan 24 19:59:56 INFO SSL: Host www.hurence.fr received fin without close notify alert from 92.143.11.69");

        final TestRunner testRunner = TestRunners.newTestRunner(new SplitText());
        testRunner.setProperty(SplitText.VALUE_REGEX, "(\\w{3}\\s+\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2})\\s([\\.\\w]+)\\/([\\.\\w]+)\\s(\\d{4}\\s+\\w{3}\\s\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\s+(\\w+)\\s+(PROXY_LOG)\\s\\[(.{26})\\]\\s\\d+\\s\\\"([^\"]+)\\\"\\s+(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s->\\s([\\w\\.{0,1}\\-]+)\\s->\\s(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s(\\d+)\\s->\\s+(\\d+|\\-)\\s(\\d+\\.\\d+\\.\\d+\\.\\d+|-)\\s(\\d+|\\-)\\s+\\\"\\s+([\\d\\.]+)\\s+([\\w]+)\\s(\\d+)\\s(\\w+)\\s(\\S+)\\s(\\S+)\\s(\\S+)\\s[^\\\"]*\\\"\\s+([\\w\\/\\-\\.\\+]+)\\s(\\d+)\\s*");
        testRunner.setProperty(SplitText.VALUE_FIELDS, "raw_date,host1,host2,raw_date2,level,source,record_time,http_user_agent,src_ip,host_name,host_ip,host_port,tunnel_local_port,tunnel_remote_local_address,tunnel_remote_port,http_version,http_method,http_result_code,http_result,http_uri,http_query,http_referrer,http_content_type,bytes_out");
        testRunner.setProperty(SplitText.KEEP_RAW_CONTENT, "true");
        testRunner.setProperty(SplitText.RECORD_TYPE, "apache_log");



        testRunner.setProperty("alt.value.regex.1", "(\\w{3}\\s+\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2})\\s([\\.\\w]+)\\/([\\.\\w]+)\\s(\\d{4}\\s+\\w{3}\\s\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\s+(\\w+)\\s+(\\w+)\\:\\s+(.*)");
        testRunner.setProperty("alt.value.fields.1", "raw_date,host1,host2,record_time,level,source,message");
        testRunner.setProperty("alt.value.regex.2", "(\\w{3}\\s+\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2})\\s([\\.\\w]+)\\/([\\.\\w]+)\\s(\\d{4}\\s+\\w{3}\\s\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\s+(\\w+)\\s+(.*)");
        testRunner.setProperty("alt.value.fields.2", "raw_date,host1,host2,record_time,level,message");
        testRunner.assertValid();
        testRunner.enqueue(logs);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(6);
        testRunner.assertOutputErrorCount(0);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldEquals("host1", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("host2", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("level", "INFO");
        out.assertFieldEquals("message", "SSL: Host www.hurence.fr received fin without close notify alert from 77.154.202.48");
        out.assertFieldEquals("raw_date", "Jan 17 18:52:18");
        out.assertFieldEquals("record_raw_value", "Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 77.154.202.48");
        out.assertFieldEquals("record_time", 1484679138000L);
        out.assertFieldEquals("record_type", "apache_log");
        out.assertRecordSizeEquals(6);

        out = testRunner.getOutputRecords().get(1);
        out.assertFieldEquals("host1", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("host2", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("level", "INFO");
        out.assertFieldEquals("message", "SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("raw_date", "Jan 17 18:52:18");
        out.assertFieldEquals("record_raw_value", "Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("record_time", 1484679138000L);
        out.assertFieldEquals("record_type", "apache_log");
        out.assertRecordSizeEquals(6);

        out = testRunner.getOutputRecords().get(2);
        out.assertFieldEquals("host1", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("host2", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("level", "INFO");
        out.assertFieldEquals("message", "SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("raw_date", "Jan 17 18:52:18");
        out.assertFieldEquals("record_raw_value", "Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("record_time", 1484679138000L);
        out.assertFieldEquals("record_type", "apache_log");
        out.assertRecordSizeEquals(6);

        out = testRunner.getOutputRecords().get(3);
        out.assertFieldEquals("host1", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("host2", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("level", "INFO");
        out.assertFieldEquals("message", "SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("raw_date", "Jan 17 18:52:18");
        out.assertFieldEquals("record_raw_value", "Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("record_time", 1484679138000L);
        out.assertFieldEquals("record_type", "apache_log");
        out.assertRecordSizeEquals(6);

        out = testRunner.getOutputRecords().get(4);
        out.assertFieldEquals("host1", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("host2", "EagleP13.prod.hurence.fr");
        out.assertFieldEquals("level", "INFO");
        out.assertFieldEquals("message", "SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("raw_date", "Jan 17 18:52:18");
        out.assertFieldEquals("record_raw_value", "Jan 17 18:52:18 EagleP13.prod.hurence.fr/EagleP13.prod.hurence.fr 2017 Jan 17 18:52:18  INFO    SSL: Host www.hurence.fr received fin without close notify alert from 92.130.95.204");
        out.assertFieldEquals("record_time", 1484679138000L);
        out.assertFieldEquals("record_type", "apache_log");
        out.assertRecordSizeEquals(6);


        out = testRunner.getOutputRecords().get(5);
        out.assertFieldEquals("host1", "EagleP12.prod.hurence.fr");
        out.assertFieldEquals("host2", "EagleP12.prod.hurence.fr");
        out.assertFieldEquals("level", "INFO");
        out.assertFieldEquals("message", "SSL: Host www.hurence.fr received fin without close notify alert from 92.143.11.69");
        out.assertFieldEquals("raw_date", "Jan 24 19:59:56");
        out.assertFieldEquals("record_raw_value", "Jan 24 19:59:56 EagleP12.prod.hurence.fr/EagleP12.prod.hurence.fr 2017 Jan 24 19:59:56 INFO SSL: Host www.hurence.fr received fin without close notify alert from 92.143.11.69");
        out.assertFieldEquals("record_time", 1485287996000L);
        out.assertFieldEquals("record_type", "apache_log");
        out.assertRecordSizeEquals(6);
    }



}
