/**
 * Copyright (C) 2017 Hurence 
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
package com.hurence.logisland.processor;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SendMail processor.
 * WARNING: These tests require a real SMTP server so for not having credentials hardcoded here,
 * there are not filled. To run this test:
 * - enable the tests by uncommenting //@Test into @Test for each method
 * - Fill the parameters marked with // <----- FILL ME
 * - For the test to be successful, you should receive in the configured mailbox one mail per test method
 *   (test method is in the mail body). 
 */
public class SendMailTest {
    
    private static Logger logger = LoggerFactory.getLogger(SendMailTest.class);

    /**
     * WARNING: Those values need to be filled so that the test can be run
     */
    private static final String TEST_SMTP_SERVER = "smtp.fillme.com"; // <----- FILL ME
    private static final String TEST_SMTP_PORT = "465"; // <----- FILL ME
    private static final String TEST_SMTP_SECURITY_USERNAME = "fill.me"; // <----- FILL ME
    private static final String TEST_SMTP_SECURITY_PASSWORD = "fillMePassword"; // <----- FILL ME
    private static final String TEST_SMTP_SECURITY_SSL = "true"; // <----- FILL ME
    private static final String TEST_MAIL_FROM_ADDRESS = "tester@logisland.net (must be a valid address!)"; // <----- FILL ME
    private static final String TEST_MAIL_FROM_NAME = "SendMail Processor Tester";
    private static final String TEST_MAIL_BOUNCE_ADDRESS = "bounce@logisland.net (must be a valid address!)"; // <----- FILL ME
    private static final String TEST_MAIL_REPLYTO_ADDRESS = "replyto@logisland.net (must be a valid address!)"; // <----- FILL ME
    private static final String TEST_MAIL_SUBJECT = "Logisland SendMail Processor Test";
    private static final String TEST_MAIL_TO = "tester@logisland.net (must be a valid address where you will receive mails!)"; // <----- FILL ME
    
    // Parameters for the HTML template
    private static final String PARAM_USER = "param_user";
    private static final String PARAM_DATE = "param_date";    

    //@Test
    public void testTextMailFromConfig() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SendMail());
        testRunner.setProperty(SendMail.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(SendMail.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(SendMail.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(SendMail.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(SendMail.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(SendMail.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        testRunner.setProperty(SendMail.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_SUBJECT, TEST_MAIL_SUBJECT);
        testRunner.setProperty(SendMail.MAIL_TO, TEST_MAIL_TO);
        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(SendMail.FIELD_MAIL_TEXT, "testBasicFromConfig:\nThis is the record message.");
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
    
    //@Test
    public void testTextMailFromRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SendMail());
        testRunner.setProperty(SendMail.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(SendMail.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(SendMail.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(SendMail.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(SendMail.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(SendMail.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);
        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(SendMail.FIELD_MAIL_TEXT, "testBasicFromRecord:\nThis is the record message.");
        record.setStringField(SendMail.FIELD_MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        record.setStringField(SendMail.FIELD_MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        record.setStringField(SendMail.FIELD_MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        record.setStringField(SendMail.FIELD_MAIL_SUBJECT, TEST_MAIL_SUBJECT + " (from record)");
        record.setStringField(SendMail.FIELD_MAIL_TO, TEST_MAIL_TO);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
    
    //@Test
    public void testHtmlMailFromConfig() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SendMail());
        testRunner.setProperty(SendMail.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(SendMail.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(SendMail.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(SendMail.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(SendMail.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(SendMail.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        testRunner.setProperty(SendMail.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_SUBJECT, TEST_MAIL_SUBJECT);
        testRunner.setProperty(SendMail.MAIL_TO, TEST_MAIL_TO);

        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(SendMail.FIELD_MAIL_TEXT, "testHtmlMailFromConfig:\nThis is the text message which"
                + " is an alternative to HTML one.");
        
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html>");
        sb.append("<html>");
        sb.append("<body>");
        sb.append("Here is a...");
        sb.append("<h2>Spectacular Image for you!</h2>");
        sb.append("<img src=\"http://www.apache.org/images/feather.gif\" alt=\"Spectacular Image\">");
        sb.append("<br>[mail_html]");
        sb.append("</body>");
        sb.append("</html>");
        record.setStringField(SendMail.FIELD_MAIL_HTML, sb.toString());

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
    
    //@Test
    public void testHtmlMailFromRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SendMail());
        testRunner.setProperty(SendMail.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(SendMail.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(SendMail.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(SendMail.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(SendMail.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(SendMail.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);

        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(SendMail.FIELD_MAIL_TEXT, "testHtmlMailFromRecord:\nThis is the text message which"
                + " is an alternative to HTML one.");
        record.setStringField(SendMail.FIELD_MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        record.setStringField(SendMail.FIELD_MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        record.setStringField(SendMail.FIELD_MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        record.setStringField(SendMail.FIELD_MAIL_SUBJECT, TEST_MAIL_SUBJECT + " (from record)");
        record.setStringField(SendMail.FIELD_MAIL_TO, TEST_MAIL_TO);
        
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html>");
        sb.append("<html>");
        sb.append("<body>");
        sb.append("Here is a...");
        sb.append("<h2>Spectacular Image for you!</h2>");
        sb.append("<img src=\"http://www.apache.org/images/feather.gif\" alt=\"Spectacular Image\">");
        sb.append("<br>[mail_html]");
        sb.append("</body>");
        sb.append("</html>");
        record.setStringField(SendMail.FIELD_MAIL_HTML, sb.toString());
        
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }

    //@Test
    public void testUseTemplateMailFromConfig() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SendMail());
        testRunner.setProperty(SendMail.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(SendMail.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(SendMail.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(SendMail.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(SendMail.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(SendMail.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        testRunner.setProperty(SendMail.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_SUBJECT, TEST_MAIL_SUBJECT);
        testRunner.setProperty(SendMail.MAIL_TO, TEST_MAIL_TO);
        
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html>");
        sb.append("<html>");
        sb.append("<body>");
        sb.append("${" + PARAM_USER + "}, here is a...");
        sb.append("<h2>Spectacular Image for you (${" + PARAM_DATE + "})!</h2>");
        sb.append("<img src=\"http://www.apache.org/images/feather.gif\" alt=\"Spectacular Image\">");
        sb.append("<br>[template]");
        sb.append("</body>");
        sb.append("</html>");
        testRunner.setProperty(SendMail.HTML_TEMPLATE, sb.toString());

        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(SendMail.FIELD_MAIL_TEXT, "testUseTemplateMailFromConfig:\nThis is the text message which"
                + " is an alternative to HTML one.");
        
        // Tells to use the configured template
        record.setField(new Field(SendMail.FIELD_MAIL_USE_TEMPLATE, FieldType.BOOLEAN, true));
        
        // Set parameters for the template
        record.setStringField(PARAM_USER, "Bob");
        record.setStringField(PARAM_DATE, (new Date()).toString());

        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
    
    //@Test
    public void testUseTemplateMailFromRecord() {
        final TestRunner testRunner = TestRunners.newTestRunner(new SendMail());
        testRunner.setProperty(SendMail.SMTP_SERVER, TEST_SMTP_SERVER);
        testRunner.setProperty(SendMail.SMTP_PORT, TEST_SMTP_PORT);
        testRunner.setProperty(SendMail.SMTP_SECURITY_USERNAME, TEST_SMTP_SECURITY_USERNAME);
        testRunner.setProperty(SendMail.SMTP_SECURITY_PASSWORD, TEST_SMTP_SECURITY_PASSWORD);
        testRunner.setProperty(SendMail.SMTP_SECURITY_SSL, TEST_SMTP_SECURITY_SSL);
        testRunner.setProperty(SendMail.MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        testRunner.setProperty(SendMail.MAIL_BOUNCE_ADDRESS, TEST_MAIL_BOUNCE_ADDRESS);

        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html>");
        sb.append("<html>");
        sb.append("<body>");
        sb.append("${" + PARAM_USER + "}, here is a...");
        sb.append("<h2>Spectacular Image for you (${" + PARAM_DATE + "})!</h2>");
        sb.append("<img src=\"http://www.apache.org/images/feather.gif\" alt=\"Spectacular Image\">");
        sb.append("<br>[template]");
        sb.append("</body>");
        sb.append("</html>");
        testRunner.setProperty(SendMail.HTML_TEMPLATE, sb.toString());

        testRunner.assertValid();
        Record record = new StandardRecord("mail_record");
        record.setStringField(SendMail.FIELD_MAIL_TEXT, "testUseTemplateMailFromRecord:\nThis is the text message which"
                + " is an alternative to HTML one.");
        record.setStringField(SendMail.FIELD_MAIL_FROM_ADDRESS, TEST_MAIL_FROM_ADDRESS);
        record.setStringField(SendMail.FIELD_MAIL_FROM_NAME, TEST_MAIL_FROM_NAME);
        record.setStringField(SendMail.FIELD_MAIL_REPLYTO_ADDRESS, TEST_MAIL_REPLYTO_ADDRESS);
        record.setStringField(SendMail.FIELD_MAIL_SUBJECT, TEST_MAIL_SUBJECT + " (from record)");
        record.setStringField(SendMail.FIELD_MAIL_TO, TEST_MAIL_TO);
        
        // Tells to use the configured template
        record.setField(new Field(SendMail.FIELD_MAIL_USE_TEMPLATE, FieldType.BOOLEAN, true));

        // Set parameters for the template
        record.setStringField(PARAM_USER, "Bob");
        record.setStringField(PARAM_DATE, (new Date()).toString());
        
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(0);
    }
}
