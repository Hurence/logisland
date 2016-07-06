/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.hurence.logisland.logisland.parser.apache

import com.hurence.logisland.logisland.parser.base.{BaseLogParserTest, BaseUnitTest}
import com.hurence.logisland.event.{EventField, Event}
import com.hurence.logisland.parser.apache.ApacheLogParser

class ApacheLogParserTest extends BaseLogParserTest {


    "An apache log" should "be parsed" in {
        val logEntryLines = List(
            "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\""
        )

        val parser = new ApacheLogParser()
        val events = logEntryLines flatMap (log => parser.parse(log))

        events.length should be(1)

        testAnApacheCombinedLogEvent(events.head,
            host = "123.45.67.89",
            user = "-",
            date = "27/Oct/2000:09:27:09 -0400",
            stamp = 972653229000L,
            request = "GET /java/javaResources.html HTTP/1.0",
            status = "200",
            byteSent = 10450,
            refere = "-",
            userAgent = "Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)"
        )
        println(events.head)
    }



    it should "parse simple log as well" in {

        val logs = List(
            "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245",
            "unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985")

        val parser = new ApacheLogParser()
        val events = logs flatMap (log => parser.parse(log))

        events.length should be(2)

        events.head.getType should be("apache")
        testAnApacheSimpleLogEvent(events.head,
            host = "199.72.81.55",
            user = "-",
            date = "01/Jul/1995:00:00:01 -0400",
            stamp = 804571201000L,
            request = "GET /history/apollo/ HTTP/1.0",
            status = "200",
            byteSent = 6245
        )
        testAnApacheSimpleLogEvent(events(1),
            host = "unicomp6.unicomp.net",
            user = "-",
            date = "01/Jul/1995:00:00:06 -0400",
            stamp = 804571206000L,
            request = "GET /shuttle/countdown/ HTTP/1.0",
            status = "200",
            byteSent = 3985
        )

        println(events.head)
        println(events(1))

    }

    private def testAnApacheCombinedLogEvent(apacheEvent: Event,
                                             host: String,
                                             user: String,
                                             date: String,
                                             stamp: Long,
                                             request: String,
                                             status: String,
                                             byteSent: Int,
                                             refere: String,
                                             userAgent: String) = {
        apacheEvent.getType should be("apache")
        testAnEventField(apacheEvent.get("host"), "host", "string", host)
        testAnEventField(apacheEvent.get("user"), "user", "string", user)
        testAnEventField(apacheEvent.get("date"), "date", "string", date)
        testAnEventField(apacheEvent.get("@timestamp"), "@timestamp", "long", stamp.asInstanceOf[Object])
        testAnEventField(apacheEvent.get("request"), "request", "string", request)
        testAnEventField(apacheEvent.get("status"), "status", "string", status)
        testAnEventField(apacheEvent.get("bytesSent"), "bytesSent", "int", byteSent.asInstanceOf[Object])
        testAnEventField(apacheEvent.get("referer"), "referer", "string", refere)
        testAnEventField(apacheEvent.get("userAgent"), "userAgent", "string", userAgent)
    }

    private def testAnApacheSimpleLogEvent(apacheEvent: Event,
                                           host: String,
                                           user: String,
                                           date: String,
                                           stamp: Long,
                                           request: String,
                                           status: String,
                                           byteSent: Int) = {
        apacheEvent.getType should be("apache")
        testAnEventField(apacheEvent.get("host"), "host", "string", host)
        testAnEventField(apacheEvent.get("user"), "user", "string", user)
        testAnEventField(apacheEvent.get("date"), "date", "string", date)
        testAnEventField(apacheEvent.get("@timestamp"), "@timestamp", "long", stamp.asInstanceOf[Object])
        testAnEventField(apacheEvent.get("request"), "request", "string", request)
        testAnEventField(apacheEvent.get("status"), "status", "string", status)
        testAnEventField(apacheEvent.get("bytesSent"), "bytesSent", "int", byteSent.asInstanceOf[Object])
    }
}