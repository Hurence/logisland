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
package com.hurence.logisland.parser.apache

import com.hurence.logisland.components.ComponentsFactory
import com.hurence.logisland.config.ComponentConfiguration
import com.hurence.logisland.event.Event
import com.hurence.logisland.log.{StandardParserContext, StandardParserInstance}
import com.hurence.logisland.parser.base.BaseLogParserTest
import com.hurence.logisland.processor.{StandardProcessContext, StandardProcessorInstance, ProcessContext}
import org.junit.Assert

import scala.collection.JavaConversions._

class ApacheLogParserTest extends BaseLogParserTest {

    val APACHE_LOG_SAMPLE = "/data/localhost_access.log"


    "An apache log" should "be parsed" in {
        val conf = new java.util.HashMap[String, String]

        conf.put("key.regex", "(\\S*):(\\S*)")
        conf.put("key.fields", "es_index,host_name")

        val componentConfiguration: ComponentConfiguration = new ComponentConfiguration

        componentConfiguration.setComponent("com.hurence.logisland.parser.apache.ApacheLogParser")
        componentConfiguration.setType("parser")
        componentConfiguration.setConfiguration(conf)

        val instance = ComponentsFactory.getParserInstance(componentConfiguration)
        val context = new StandardParserContext(instance)
        Assert.assertTrue(instance != null)



        val logEntryLines = List(
            "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\""
        )

        val parser = instance.getParser
        val events = logEntryLines flatMap (log => parser.parse(context, "", log))

        events.length should be(1)

        testAnApacheCombinedLogEvent(events.head,
            dest_ip = "123.45.67.89",
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



    it should "resist to a bad config" in {


        val conf = new java.util.HashMap[String, String]

      /*  conf.put("key.regex", "(\\S*):(\\S*)")
        conf.put("key.fields", "es_index,host_name")*/

        val componentConfiguration: ComponentConfiguration = new ComponentConfiguration

        componentConfiguration.setComponent("com.hurence.logisland.parser.apache.ApacheLogParser")
        componentConfiguration.setType("parser")
        componentConfiguration.setConfiguration(conf)

        val instance = ComponentsFactory.getParserInstance(componentConfiguration)
        val context = new StandardParserContext(instance)
        Assert.assertTrue(instance != null)


        val logs = List(
        "10.3.10.134 - - [24/Jul/2016:08:49:40 +0200] \"POST /usr/rest/session HTTP/1.1\" 200 1082",
        "10.3.10.134 - - [24/Jul/2016:08:49:40 +0200] \"DELETE /usr/rest/session HTTP/1.1\" 204 -",
        "10.3.10.133 - - [24/Jul/2016:08:49:40 +0200] \"GET /usr/rest/bank/purses?activeOnly=true HTTP/1.1\" 200 240",
        "10.3.10.133 - - [24/Jul/2016:08:49:40 +0200] \"GET /usr/rest/limits/moderato?siteCode=FDJ_WEB HTTP/1.1\" 200 53"
        )

        val parser = instance.getParser
        val events = logs flatMap (log => parser.parse(context, "", log))

        events.length should be(4)

    }


    it should "parse real log file" in {

        val logs = scala.io.Source.fromFile(classOf[ApacheLogParserTest].getResource(APACHE_LOG_SAMPLE).getFile).getLines().toList

        val conf = new java.util.HashMap[String, String]

        conf.put("key.regex", "(\\S*):(\\S*)")
        conf.put("key.fields", "es_index,host_name")

        val componentConfiguration: ComponentConfiguration = new ComponentConfiguration

        componentConfiguration.setComponent("com.hurence.logisland.parser.apache.ApacheLogParser")
        componentConfiguration.setType("parser")
        componentConfiguration.setConfiguration(conf)

        val instance = ComponentsFactory.getParserInstance(componentConfiguration)
        val context = new StandardParserContext(instance)
        Assert.assertTrue(instance != null)


        val parser = instance.getParser
        val events = logs flatMap (log => parser.parse(context, "", log + "\n"))


        events.length should be(4993)
        val errors = events.filter(event => event.get("error") != null)
        errors.length should be(66)

    }


    it should "parse simple log as well" in {


        val conf = new java.util.HashMap[String, String]

        conf.put("key.regex", "(\\S*):(\\S*)")
        conf.put("key.fields", "es_index,host_name")

        val componentConfiguration: ComponentConfiguration = new ComponentConfiguration

        componentConfiguration.setComponent("com.hurence.logisland.parser.apache.ApacheLogParser")
        componentConfiguration.setType("parser")
        componentConfiguration.setConfiguration(conf)

        val instance = ComponentsFactory.getParserInstance(componentConfiguration)
        val context = new StandardParserContext(instance)
        Assert.assertTrue(instance != null)


        val logs = List(
            "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245",
            "unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985")

        val parser = instance.getParser
        val events = logs flatMap (log => parser.parse(context, "", log))

        events.length should be(2)

        events.head.getType should be("apache_log")
        testAnApacheSimpleLogEvent(events.head,
            dest_ip = "199.72.81.55",
            user = "-",
            date = "01/Jul/1995:00:00:01 -0400",
            stamp = 804571201000L,
            request = "GET /history/apollo/ HTTP/1.0",
            status = "200",
            byteSent = 6245
        )
        testAnApacheSimpleLogEvent(events(1),
            dest_ip = "unicomp6.unicomp.net",
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
                                             dest_ip: String,
                                             user: String,
                                             date: String,
                                             stamp: Long,
                                             request: String,
                                             status: String,
                                             byteSent: Int,
                                             refere: String,
                                             userAgent: String) = {
        apacheEvent.getType should be("apache_log")
        testAnEventField(apacheEvent.get("dest_ip"), "dest_ip", "string", dest_ip)
        testAnEventField(apacheEvent.get("user"), "user", "string", user)
        testAnEventField(apacheEvent.get("event_time"), "event_time", "long", stamp.asInstanceOf[Object])
        testAnEventField(apacheEvent.get("http_request"), "http_request", "string", request)
        testAnEventField(apacheEvent.get("status"), "status", "string", status)
        testAnEventField(apacheEvent.get("bytes_out"), "bytes_out", "int", byteSent.asInstanceOf[Object])
        testAnEventField(apacheEvent.get("referer"), "referer", "string", refere)
        testAnEventField(apacheEvent.get("user_agent"), "user_agent", "string", userAgent)
    }

    private def testAnApacheSimpleLogEvent(apacheEvent: Event,
                                           dest_ip: String,
                                           user: String,
                                           date: String,
                                           stamp: Long,
                                           request: String,
                                           status: String,
                                           byteSent: Int) = {
        apacheEvent.getType should be("apache_log")
        testAnEventField(apacheEvent.get("dest_ip"), "dest_ip", "string", dest_ip)
        testAnEventField(apacheEvent.get("user"), "user", "string", user)
        testAnEventField(apacheEvent.get("event_time"), "event_time", "long", stamp.asInstanceOf[Object])
        testAnEventField(apacheEvent.get("http_request"), "http_request", "string", request)
        testAnEventField(apacheEvent.get("status"), "status", "string", status)
        testAnEventField(apacheEvent.get("bytes_out"), "bytes_out", "int", byteSent.asInstanceOf[Object])
    }
}