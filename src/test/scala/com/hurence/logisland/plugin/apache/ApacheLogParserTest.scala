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
package com.hurence.logisland.plugin.apache


import org.scalatest._

class ApacheLogParserTest extends FlatSpec with Matchers {


    "An apache log" should "be parsed" in {
        val logEntryLine = "123.45.67.89 - - [27/Oct/2000:09:27:09 -0400] \"GET /java/javaResources.html HTTP/1.0\" 200 10450 \"-\" \"Mozilla/4.6 [en] (X11; U; OpenBSD 2.8 i386; Nav)\""

        val parser = new ApacheLogParser()
        val events = parser.parse(logEntryLine)

        events.length should be(1)
        events(0).getType should be("apache")
        println(events(0))
    }



    it should "parse simple log as well" in {

        val logs = List(
            "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245",
            "unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 200 3985")

        val parser = new ApacheLogParser()
        logs.foreach(log => {
            val events = parser.parse(log)

            events.length should be(1)
            events(0).getType should be("apache")
            println(events(0))
        })

    }
}