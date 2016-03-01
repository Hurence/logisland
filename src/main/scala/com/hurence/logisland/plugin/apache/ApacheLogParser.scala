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

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import com.hurence.logisland.event.Event
import com.hurence.logisland.log.LogParser


/**
  * Parse an Apache log file with Regular Expressions
  */
class ApacheLogParser extends LogParser {

    val EVENT_TYPE = "apache"

    val simplePattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)")
    val combinedPattern = Pattern.compile("^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"")

    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")

    override def parse(lines: String): Array[Event] = {
        val event = new Event(EVENT_TYPE)
        event.put("source", lines)



        val matcher = combinedPattern.matcher(lines)
        if (matcher.matches()) {
            event.put("host", "string", matcher.group(1))
            event.put("user", "string", matcher.group(3))
            event.put("date", "string", matcher.group(4))
            event.put("@timestamp", "long", sdf.parse(matcher.group(4)).getTime)
            event.put("request", "string", matcher.group(5))
            event.put("status", "string", matcher.group(6))
            event.put("bytesSent", "int", matcher.group(7).toInt)
            event.put("referer", "string", matcher.group(8))
            event.put("userAgent", "string", matcher.group(9))
        }else {

            val simpleMatcher = simplePattern.matcher(lines)
            if (simpleMatcher.matches()) {
                event.put("host", "string", simpleMatcher.group(1))
                event.put("user", "string", simpleMatcher.group(3))
                event.put("date", "string", simpleMatcher.group(4))
                event.put("@timestamp", "long", sdf.parse(simpleMatcher.group(4)).getTime)
                event.put("request", "string", simpleMatcher.group(5))
                event.put("status", "string", simpleMatcher.group(6))
                event.put("bytesSent", "int", simpleMatcher.group(7).toInt)
            }else {

                event.put("error", "string", "bad log entry (or problem with RE?)")
            }
        }
        Array(event)
    }

}




