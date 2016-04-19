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
package com.hurence.logisland.plugin.syslog

import com.hurence.logisland.event.Event
import com.hurence.logisland.log.LogParser

/**
  * Created by gregoire on 13/04/16.
  */
/**
  * Parse an Syslog log file with Regular Expression.
  *
  * The Syslog regular expressions below were copied from the Apache Nifi project.
  */
class SyslogParser extends LogParser {
    val EVENT_TYPE = "syslog"
    final val SYSLOG_MSG_RFC5424_0 =
        ("(?:\\<(\\d{1,3})\\>)" + // priority
            "(?:(\\d)?\\s?)" + // version
            /* yyyy-MM-dd'T'HH:mm:ss.SZ or yyyy-MM-dd'T'HH:mm:ss.S+hh:mm or - (null stamp) */
            "(?:" +
            "(\\d{4}[-]\\d{2}[-]\\d{2}[T]\\d{2}[:]\\d{2}[:]\\d{2}" +
            "(?:\\.\\d{1,6})?(?:[+-]\\d{2}[:]\\d{2}|Z)?)|-)" + // stamp
            "\\s" + // separator
            "(?:([\\w][\\w\\d\\.@\\-]*)|-)" + // host name or - (null)
            "\\s" + // separator
            "(.*)$").r // body

    final val SYSLOG_MSG_RFC3164_0 =
        ("(?:\\<(\\d{1,3})\\>)" +
            "(?:(\\d)?\\s?)" + // version
            // stamp MMM d HH:mm:ss, single digit date has two spaces
            "([A-Z][a-z][a-z]\\s{1,2}\\d{1,2}\\s\\d{2}[:]\\d{2}[:]\\d{2})" +
            "\\s" + // separator
            "([\\w][\\w\\d\\.@-]*)" + // host
            "\\s(.*)$").r // body

    override def parse(lines: String): Array[Event] = {
        val event = new Event(EVENT_TYPE)
        event.put("source", "string", lines)

        lines match {
            case SYSLOG_MSG_RFC5424_0(priority, version, stamp, host, body) => fillSyslogEvent(event, priority, version, stamp, host, body)
            case SYSLOG_MSG_RFC3164_0(priority, version, stamp, host, body) => fillSyslogEvent(event, priority, version, stamp, host, body)
            case x => event.put("error", "string", "bad log entry (or problem with RE?)")
        }
        Array(event)
    }

    def fillSyslogEvent(event: Event, priority: String, version: String, stamp: String, host: String, body: String) = {
        event.put("priority", "string", priority)
        try {
            if (version != null) event.put("version", "int", version.toInt)
        } catch {
            case e: NumberFormatException =>
                event.put("version", "string", version)
            case e: Throwable => throw new Error("an unexpected error occured during parsing of version in syslog", e)
        }
        event.put("date", "string", stamp)
        event.put("host", "string", host)
        event.put("body", "string", body)
    }
}
