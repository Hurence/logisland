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
package com.hurence.logisland.parser.syslog

import java.util
import java.util.{Calendar, Collections, Date}

import com.hurence.logisland.component.ComponentContext
import com.hurence.logisland.record.{FieldType, Record}
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

/**
  * Created by gregoire on 13/04/16.
  */
/**
  * Parse an Syslog log file with Regular Expression.
  *
  * The Syslog regular expressions below were copied from the Apache Nifi project.
  */
class SyslogParser {
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

    final val DTF1_SYSLOG_MSG_RFC5424_0 = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SZ").withZone(DateTimeZone.UTC)
    final val DTF2_SYSLOG_MSG_RFC5424_0 = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.S+hh:mm").withZone(DateTimeZone.UTC)
    final val DTF3_SYSLOG_MSG_RFC3164_0 = DateTimeFormat.forPattern("MMM d HH:mm:ss").withZone(DateTimeZone.UTC)
        .withDefaultYear(Calendar.getInstance().get(Calendar.YEAR))


    def parse(context: ComponentContext, key: String, value: String): util.Collection[Record] = {
        val event = new Record(EVENT_TYPE)
        event.setStringField("source", value)

        value match {
            case SYSLOG_MSG_RFC5424_0(priority, version, stamp, host, body) => fillSyslogEvent(event, priority, version, stamp, host, body)
            case SYSLOG_MSG_RFC3164_0(priority, version, stamp, host, body) => fillSyslogEvent(event, priority, version, stamp, host, body)
            case x => event.setStringField("error", "bad log entry (or problem with RE?)")
        }
        Collections.singletonList(event)
    }

    def fillSyslogEvent(event: Record, priority: String, version: String, stamp: String, host: String, body: String) = {
        event.setStringField("priority", priority)
        try {
            if (version != null) event.setField("version", FieldType.INT, version.toInt)
        } catch {
            case e: NumberFormatException =>
                event.setStringField("versionNotAnInt", version)
            case e: Throwable => throw new Error("an unexpected error occured during parsing of version in syslog", e)
        }


        try {
            // stamp MMM d HH:mm:ss, single digit date has two spaces
            val timestamp = DTF1_SYSLOG_MSG_RFC5424_0.parseDateTime(stamp).getMillis
            event.setField(Record.RECORD_TIME, FieldType.LONG, timestamp)
        } catch {
            case e: Throwable =>
                try {
                    // stamp MMM d HH:mm:ss, single digit date has two spaces
                    val timestamp = DTF2_SYSLOG_MSG_RFC5424_0.parseDateTime(stamp).getMillis
                    event.setField(Record.RECORD_TIME, FieldType.LONG, timestamp)
                } catch {
                    case e: Throwable =>
                        try {
                            // stamp MMM d HH:mm:ss, single digit date has two spaces
                            val timestamp = DTF3_SYSLOG_MSG_RFC3164_0.parseDateTime(stamp).getMillis
                            event.setField(Record.RECORD_TIME, FieldType.LONG, timestamp)
                        } catch {
                            case e: Throwable =>
                                event.setField(Record.RECORD_TIME, FieldType.LONG, new Date().getTime)
                        }
                }
        }
        event.setStringField("date", stamp)
        event.setStringField("host", host)
        event.setStringField("body", body)
    }
}
