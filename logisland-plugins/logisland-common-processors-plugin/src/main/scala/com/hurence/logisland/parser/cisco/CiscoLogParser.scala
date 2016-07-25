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

package com.hurence.logisland.parser.cisco

import java.text.SimpleDateFormat
import java.util
import java.util.Collections

import com.hurence.logisland.event.Event
import com.hurence.logisland.log.{LogParserException, LogParser}
import com.hurence.logisland.processor.ProcessContext
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by tom on 12/01/16.
  */
class CiscoLogParser extends LogParser with LazyLogging {

    val EVENT_TYPE = "cisco"

    /**
      * take a line of csv and convert it to a NetworkFlow
      *
      * @param line
      * @return
      */
    override def parse(context:ProcessContext, key:String, line: String): util.Collection[Event] = {
        val event = new Event(EVENT_TYPE)
        try {

            // parse the line
            val records = line.split("\t")
            val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
            val timestamp = try {
                sdf.parse(records(0)).getTime
            } catch {
                case t: Throwable => 0
            }

            val tags = if (records.length == 13)
                records(12).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "")
            else ""


            // build the event
            event.put("timestamp", "Long", timestamp)
            event.put("method", "String", records(1))
            event.put("ipSource", "String", records(2))
            event.put("ipTarget", "String", records(3))
            event.put("urlScheme", "String", records(4))
            event.put("urlHost", "String", records(5))
            event.put("urlPort", "String", records(6))
            event.put("urlPath", "String", records(7))
            event.put("requestSize", "Int", records(8).toInt)
            event.put("responseSize", "Int", records(9).toInt)
            event.put("isOutsideOfficeHours", "Boolean", records(10).toBoolean)
            event.put("isHostBlacklisted", "Boolean", records(11).toBoolean)
            event.put("tags", "String", tags)

            Collections.singletonList(event)
        } catch {
            case t: Exception => {
                val errorMessage = s"exception parsing row : ${t.getMessage}"
                logger.error(errorMessage)
                throw new LogParserException(s"error parsing log line : $line", t)
            }
        }
    }

}
