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

import com.hurence.logisland.event.EventMapper
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._

/**
  * Created by gregoire on 13/04/16.
  */
/**
  *     event.put("priority", "string", priority)
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
  */
class SyslogEventMapper extends EventMapper{
    val EVENT_TYPE = "syslog"

    override def getMapping: XContentBuilder = {
        jsonBuilder().startObject().startObject(EVENT_TYPE)
            .startObject("_ttl").field("enabled", "true").field("default", "30d").endObject()
            .startObject("properties")

            .startObject("priority").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("version").field("type", "integer").field("store", "yes").endObject()
            .startObject("versionNotAnInt").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("date").field("type", "date").field("format", "dateOptionalTime").endObject()
            .startObject("host").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("body").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()

            .endObject()
            .endObject().endObject()
    }

    override def getDocumentType: String = EVENT_TYPE
}
