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

import com.hurence.logisland.event.EventMapper
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._

/**
  * Created by tom on 12/01/16.
  */
class ApacheEventMapper extends EventMapper {


    /**
      * event.put("host", "string", matcher.group(1))
            event.put("user", "string", matcher.group(3))
            event.put("date", "long", sdf.parse(matcher.group(4)).getTime)
            event.put("request", "string", matcher.group(5))
            event.put("status", "string", matcher.group(6))
            event.put("bytesSent", "int", matcher.group(7).toInt)
            event.put("referer", "string", matcher.group(8))
            event.put("userAgent", "string", matcher.group(9))
      */
    val EVENT_TYPE = "apache"

    override def getMapping: XContentBuilder = {
        jsonBuilder().startObject().startObject(EVENT_TYPE)
            .startObject("_ttl").field("enabled", "true").field("default", "30d").endObject()
            .startObject("properties")
            .startObject("date").field("type", "string").field("store", "yes").endObject()
            .startObject("@timestamp").field("type", "date").field("format", "dateOptionalTime").endObject()
            .startObject("host").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("user").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("request").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("status").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("bytesSent").field("type", "integer").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("referer").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("userAgent").field("type", "string").field("index", "not_analyzed").endObject()

            .startObject("tags").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .endObject()
            .endObject().endObject()
    }

    override def getDocumentType: String = EVENT_TYPE
}
