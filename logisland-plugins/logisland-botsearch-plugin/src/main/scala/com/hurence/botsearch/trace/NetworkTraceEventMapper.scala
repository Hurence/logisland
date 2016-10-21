/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.botsearch.trace

import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._

/**
  * Created by tom on 12/01/16.
  */
class NetworkTraceEventMapper  {
    val EVENT_TYPE = "logisland-trace"

    def getMapping: XContentBuilder = {
        jsonBuilder().startObject().startObject(EVENT_TYPE)
            .startObject("_ttl").field("enabled", "true").field("default", "30d").endObject()
            .startObject("properties")
            .startObject("ipSource").field("type", "string").field("index", "not_analyzed").endObject()
            .startObject("ipTarget").field("type", "string").field("index", "not_analyzed").endObject()
            .startObject("centroid").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("avgTimeBetweenTwoFLows").field("type", "float").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("avgUploadedBytes").field("type", "float").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("avgDownloadedBytes").field("type", "float").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("mostSignificantFrequency").field("type", "float").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("flowsCount").field("type", "long").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("tags").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .endObject()
            .endObject().endObject()

    }

    def getDocumentType: String = EVENT_TYPE

}
