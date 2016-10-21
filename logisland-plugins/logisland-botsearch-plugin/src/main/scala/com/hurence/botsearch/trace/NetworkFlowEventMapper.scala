package com.hurence.botsearch.trace

import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._

/**
  * Created by tom on 12/01/16.
  */
class NetworkFlowEventMapper  {
    val EVENT_TYPE = "logisland-flow"

    def getMapping: XContentBuilder = {
        jsonBuilder().startObject().startObject(EVENT_TYPE)
            .startObject("_ttl").field("enabled", "true").field("default", "30d").endObject()
            .startObject("properties")
            .startObject("date").field("type", "string").field("store", "yes").endObject()
            .startObject("@timestamp").field("type", "date").field("format", "dateOptionalTime").endObject()
            .startObject("method").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("ipSource").field("type", "string").field("index", "not_analyzed").endObject()
            .startObject("ipTarget").field("type", "string").field("index", "not_analyzed").endObject()
            .startObject("urlScheme").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("urlHost").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .startObject("urlPort").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("urlPath").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("requestSize").field("type", "long").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("responseSize").field("type", "long").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("isOutsideOfficeHours").field("type", "boolean").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("isHostBlacklisted").field("type", "boolean").field("store", "yes").field("index", "not_analyzed").endObject()
            .startObject("tags").field("type", "string").field("store", "yes").field("index", "analyzed").endObject()
            .endObject()
            .endObject().endObject()

    }

    def getDocumentType: String = EVENT_TYPE

}
