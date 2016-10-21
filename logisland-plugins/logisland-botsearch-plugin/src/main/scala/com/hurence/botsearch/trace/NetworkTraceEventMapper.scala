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
