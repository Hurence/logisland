package com.hurence.botsearch.trace

import java.util

import com.hurence.logisland.component.ComponentContext
import com.hurence.logisland.record.Record
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by tom on 12/01/16.
  */
class NetworkTraceLogParser extends LazyLogging {

    val EVENT_TYPE = "log-island-trace"

    /**
      * take a line of csv and convert it to a NetworkFlow
      *
      * @param records
      * @return
      */
    def process(context: ComponentContext, records: util.Collection[Record]): util.Collection[Record] = {
        val event = new Record(EVENT_TYPE)


            // build the event
            /*  event.setField("ipSource", "String", trace.ipSource)
              event.setField("ipTarget", "String", trace.ipTarget)
              event.setField("avgUploadedBytes", "Float", trace.avgUploadedBytes)
              event.setField("avgDownloadedBytes", "Float", trace.avgDownloadedBytes)
              event.setField("avgTimeBetweenTwoFLows", "Float", trace.avgTimeBetweenTwoFLows)
              event.setField("mostSignificantFrequency", "Float", trace.mostSignificantFrequency)
              event.setField("flowsCount", "Integer", trace.flowsCount)
              event.setField("tags", "String", trace.tags)
              event.setField("centroid", "Integer", trace.centroid)
  */

            util.Collections.singletonList(event)

    }

}
