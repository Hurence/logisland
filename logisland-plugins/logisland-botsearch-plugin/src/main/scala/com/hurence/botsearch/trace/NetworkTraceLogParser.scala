package com.hurence.botsearch.trace

import com.hurence.logisland.event.Event
import com.hurence.logisland.log.{LogParser, LogParserException}
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by tom on 12/01/16.
  */
class NetworkTraceLogParser extends LogParser with LazyLogging {

    val EVENT_TYPE = "log-island-trace"

    /**
      * take a line of csv and convert it to a NetworkFlow
      *
      * @param trace
      * @return
      */
    override def parse(trace: String): Array[Event] = {
        val event = new Event(EVENT_TYPE)
        try {

            // build the event
            /*  event.put("ipSource", "String", trace.ipSource)
              event.put("ipTarget", "String", trace.ipTarget)
              event.put("avgUploadedBytes", "Float", trace.avgUploadedBytes)
              event.put("avgDownloadedBytes", "Float", trace.avgDownloadedBytes)
              event.put("avgTimeBetweenTwoFLows", "Float", trace.avgTimeBetweenTwoFLows)
              event.put("mostSignificantFrequency", "Float", trace.mostSignificantFrequency)
              event.put("flowsCount", "Integer", trace.flowsCount)
              event.put("tags", "String", trace.tags)
              event.put("centroid", "Integer", trace.centroid)
  */

            Array(event)
        } catch {
            case t: Exception => {
                val errorMessage = s"exception parsing row : ${t.getMessage}"
                logger.error(errorMessage)
                throw new LogParserException(s"error parsing trace : $trace", t)
            }
        }
    }

}
