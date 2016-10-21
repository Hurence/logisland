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

import java.util

import com.hurence.logisland.component.ProcessContext
import com.hurence.logisland.record.StandardRecord
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by tom on 12/01/16.
  */
class NetworkTraceLogParser extends LazyLogging {

    val EVENT_TYPE = "logisland-trace"

    /**
      * take a line of csv and convert it to a NetworkFlow
      *
      * @param records
      * @return
      */
    def process(context: ProcessContext, records: util.Collection[StandardRecord]): util.Collection[StandardRecord] = {
        val event = new StandardRecord(EVENT_TYPE)


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
