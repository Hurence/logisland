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

package com.hurence.logisland.utils.elasticsearch

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.hurence.logisland.record.FieldDictionary.RECORD_TIME
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.elasticsearch.common.xcontent.XContentFactory._
import org.joda.time.format.ISODateTimeFormat

import scala.collection.JavaConversions._

/**
  * Take an Event and convert it to a String representing an Elasticsearch document
  */
object ElasticsearchRecordConverter extends LazyLogging {
    /**
      * Converts an Event into an Elasticsearch document
      * to be indexed later
      *
      * @param event
      * @return
      */
    def convert(event: Record): String = {

        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
        val document = jsonBuilder().startObject()

        event.getAllFields.foreach(field => {
            var fieldName = ""
            try {
                fieldName = field.getName.toLowerCase().replaceAll("\\.", "_")

                val fieldValue = if (field.getType == FieldType.STRING) {
                    field.asString()
                } else if (field.getType == FieldType.INT ) {
                    field.asInteger()
                }
                else if (field.getType == FieldType.LONG ) {
                    // convert event_time as ISO for ES
                    if (fieldName.equals(FieldDictionary.RECORD_TIME)) {
                        try {
                            val dateParser = ISODateTimeFormat.dateTimeNoMillis()
                            dateParser.print(field.asLong())
                        } catch {
                            case ex: Throwable => field.asLong()
                        }
                    } else {
                        field.asLong()
                    }
                }else if (field.getType == FieldType.FLOAT ) {
                    field.asFloat()
                }
                else if (field.getType == FieldType.DOUBLE ) {
                    field.asDouble()
                }
                else if (field.getType == FieldType.BOOLEAN ) {
                    field.asBoolean()
                }
                else {
                    field.getRawValue
                }




            document.field(fieldName, fieldValue)

        } catch
        {
            case ex: Throwable => logger.error(s"unable to process a $fieldName in event : $event, ${ex.getMessage}")
        }

    }

    )

    val result = document.endObject().string()
    document.flush()
    result
}


}
