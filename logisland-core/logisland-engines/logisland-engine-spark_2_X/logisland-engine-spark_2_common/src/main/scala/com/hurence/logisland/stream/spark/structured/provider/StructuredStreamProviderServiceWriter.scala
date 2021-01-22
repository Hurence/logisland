/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.hurence.logisland.stream.spark.structured.provider

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util
import java.util.Date

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.logging.ComponentLog
import com.hurence.logisland.record._
import com.hurence.logisland.runner.GlobalOptions
import com.hurence.logisland.serializer.{NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, ProcessorMetrics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


trait StructuredStreamProviderServiceWriter extends ControllerService {

  protected def getLogger: ComponentLog

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param streamContext
    * @return DataFrame currently loaded
    */
  protected def write(df: Dataset[Record],
                      controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                      streamContext: StreamContext): DataStreamWriter[_]

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param streamContext
    * @return DataFrame currently loaded
    */
  def save(df: Dataset[Record],
           controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
           streamContext: StreamContext): StreamingQuery = {


    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    // make sure controller service lookup won't be serialized !!
    streamContext.setControllerServiceLookup(null)

    // create serializer
    val serializer = SerializerProvider.getSerializer(
      streamContext.getPropertyValue(WRITE_TOPICS_SERIALIZER).asString,
      streamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)

    // create serializer
    val keySerializer = SerializerProvider.getSerializer(
      streamContext.getPropertyValue(WRITE_TOPICS_KEY_SERIALIZER).asString, null)

    // do the parallel processing
    val df2 = df.mapPartitions(record => record.map(record => serializeRecords(serializer, keySerializer, record)))

    var checkpointLocation : String = "checkpoints/" + streamContext.getIdentifier
    if (GlobalOptions.checkpointLocation != null) {
      checkpointLocation = GlobalOptions.checkpointLocation
      getLogger.info(s"Saving structured stream using checkpointLocation: $checkpointLocation")
    }

    write(df2, controllerServiceLookupSink, streamContext)
      .queryName(streamContext.getIdentifier)
      // .outputMode("update")
      .option("checkpointLocation", checkpointLocation)
      .start()
    // .processAllAvailable()

  }


  protected def serializeRecords(valueSerializer: RecordSerializer,
                                 keySerializer: RecordSerializer,
                                 record: Record) = {
    try {
      val ret = /*valueSerializer match {
        case s: JsonSerializer =>
          new StandardRecord()
            .setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, doSerializeAsString(valueSerializer, record))
        case _ =>*/
        new StandardRecord()
          .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, doSerialize(valueSerializer, record))
      // }
      val fieldKey = record.getField(FieldDictionary.RECORD_KEY)
      if (fieldKey != null) {
        ret.setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, doSerialize(keySerializer, new StandardRecord().setField(fieldKey)))
      } else {
        ret.setField(FieldDictionary.RECORD_KEY, FieldType.NULL, null)

      }
      ret
    } catch {
      case t: Throwable =>
        getLogger.error(s"exception while serializing events ${
          t.getMessage
        }")
        null
    }
  }

  private def doSerialize(serializer: RecordSerializer,
                          record: Record): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    serializer.serialize(baos, record)
    val bytes = baos.toByteArray
    baos.close()
    bytes


  }
}
