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
import java.util.Collections

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.record._
import com.hurence.logisland.serializer.{JsonSerializer, NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, ProcessorMetrics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming.{DataStreamWriter, GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

trait StructuredStreamProviderService extends ControllerService {

  val logger = LoggerFactory.getLogger(this.getClass)


  /**
    * create a streaming DataFrame that represents data received
    *
    * @param spark
    * @param streamContext
    * @return DataFrame currently loaded
    */
  protected def read(spark: SparkSession, streamContext: StreamContext): Dataset[Record]

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param streamContext
    * @return DataFrame currently loaded
    */
  protected def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): DataStreamWriter[_]

  /**
    *
    *
    * @param spark
    * @param streamContext
    * @return
    */
  def load(spark: SparkSession, controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): Dataset[Record] = {
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    import spark.implicits._

    val df = read(spark, streamContext)

    /**
      * create serializers
      */
    val serializer = SerializerProvider.getSerializer(
      streamContext.getPropertyValue(READ_TOPICS_SERIALIZER).asString,
      streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)

    val keySerializer = SerializerProvider.getSerializer(
      streamContext.getPropertyValue(READ_TOPICS_KEY_SERIALIZER).asString,
      null)


    // convert to logisland records

    val processingRecords: Dataset[Record] = df.flatMap(r => {
      serializer match {
        case sr: NoopSerializer => Some(r)
        case _ => deserializeRecords(serializer, keySerializer, r)
      }
    })


    if (streamContext.getPropertyValue(GROUPBY_KEYS).isSet) {

      val keys = streamContext.getPropertyValue(GROUPBY_KEYS).asString()


      processingRecords
        .filter(_.hasField(keys))
        .groupByKey(_.getField(keys).asString())
        .flatMapGroupsWithState(
          outputMode = OutputMode.Append,
          timeoutConf = GroupStateTimeout.NoTimeout)( (sessionId: String, eventsIter: Iterator[Record], state: GroupState[Record]) =>{


            val events = executePipeline(controllerServiceLookupSink, streamContext, eventsIter)
            /*  val updatedSession = if (state.exists) {
                val existingState = state.get
                val sum = events.map(event => event.getField(FieldDictionary.RECORD_VALUE).asDouble()).reduce(_ + _)
                existingState.setField("agg_sum", FieldType.DOUBLE, sum)
                existingState
              }
              else {
                new StandardRecord()
                  .setField("tagname", FieldType.STRING, events.head.getField("tagname").asString())
                  .setField("agg_sum", FieldType.DOUBLE,
                    events.map(event => event.getField(FieldDictionary.RECORD_VALUE).asDouble()).reduce(_ + _)
                  )
                  .setField("agg_count", FieldType.INT, events.size)

              }
              state.update(updatedSession)*/
            //check did we get end signal or not

            events
        })

    } else {
      processingRecords.mapPartitions(iterator => {
        executePipeline(controllerServiceLookupSink, streamContext, iterator)
      })
    }


  }


  def executeGroupedPipeleine(
                               id: String,
                               userEvents: Iterator[Record],
                               state: GroupState[Record]): Record = {
    if (state.hasTimedOut) {
      // We've timed out, lets extract the state and send it down the stream
      state.remove()
      state.get
    } else {
      /*
        New data has come in for the given user id. We'll look up the current state
        to see if we already have something stored. If not, we'll just take the current user events
        and update the state, otherwise will concatenate the user events we already have with the
        new incoming events.
      */
      val currentState = state.getOption
      //  val updatedUserSession = currentState.fold(RecordSession(userEvents.toSeq))(currentUserSession => RecordSession(currentUserSession.userEvents ++ userEvents.toSeq))

      /* if (updatedUserSession.userEvents.exists(_.isLast)) {

       //  If we've received a flag indicating this should be the last event batch, let's close
       //  the state and send the user session downstream.

         state.remove()
         updatedUserSession
       } else {
         state.update(updatedUserSession)
         state.setTimeoutDuration("1 minute")
         None
       }*/
      new StandardRecord()
    }
  }

  private def executePipeline(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext, iterator: Iterator[Record]) = {
    val controllerServiceLookup = controllerServiceLookupSink.value.getControllerServiceLookup()


    // convert to logisland records
    var processingRecords: util.Collection[Record] = iterator.toList

    val pipelineMetricPrefix = streamContext.getIdentifier + "."
    // loop over processor chain
    streamContext.getProcessContexts.foreach(processorContext => {
      val startTime = System.currentTimeMillis()
      val processor = processorContext.getProcessor

      val processorTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix +
        processorContext.getIdentifier + ".processing_time_ms").time()

      // injects controller service lookup into processor context
      if (processor.hasControllerService) {
        processorContext.setControllerServiceLookup(controllerServiceLookup)
      }

      // processor setup (don't forget that)
      processor.init(processorContext)

      // do the actual processing
      processingRecords = processor.process(processorContext, processingRecords)

      // compute metrics
      ProcessorMetrics.computeMetrics(
        pipelineMetricPrefix + processorContext.getIdentifier + ".",
        processingRecords,
        processingRecords,
        0,
        processingRecords.size,
        System.currentTimeMillis() - startTime)

      processorTimerContext.stop()
    })


    processingRecords.asScala.iterator
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param streamContext
    * @return DataFrame currently loaded
    */
  def save(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext) = {

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
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]
    val df2 = df.mapPartitions(record => record.map(record => serializeRecords(serializer, keySerializer, record)))

    write(df2, controllerServiceLookupSink, streamContext).queryName(streamContext.getIdentifier)
     // .outputMode("update")
      .start()


  }


  protected def serializeRecords(valueSerializer: RecordSerializer, keySerializer: RecordSerializer, record: Record) = {

    try {
      val ret = valueSerializer match {
        case s: JsonSerializer =>
          new StandardRecord()
            .setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, doSerializeAsString(valueSerializer, record))
        case _ =>
          new StandardRecord()
            .setField(FieldDictionary.RECORD_VALUE, FieldType.BYTES, doSerialize(valueSerializer, record))
      }
      val fieldKey = record.getField(FieldDictionary.RECORD_KEY);
      if (fieldKey != null) {
        ret.setField(FieldDictionary.RECORD_KEY, FieldType.BYTES, doSerialize(keySerializer, new StandardRecord().setField(fieldKey)))
      } else {
        ret.setField(FieldDictionary.RECORD_KEY, FieldType.NULL, null)

      }
      ret

    } catch {
      case t: Throwable =>
        logger.error(s"exception while serializing events ${
          t.getMessage
        }")
        null
    }


  }

  private def doSerializeAsString(serializer: RecordSerializer, record: Record): String = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    serializer.serialize(baos, record)
    val bytes = baos.toByteArray
    baos.close()
    new String(bytes)


  }

  private def doSerialize(serializer: RecordSerializer, record: Record): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    serializer.serialize(baos, record)
    val bytes = baos.toByteArray
    baos.close()
    bytes


  }

  private def doDeserialize(serializer: RecordSerializer, field: Field): Record = {
    val f = field.getRawValue
    val s = if (f.isInstanceOf[String]) f.asInstanceOf[String].getBytes else f;
    val bais = new ByteArrayInputStream(s.asInstanceOf[Array[Byte]])
    try {
      serializer.deserialize(bais)
    } finally {
      bais.close()
    }
  }

  protected def deserializeRecords(serializer: RecordSerializer, keySerializer: RecordSerializer, r: Record) = {
    try {
      val deserialized = doDeserialize(serializer, r.getField(FieldDictionary.RECORD_VALUE))
      // copy root record field
      if (r.hasField(FieldDictionary.RECORD_NAME))
        deserialized.setField(r.getField(FieldDictionary.RECORD_NAME))

      if (r.hasField(FieldDictionary.RECORD_KEY) && r.getField(FieldDictionary.RECORD_KEY).getRawValue != null) {
        val deserializedKey = doDeserialize(keySerializer, r.getField(FieldDictionary.RECORD_KEY))
        if (deserializedKey.hasField(FieldDictionary.RECORD_VALUE) && deserializedKey.getField(FieldDictionary.RECORD_VALUE).getRawValue != null) {
          val f = deserializedKey.getField(FieldDictionary.RECORD_VALUE)
          deserialized.setField(FieldDictionary.RECORD_KEY, f.getType, f.getRawValue)
        } else {
          logger.warn("Unable to serialize key for record $r with serializer $keySerializer")
        }
      }

      Some(deserialized)

    } catch {
      case t: Throwable =>
        logger.error(s"exception while deserializing events ${
          t.getMessage
        }")
        None
    }
  }


}
