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
import java.util.Date

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.record._
import com.hurence.logisland.runner.GlobalOptions
import com.hurence.logisland.serializer.{JsonSerializer, NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, ProcessorMetrics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


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

    import spark.implicits._
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

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


    if (streamContext.getPropertyValue(GROUPBY).isSet) {

      val keys = streamContext.getPropertyValue(GROUPBY).asString()
      val stateTimeoutDuration = streamContext.getPropertyValue(STATE_TIMEOUT_MS).asLong()
      val chunkSize = streamContext.getPropertyValue(CHUNK_SIZE).asInteger()

      processingRecords
        .filter(_.hasField(keys))
        .groupByKey(_.getField(keys).asString())
        .flatMapGroupsWithState(outputMode = OutputMode.Append, timeoutConf = GroupStateTimeout.ProcessingTimeTimeout())(
          mappingFunction(controllerServiceLookupSink, streamContext, chunkSize, stateTimeoutDuration)
        )

    } else {
      processingRecords.mapPartitions(iterator => {
        executePipeline(controllerServiceLookupSink, streamContext, iterator)
      })
    }


  }

  val ALL_RECORDS = "all_records"
  val CHUNK_CREATION_TS = "chunk_creation_ts"

  def mappingFunction(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                      streamContext: StreamContext,
                      chunkSize: Int,
                      timeOutDuration: Long)
                     (key: String,
                      value: Iterator[Record],
                      state: GroupState[Record]): Iterator[Record] = {


    val currentTimestamp = new Date().getTime
    val inputRecords = value.toList
    val allRecords = if (state.exists) state.get.getField(ALL_RECORDS).getRawValue.asInstanceOf[List[Record]] ++ inputRecords else inputRecords
    val recordChunks = allRecords.grouped(chunkSize).toList


    if (state.hasTimedOut || (state.exists && (currentTimestamp - state.get.getField(CHUNK_CREATION_TS).asLong()) >= timeOutDuration)) {
      state.remove()
      //  logger.debug("TIMEOUT key " + key + ", flushing " + allRecords.size + " records in " + recordChunks.size + "chunks")
      recordChunks
        .flatMap(subset => executePipeline(controllerServiceLookupSink, streamContext, subset.iterator))
        .iterator
    }
    else if (recordChunks.last.size == chunkSize) {
      state.remove()
      //logger.debug("REMOVE key " + key + ", flushing " + allRecords.size + " records in " + recordChunks.size + "chunks")
      recordChunks
        .flatMap(subset => executePipeline(controllerServiceLookupSink, streamContext, subset.iterator))
        .iterator
    }
    else if (!state.exists) {

      val newChunk = new StandardRecord("chunk_record") //Chunk(key, recordChunks.last)
      newChunk.setObjectField(ALL_RECORDS, recordChunks.last)
      newChunk.setStringField(FieldDictionary.RECORD_KEY, key)
      newChunk.setLongField(CHUNK_CREATION_TS, new Date().getTime)
      // logger.debug("CREATE key " + key + " new chunk with " + allRecords.size + " records")

      state.update(newChunk)
      state.setTimeoutDuration(timeOutDuration)

      recordChunks
        .slice(0, recordChunks.length - 1)
        .flatMap(subset => executePipeline(controllerServiceLookupSink, streamContext, subset.iterator))
        .iterator
    }


    else {
      val currentChunk = state.get
      if (recordChunks.size == 1) {
        currentChunk.setObjectField(ALL_RECORDS, allRecords)
        state.update(currentChunk)
        // logger.debug("UPDATE key " + key + ", allRecords " + allRecords.size + ", recordChunks " + recordChunks.size)
        Iterator.empty
      } else {
        currentChunk.setObjectField(ALL_RECORDS, recordChunks.last)
        //logger.debug("UPDATE key " + key + ", allRecords " + allRecords.size + ", recordChunks " + recordChunks.size)

        state.update(currentChunk)
        state.setTimeoutDuration(timeOutDuration)

        recordChunks
          .slice(0, recordChunks.length - 1)
          .flatMap(subset => executePipeline(controllerServiceLookupSink, streamContext, subset.iterator))
          .iterator
      }

    }


  }

  private def executePipeline(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext, iterator: Iterator[Record])

  = {
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
      if (!processor.isInitialized)
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
  def save(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): StreamingQuery = {


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
      logger.info(s"Saving structured stream using checkpointLocation: $checkpointLocation")
    }

    write(df2, controllerServiceLookupSink, streamContext)
      .queryName(streamContext.getIdentifier)
      // .outputMode("update")
      .option("checkpointLocation", checkpointLocation)
      .start()
    // .processAllAvailable()

  }


  protected def serializeRecords(valueSerializer: RecordSerializer, keySerializer: RecordSerializer, record: Record)

  = {

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
        logger.error(s"exception while serializing events ${
          t.getMessage
        }")
        null
    }


  }

  private def doSerializeAsString(serializer: RecordSerializer, record: Record): String

  = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    serializer.serialize(baos, record)
    val bytes = baos.toByteArray
    baos.close()
    new String(bytes)


  }

  private def doSerialize(serializer: RecordSerializer, record: Record): Array[Byte]

  = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream
    serializer.serialize(baos, record)
    val bytes = baos.toByteArray
    baos.close()
    bytes


  }

  private def doDeserialize(serializer: RecordSerializer, field: Field): Record

  = {
    val f = field.getRawValue
    val s = if (f.isInstanceOf[String]) f.asInstanceOf[String].getBytes else f;
    val bais = new ByteArrayInputStream(s.asInstanceOf[Array[Byte]])
    try {
      serializer.deserialize(bais)
    } finally {
      bais.close()
    }
  }

  protected def deserializeRecords(serializer: RecordSerializer, keySerializer: RecordSerializer, r: Record)

  = {
    try {
      val deserialized = doDeserialize(serializer, r.getField(FieldDictionary.RECORD_VALUE))
      // copy root record field
      if (r.hasField(FieldDictionary.RECORD_NAME))
        deserialized.setField(r.getField(FieldDictionary.RECORD_NAME))

      // TODO : handle key stuff
     /* if (r.hasField(FieldDictionary.RECORD_KEY) && r.getField(FieldDictionary.RECORD_KEY).getRawValue != null) {

        try {
          val deserializedKey = doDeserialize(keySerializer, r.getField(FieldDictionary.RECORD_KEY))
          if (deserializedKey.hasField(FieldDictionary.RECORD_VALUE) && deserializedKey.getField(FieldDictionary.RECORD_VALUE).getRawValue != null) {
            val f = deserializedKey.getField(FieldDictionary.RECORD_VALUE)
            deserialized.setField(FieldDictionary.RECORD_KEY, f.getType, f.getRawValue)
          }
        } catch {
          case t: Throwable => logger.trace(s"Unable to serialize key for record $r with serializer $keySerializer")
        }
      }*/

      Some(deserialized)

    }

    catch {
      case t: Throwable =>
        logger.error(s"exception while deserializing events ${
          t.getMessage
        }")
        None
    }
  }


}
