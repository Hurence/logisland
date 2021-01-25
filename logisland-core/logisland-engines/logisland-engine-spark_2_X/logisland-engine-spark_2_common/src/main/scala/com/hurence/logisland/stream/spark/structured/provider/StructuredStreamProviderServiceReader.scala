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

import java.util
import java.util.Date

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.logging.ComponentLog
import com.hurence.logisland.record._
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, ProcessorMetrics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


trait StructuredStreamProviderServiceReader extends ControllerService {

  protected def getLogger: ComponentLog

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param spark
    * @param streamContext
    * @return DataFrame currently loaded
    */
  protected def read(spark: SparkSession, streamContext: StreamContext): Dataset[Record]

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

    if (streamContext.getPropertyValue(GROUPBY).isSet) {
      val keys = streamContext.getPropertyValue(GROUPBY).asString()
      val stateTimeoutDuration = streamContext.getPropertyValue(STATE_TIMEOUT_MS).asLong()
      val chunkSize = streamContext.getPropertyValue(CHUNK_SIZE).asInteger()
      df
        .filter(_.hasField(keys))
        .groupByKey(_.getField(keys).asString())
        .flatMapGroupsWithState(outputMode = OutputMode.Append, timeoutConf = GroupStateTimeout.ProcessingTimeTimeout())(
          mappingFunction(controllerServiceLookupSink, streamContext, chunkSize, stateTimeoutDuration)
        )
    } else {
      df.mapPartitions(iterator => {
        executePipeline(controllerServiceLookupSink, streamContext, iterator)
      })
    }
  }

  private val ALL_RECORDS = "all_records"
  private val CHUNK_CREATION_TS = "chunk_creation_ts"

  private def mappingFunction(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
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

    if (processingRecords.size() >0) {
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
        if (!processor.isInitialized) {
          processor.init(processorContext)
        }
        processor.start()
        // do the actual processing
        processingRecords = processor.process(processorContext, processingRecords)
        processor.stop()

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
    }

    processingRecords.asScala.iterator
  }
}
