/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.stream.spark.structured

import java.util
import java.util.Date

import com.hurence.logisland.record.{FieldDictionary, Record, StandardRecord}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.spark.SparkStreamContext
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, ProcessorMetrics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming.GroupState
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


class SparkPipeLineMethods(val streamContext: StreamContext,
                           val controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]) extends Serializable {

  def this(sparkStreamContext: SparkStreamContext) {
    this(sparkStreamContext.logislandStreamContext, sparkStreamContext.broadCastedControllerServiceLookupSink)
  }

  def executePipeline(iterator: Iterator[Record]) = {
    SparkPipeLineMethods.executePipeline(controllerServiceLookupSink, streamContext, iterator)
  }

  def executePipeline(key: String, iterator: Iterator[Record]) = {
    SparkPipeLineMethods.executePipeline(controllerServiceLookupSink, streamContext, iterator)
  }


  def mappingFunction2(timeOutDuration: Long)
                      (key: String,
                       value: Iterator[Record],
                       state: GroupState[Record]): Iterator[Record] = {
    SparkPipeLineMethods
      .mappingFunction(controllerServiceLookupSink,
        streamContext,
        timeOutDuration)(key, value, state)
  }

}

object SparkPipeLineMethods {
  private val logger = LoggerFactory.getLogger(classOf[SparkPipeLineMethods])

  private def executePipeline(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                              streamContext: StreamContext,
                              iterator: Iterator[Record]) = {
    val controllerServiceLookup = controllerServiceLookupSink.value.getControllerServiceLookup()

    // convert to logisland records
    var processingRecords: util.Collection[Record] = iterator.toList

    if (processingRecords.size() > 0) {
      val pipelineMetricPrefix = streamContext.getIdentifier + "."
      // loop over processor chain
      streamContext.getProcessContexts.foreach(processorContext => {
        val startTime = System.currentTimeMillis()
        val processor = processorContext.getProcessor
        /*
          Does this cause problem ? same instance used in concurrence ?
          I tested to serialize an object into a mapPArtition transformation
          and could not find any bug related to concurrence. It seems for each partition we got a new instance
          of the processor.
        */
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

  //1 bourrin
  //process current records (enrichment)
  // concatenates with old already processed
  // => apply sessionization output all records

  //2 statefull intelligent
  // If no state exist => init state with Es GroupSate<WebSessions> (BESOIN UNIQUEMENT POUR REWIND car restart checkpoint devrait le sauvegarder ? => à tester)
  // process current records then apply sessionization and save sessionization state in GroupSate<WebSessions>
  // WebSessions doit stoquer toutes les sous sessions accosiciés à un divolteId. Ainsi il peut mettre à jour la session correspondante aux évènements arrivants.
  //
  // on output tous les évènements qui ont été modifiés ainsi que toutes les sessions qui ont été modifiés.
  private def sessionization(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                               streamContext: StreamContext,
                               timeOutDuration: Long)
                              (key: String,
                               value: Iterator[Record],
                               state: GroupState[Record]): Iterator[Record] = {

    val currentTimestamp = new Date().getTime
    val inputRecords = value.toList
    val allRecords = if (state.exists) state.get.getField(ALL_RECORDS).getRawValue.asInstanceOf[List[Record]] ++ inputRecords else inputRecords
    //    val recordChunks = allRecords.grouped(chunkSize).toList

    if (state.hasTimedOut || (state.exists && (currentTimestamp - state.get.getField(CHUNK_CREATION_TS).asLong()) >= timeOutDuration)) {
      state.remove()
      streamContext.getLogger.debug("TIMEOUT session " + key + ", flushing " + allRecords.size + " events.")
      executePipeline(controllerServiceLookupSink, streamContext, allRecords.iterator)
    }
    else if (!state.exists) {
      val newChunk = new StandardRecord("chunk_record") //Chunk(key, recordChunks.last)
      newChunk.setObjectField(ALL_RECORDS, allRecords)
      newChunk.setStringField(FieldDictionary.RECORD_KEY, key)
      newChunk.setLongField(CHUNK_CREATION_TS, new Date().getTime)
      streamContext.getLogger.debug("CREATE session " + key + " new session with " + allRecords.size + " events")
      state.update(newChunk)
      state.setTimeoutDuration(timeOutDuration)
      executePipeline(controllerServiceLookupSink, streamContext, allRecords.iterator)
    }
    else {
      val passedEvents = state.get
      passedEvents.setObjectField(ALL_RECORDS, allRecords)
      state.update(passedEvents)
      state.setTimeoutDuration(timeOutDuration)
      executePipeline(controllerServiceLookupSink, streamContext, allRecords.iterator)
    }
  }

  private val ALL_RECORDS = "all_records"
  private val CHUNK_CREATION_TS = "chunk_creation_ts"


  private def mappingFunction(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                               streamContext: StreamContext,
                               timeOutDuration: Long)
                              (key: String,
                               value: Iterator[Record],
                               state: GroupState[Record]): Iterator[Record] = {

    val currentTimestamp = new Date().getTime
    val inputRecords = value.toList
    val allRecords = if (state.exists) state.get.getField(ALL_RECORDS).getRawValue.asInstanceOf[List[Record]] ++ inputRecords else inputRecords

    if (state.hasTimedOut || (state.exists && (currentTimestamp - state.get.getField(CHUNK_CREATION_TS).asLong()) >= timeOutDuration)) {
      state.remove()
      streamContext.getLogger.debug("TIMEOUT session " + key + ", flushing " + allRecords.size + " events.")
      executePipeline(controllerServiceLookupSink, streamContext, allRecords.iterator)
    }
    else if (!state.exists) {
      val newChunk = new StandardRecord("chunk_record") //Chunk(key, recordChunks.last)
      newChunk.setObjectField(ALL_RECORDS, allRecords)
      newChunk.setStringField(FieldDictionary.RECORD_KEY, key)
      newChunk.setLongField(CHUNK_CREATION_TS, new Date().getTime)
      streamContext.getLogger.debug("CREATE session " + key + " new session with " + allRecords.size + " events")
      state.update(newChunk)
      state.setTimeoutDuration(timeOutDuration)
      executePipeline(controllerServiceLookupSink, streamContext, allRecords.iterator)
    }
    else {
      val passedEvents = state.get
      passedEvents.setObjectField(ALL_RECORDS, allRecords)
      state.update(passedEvents)
      state.setTimeoutDuration(timeOutDuration)
      executePipeline(controllerServiceLookupSink, streamContext, allRecords.iterator)
    }
  }

}
