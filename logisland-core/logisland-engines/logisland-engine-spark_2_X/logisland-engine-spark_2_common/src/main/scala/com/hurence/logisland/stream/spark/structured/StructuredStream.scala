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
package com.hurence.logisland.stream.spark.structured

import java.util
import java.util.{Collections, Date}

import com.hurence.logisland.component.{ComponentContext, PropertyDescriptor}
import com.hurence.logisland.engine.spark.remote.PipelineConfigurationBroadcastWrapper
import com.hurence.logisland.record.{FieldDictionary, Record, StandardRecord}
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.{StructuredStreamProviderServiceReader, StructuredStreamProviderServiceWriter}
import com.hurence.logisland.stream.spark.{SparkRecordStream, SparkStreamContext}
import com.hurence.logisland.util.spark.{ControllerServiceLookupSink, ProcessorMetrics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StructuredStream extends AbstractRecordStream with SparkRecordStream {

  protected var appName: String = ""
  @transient protected var ssc: StreamingContext = _
  @transient protected var streamContext: SparkStreamContext = _
  protected var needMetricsReset = false


  private val logger = LoggerFactory.getLogger(this.getClass)

  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.add(READ_STREAM_SERVICE_PROVIDER)
    descriptors.add(WRITE_STREAM_SERVICE_PROVIDER)
    Collections.unmodifiableList(descriptors)
  }

  override def init(streamContext: SparkStreamContext) = {
    super.init(streamContext.asInstanceOf[ComponentContext])
    this.appName = streamContext.appName
    this.ssc = streamContext.ssc
    this.streamContext = streamContext
  }

  private def getStreamingContext(): StreamingContext = this.ssc


  override def start() = {
    if (ssc == null)
      throw new IllegalStateException("stream not initialized")
    try {

      val pipelineMetricPrefix = streamContext.getIdentifier /*+ ".partition" + partitionId*/ + "."
      val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms").time()

      val spark = SparkSession.builder()
        .config(this.ssc.sparkContext.getConf)
        .getOrCreate()

      spark.sqlContext.setConf("spark.sql.shuffle.partitions", "4")//TODO make this configurable

      //TODO Je pense que ces deux ligne ne servent a rien
      val controllerServiceLookup = streamContext.broadCastedControllerServiceLookupSink.value.getControllerServiceLookup()
      streamContext.setControllerServiceLookup(controllerServiceLookup)

      val readStreamService = streamContext.getPropertyValue(READ_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderServiceReader]

      //TODO stange way to update streamcontext, should'nt it be broadcasted ?
      // moreover the streamcontext should always be the last updated one in this function for me.
      // If driver wants to change it, it should call setup which would use a broadcast value for example ?
      // Unfortunately we should not attempt changes before having good unit test so that we do not broke streams
      // while cleaning streams code... Indeed I am afraid the remote api engines use this strange behaviour here
      // to change config on the fly when it should use the setup method (maybe using broadcast as well).
      // In this method start, the config should be considered already up to date in my opinion.
      streamContext.getProcessContexts.clear()
      streamContext.getProcessContexts.addAll(
        PipelineConfigurationBroadcastWrapper.getInstance().get(streamContext.getIdentifier))

      val readDF = readStreamService.read(spark)
      val transformedInputData: Dataset[Record] = transformInputData(readDF)

      val writeStreamService = streamContext.getPropertyValue(WRITE_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderServiceWriter]
        .write(transformedInputData, streamContext.broadCastedControllerServiceLookupSink)

      pipelineTimerContext.stop()
    }
    catch {
      case ex: Throwable =>
        logger.error("Error while processing the streaming query. ", ex)
        throw new IllegalStateException("Error while processing the streaming query", ex)
    }
  }

  override def stop(): Unit

  = {
    super.stop()
    //stop the source
    val thisStream = SQLContext.getOrCreate(getStreamingContext().sparkContext).streams.active.find(stream => streamContext.getIdentifier.equals(stream.name));
    if (thisStream.isDefined) {
      if (!getStreamingContext().sparkContext.isStopped && thisStream.get.isActive) {
        try {
          thisStream.get.stop()
          thisStream.get.awaitTermination()
        } catch {
          case ex: Throwable => logger.warn(s"Stream ${streamContext.getIdentifier} may not have been correctly stopped")
        }
      }
    } else {
      logger.warn(s"Unable to find an active streaming query for stream ${streamContext.getIdentifier}")
    }
  }

  def transformInputData(readDF: Dataset[Record]): Dataset[Record] = {
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]
    if (streamContext.getPropertyValue(GROUPBY).isSet) {
      val keys = streamContext.getPropertyValue(GROUPBY).asString()
      val stateTimeoutDuration = streamContext.getPropertyValue(STATE_TIMEOUT_MS).asLong()
      val chunkSize = streamContext.getPropertyValue(CHUNK_SIZE).asInteger()
      import readDF.sparkSession.implicits._
      readDF
        .filter(_.hasField(keys))
        .groupByKey(_.getField(keys).asString())
        .flatMapGroupsWithState(outputMode = OutputMode.Append, timeoutConf = GroupStateTimeout.ProcessingTimeTimeout())(
          mappingFunction(streamContext.broadCastedControllerServiceLookupSink, streamContext, chunkSize, stateTimeoutDuration)
        )
    } else {
      readDF.mapPartitions(iterator => {
        executePipeline(streamContext.broadCastedControllerServiceLookupSink, streamContext, iterator)
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


  private def executePipeline(controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink],
                              streamContext: StreamContext,
                              iterator: Iterator[Record]) = {
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


