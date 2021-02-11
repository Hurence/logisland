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

  def mappingFunction(chunkSize: Int,
                              timeOutDuration: Long)
                             (key: String,
                              value: Iterator[Record],
                              state: GroupState[Record]): Iterator[Record] = {
    SparkPipeLineMethods
      .mappingFunction(controllerServiceLookupSink,
        streamContext,
        chunkSize,
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
}
