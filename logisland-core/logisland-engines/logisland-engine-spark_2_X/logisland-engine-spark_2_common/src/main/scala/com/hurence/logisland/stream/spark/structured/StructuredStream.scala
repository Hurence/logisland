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
import java.util.Collections

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.record.Record
import com.hurence.logisland.stream.AbstractRecordStream
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.{StructuredStreamProviderServiceReader, StructuredStreamProviderServiceWriter}
import com.hurence.logisland.stream.spark.{SparkRecordStream, SparkStreamContext}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SQLContext}

class StructuredStream extends AbstractRecordStream with SparkRecordStream {

  @transient protected var sparkStreamContext: SparkStreamContext = _
  private var isReady = false
  private var keys: String = _
  private var stateTimeoutDuration: Long = _
  private var chunkSize: Int = _

  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.add(READ_STREAM_SERVICE_PROVIDER)
    descriptors.add(WRITE_STREAM_SERVICE_PROVIDER)
    Collections.unmodifiableList(descriptors)
  }

  override def init(sparkStreamContext: SparkStreamContext) = {
    super.init(sparkStreamContext.logislandStreamContext)
    this.sparkStreamContext = sparkStreamContext
    if (context.getPropertyValue(GROUPBY).isSet) {
      keys = context.getPropertyValue(GROUPBY).asString()
      stateTimeoutDuration = context.getPropertyValue(STATE_TIMEOUT_MS).asLong()
      chunkSize = context.getPropertyValue(CHUNK_SIZE).asInteger()
    }
    isReady = true;
  }

  private def context = sparkStreamContext.logislandStreamContext

  private def sparkSession = sparkStreamContext.spark

  override def start() = {
    if (!isReady)
      throw new IllegalStateException("stream not initialized")
    try {

      val pipelineMetricPrefix = context.getIdentifier /*+ ".partition" + partitionId*/ + "."
      val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms").time()

      sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "4")//TODO make this configurable

      //TODO Je pense que ces deux ligne ne servent a rien
      val controllerServiceLookup = sparkStreamContext.broadCastedControllerServiceLookupSink.value.getControllerServiceLookup()
      context.setControllerServiceLookup(controllerServiceLookup)

      //TODO stange way to update streamcontext, should'nt it be broadcasted ?
      // moreover the streamcontext should always be the last updated one in this function for me.
      // If driver wants to change it, it should call setup which would use a broadcast value for example ?
      // The remote api engines use this strange behaviour here
      // to change config on the fly when it should use the setup method (maybe using broadcast as well).
      // In this method start, the config should be considered already up to date in my opinion.
      // So currently this stream is not compatible with remoteApi change conf on the fly...
      // Anyway modification of a stream should be done at engine level !!!! stopping specific stream then init and restarting it with new StreamContext/ ProcessContext
//      sparkStreamContext.streamingContext.getProcessContexts.clear()
//      sparkStreamContext.streamingContext.getProcessContexts.addAll(
//        PipelineConfigurationBroadcastWrapper.getInstance().get(sparkStreamContext.streamingContext.getIdentifier))

      val readDF = context.getPropertyValue(READ_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderServiceReader].read(sparkSession)

      val transformedInputData: Dataset[Record] = transformInputData(readDF)

      val writeStreamService = context.getPropertyValue(WRITE_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderServiceWriter]
        .write(transformedInputData, sparkStreamContext.broadCastedControllerServiceLookupSink)

      pipelineTimerContext.stop()
    }
    catch {
      case ex: Throwable =>
        getLogger.error("Error while processing the streaming query. ", ex)
        throw new IllegalStateException("Error while processing the streaming query", ex)
    }
  }

  override def stop(): Unit = {
    super.stop()
    //stop the source

    val thisStream = SQLContext.getOrCreate(sparkSession.sparkContext).streams.active.find(stream => context.getIdentifier.equals(stream.name));
    if (thisStream.isDefined) {
      if (!sparkSession.sparkContext.isStopped && thisStream.get.isActive) {
        try {
          thisStream.get.stop()
          thisStream.get.awaitTermination()
        } catch {
          case ex: Throwable => getLogger.warn(s"Stream ${context.getIdentifier} may not have been correctly stopped")
        }
      }
    } else {
      getLogger.warn(s"Unable to find an active streaming query for stream ${context.getIdentifier}")
    }
    this.isReady = false
  }

  def transformInputData(readDF: Dataset[Record]): Dataset[Record] = {
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]
    val pipelineMethods = new SparkPipeLineMethods(sparkStreamContext)
    if (context.getPropertyValue(GROUPBY).isSet) {
      import readDF.sparkSession.implicits._
      readDF
        .filter(_.hasField(keys))
        .groupByKey(_.getField(keys).asString())
        .flatMapGroupsWithState(outputMode = OutputMode.Append, timeoutConf = GroupStateTimeout.ProcessingTimeTimeout())(
          pipelineMethods.mappingFunction(chunkSize, stateTimeoutDuration)
        )
    } else {
      readDF.mapPartitions(iterator => {
        pipelineMethods.executePipeline(iterator)
      })
    }
  }
}