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
package com.hurence.logisland.stream.spark.structured

import java.util
import java.util.Collections

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.engine.spark.remote.PipelineConfigurationBroadcastWrapper
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.SparkRecordStream
import com.hurence.logisland.stream.spark.structured.provider.StructuredStreamProviderService
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.util.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory


class StructuredStream extends AbstractRecordStream with SparkRecordStream {


  protected var provider: StructuredStreamProviderService = _


  protected var appName: String = ""
  @transient protected var ssc: StreamingContext = _
  @transient protected var streamContext: StreamContext = _
  protected var engineContext: EngineContext = _
  protected var controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink] = _
  protected var needMetricsReset = false


  private val logger = LoggerFactory.getLogger(this.getClass)

  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

    descriptors.add(READ_STREAM_SERVICE_PROVIDER)
    descriptors.add(READ_TOPICS_SERIALIZER)
    descriptors.add(READ_TOPICS_KEY_SERIALIZER)
    descriptors.add(WRITE_STREAM_SERVICE_PROVIDER)
    descriptors.add(WRITE_TOPICS_SERIALIZER)
    descriptors.add(WRITE_TOPICS_KEY_SERIALIZER)
    descriptors.add(GROUPBY)
    descriptors.add(STATE_TIMEOUT_MS)
    descriptors.add(CHUNK_SIZE)

    Collections.unmodifiableList(descriptors)
  }

  override def setup(appName: String, ssc: StreamingContext, streamContext: StreamContext, engineContext: EngineContext) = {
    this.appName = appName
    this.ssc = ssc
    this.streamContext = streamContext
    this.engineContext = engineContext
  }

  override def getStreamContext(): StreamingContext = this.ssc

  override def start() = {
    if (ssc == null)
      throw new IllegalStateException("stream not initialized")

    try {

      val pipelineMetricPrefix = streamContext.getIdentifier /*+ ".partition" + partitionId*/ + "."
      val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms").time()

      controllerServiceLookupSink = ssc.sparkContext.broadcast(
        ControllerServiceLookupSink(engineContext.getControllerServiceConfigurations)
      )
      val spark = SparkSession.builder()
        .config(this.ssc.sparkContext.getConf)
        .getOrCreate()

      spark.sqlContext.setConf("spark.sql.shuffle.partitions", "4")//TODO make this configurable


      val controllerServiceLookup = controllerServiceLookupSink.value.getControllerServiceLookup()
      streamContext.setControllerServiceLookup(controllerServiceLookup)


      val readStreamService = streamContext.getPropertyValue(READ_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderService]

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

      val readDF = readStreamService.load(spark, controllerServiceLookupSink, streamContext)

      val writeStreamService = streamContext.getPropertyValue(WRITE_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderService]

      // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
      val ds = writeStreamService.save(readDF, controllerServiceLookupSink, streamContext)
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
    val thisStream = SQLContext.getOrCreate(getStreamContext().sparkContext).streams.active.find(stream => streamContext.getIdentifier.equals(stream.name));
    if (thisStream.isDefined) {
      if (!getStreamContext().sparkContext.isStopped && thisStream.get.isActive) {
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
}


