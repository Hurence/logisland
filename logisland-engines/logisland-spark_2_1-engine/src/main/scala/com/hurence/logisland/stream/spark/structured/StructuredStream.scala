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
import com.hurence.logisland.logging.StandardComponentLogger
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.SparkRecordStream
import com.hurence.logisland.stream.spark.structured.handler.StructuredStreamHandler
import com.hurence.logisland.stream.spark.structured.provider.StructuredStreamProviderService
import com.hurence.logisland.stream.{AbstractRecordStream, StreamContext}
import com.hurence.logisland.util.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

class StructuredStream extends AbstractRecordStream with SparkRecordStream {


    protected var handler: StructuredStreamHandler = _
    protected var provider: StructuredStreamProviderService = _


    protected var appName: String = ""
    @transient protected var ssc: StreamingContext = _
    @transient protected var streamContext: StreamContext = _
    protected var engineContext: EngineContext = _
    protected var controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink] = _
    protected var needMetricsReset = false


    private val logger = new StandardComponentLogger(this.getIdentifier, this.getClass)

    override def getSupportedPropertyDescriptors() = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

        descriptors.add(READ_TOPICS)
        descriptors.add(READ_TOPICS_CLIENT_SERVICE)
        descriptors.add(READ_TOPICS_SERIALIZER)
        descriptors.add(READ_TOPICS_KEY_SERIALIZER)
        descriptors.add(WRITE_TOPICS)
        descriptors.add(WRITE_TOPICS_CLIENT_SERVICE)
        descriptors.add(WRITE_TOPICS_SERIALIZER)
        descriptors.add(WRITE_TOPICS_KEY_SERIALIZER)

        Collections.unmodifiableList(descriptors)
    }

    override def setup(appName: String, ssc: StreamingContext, streamContext: StreamContext, engineContext: EngineContext) = {
        this.appName = appName
        this.ssc = ssc
        this.streamContext = streamContext
        this.engineContext = engineContext
        SparkUtils.customizeLogLevels
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


            val spark = SparkSession.builder().getOrCreate()


            val controllerServiceLookup = controllerServiceLookupSink.value.getControllerServiceLookup()
            streamContext.addControllerServiceLookup(controllerServiceLookup)


            val readStreamService = streamContext.getPropertyValue(READ_TOPICS_CLIENT_SERVICE)
                .asControllerService()
                .asInstanceOf[StructuredStreamProviderService]




            val readDF = readStreamService.load(spark, controllerServiceLookupSink, streamContext)

            // apply windowing
            /*val windowedDF:Dataset[Record] = if (streamContext.getPropertyValue(WINDOW_DURATION).isSet) {
                if (streamContext.getPropertyValue(SLIDE_DURATION).isSet)
                    readDF.groupBy(
                        window($"timestamp",
                            streamContext.getPropertyValue(WINDOW_DURATION).asLong() + " seconds",
                            streamContext.getPropertyValue(SLIDE_DURATION).asLong() + " seconds")
                    )
                else
                    readDF.groupBy(window($"timestamp",
                        streamContext.getPropertyValue(WINDOW_DURATION).asLong() + " seconds")
                    )
            } else readDF*/

            //   val processedDF = handler.process(streamContext, controllerServiceLookupSink, windowedDF)


            val writeStreamService = streamContext.getPropertyValue(WRITE_TOPICS_CLIENT_SERVICE)
                .asControllerService()
                .asInstanceOf[StructuredStreamProviderService]


            // Write key-value data from a DataFrame to a specific Kafka topic specified in an option
            val ds = writeStreamService.save(readDF, streamContext)
            pipelineTimerContext.stop()

        } catch {
            case ex: Throwable =>
                logger.error("something bad happened, please check Kafka or Zookeeper health : {}", ex)
        }
    }

}


