package com.hurence.logisland.stream.spark.structured.provider

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.serializer.{RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    protected def write(df: Dataset[Record], streamContext: StreamContext)

    /**
      *
      *
      * @param spark
      * @param streamContext
      * @return
      */
    def load(spark: SparkSession, controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink], streamContext: StreamContext): Dataset[Record] = {
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]


        val df = read(spark, streamContext)

        df.mapPartitions(iterator => {


            val controllerServiceLookup = controllerServiceLookupSink.value.getControllerServiceLookup()

            /**
              * create serializers
              */
            val serializer = SerializerProvider.getSerializer(
                streamContext.getPropertyValue(READ_TOPICS_SERIALIZER).asString,
                streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)


            val pipelineMetricPrefix = streamContext.getIdentifier /*+ ".partition" + partitionId*/ + "."
            val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms").time()


            // convert to logisland records
            val incomingEvents = iterator.toList
                .flatMap(r => {
                    var processingRecords: util.Collection[Record] = deserializeRecords(serializer, r).toList

                    // loop over processor chain
                    streamContext.getProcessContexts.foreach(processorContext => {
                        val startTime = System.currentTimeMillis()
                        val processor = processorContext.getProcessor

                        val processorTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix +
                            processorContext.getName + ".processing_time_ms").time()


                        // injects controller service lookup into processor context
                        if (processor.hasControllerService) {
                            //  val controllerServiceLookup = streamContext.getControllerServiceLookup
                            processorContext.addControllerServiceLookup(controllerServiceLookup)
                        }

                        // processor setup (don't forget that)
                        processor.init(processorContext)

                        // do the actual processing
                        processingRecords = processor.process(processorContext, processingRecords)

                        // compute metrics
                        /*   ProcessorMetrics.computeMetrics(
                               pipelineMetricPrefix + processorContext.getName + ".",
                               incomingEvents.toList,
                               processingRecords,
                               0,
                               0,
                               System.currentTimeMillis() - startTime)*/

                        processorTimerContext.stop()

                        processingRecords
                    })
                    processingRecords
                })
                .iterator

            incomingEvents
        })


    }


    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    def save(df: Dataset[Record], streamContext: StreamContext) = {

        // make sure controller service lookup won't be serialized !!
        streamContext.addControllerServiceLookup(null)

       /* val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]




        // do the parallel processing
        df.mapPartitions(partition => {

            // create serializer
            val serializer = SerializerProvider.getSerializer(
                streamContext.getPropertyValue(WRITE_TOPICS_SERIALIZER).asString,
                streamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)

            partition.toList
                .flatMap(r => serializeRecords(serializer, r))
                .iterator
        })*/

        write(df, streamContext)

    }


    protected def serializeRecords(serializer: RecordSerializer, record: Record): Array[Byte] = {


        // messages are serialized with kryo first
        val baos: ByteArrayOutputStream = new ByteArrayOutputStream
        serializer.serialize(baos, record)

        // and then converted to KeyedMessage
        val key = if (record.hasField(FieldDictionary.RECORD_ID))
            record.getField(FieldDictionary.RECORD_ID).asString()
        else
            ""

        val bytes = baos.toByteArray
        baos.close()

        bytes
    }

    // TODO handle key also
    protected def deserializeRecords(serializer: RecordSerializer, r: Record) = {
        try {
            val bais = new ByteArrayInputStream(r.getField(FieldDictionary.RECORD_VALUE).getRawValue.asInstanceOf[Array[Byte]])
            val deserialized = serializer.deserialize(bais)
            bais.close()

            // copy root record field
            if(r.hasField(FieldDictionary.RECORD_NAME))
                deserialized.setField(r.getField(FieldDictionary.RECORD_NAME))

            Some(deserialized)
        } catch {
            case t: Throwable =>
                logger.error(s"exception while deserializing events ${t.getMessage}")
                None
        }
    }


}
