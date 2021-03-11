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
package com.hurence.logisland.stream.spark

import java.util
import java.util.Collections
import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.record.{FieldDictionary, Record, RecordUtils}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.KafkaProperties.{ERROR_SERIALIZER, ERROR_TOPICS, INPUT_SERIALIZER, INPUT_TOPICS, KAFKA_METADATA_BROKER_LIST, OUTPUT_SERIALIZER, OUTPUT_TOPICS,AVRO_SCHEMA_URL,AVRO_SCHEMA_NAME,AVRO_SCHEMA_VERSION}
import com.hurence.logisland.util.record.RecordSchemaUtil
import com.hurence.logisland.util.spark.ProcessorMetrics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.spark.TaskContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


class KafkaRecordStreamParallelProcessing extends AbstractKafkaRecordStream {
    val logger = LoggerFactory.getLogger(this.getClass)

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

        descriptors.addAll(super.getSupportedPropertyDescriptors())
        Collections.unmodifiableList(descriptors)
    }



    /**
      * launch the chain of processing for each partition of the RDD in parallel
      *
      * @param rdd
      */
    override def process(rdd: RDD[ConsumerRecord[Array[Byte], Array[Byte]]]): Option[Array[OffsetRange]] = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            rdd.foreachPartition(partition => {
                try {
                    if (partition.nonEmpty) {
                        /**
                          * index to get the correct offset range for the rdd partition we're working on
                          * This is safe because we haven't shuffled or otherwise disrupted partitioning,
                          * and the original input rdd partitions were 1:1 with kafka partitions
                          */
                        val partitionId = TaskContext.get.partitionId()
                        val offsetRange = offsetRanges(TaskContext.get.partitionId)

                        val pipelineMetricPrefix = sparkStreamContext.logislandStreamContext.getIdentifier + "." +
                            "partition" + partitionId + "."
                        val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms" ).time()
                        var inputSchema = ""

                        if (sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_SCHEMA_NAME).isSet
                            && sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_SCHEMA_URL).isSet) {

                          val schemaUrl = sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_SCHEMA_URL).asString
                          val schemaName = sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_SCHEMA_NAME).asString

                          if (sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_SCHEMA_VERSION).isSet) {
                            val schemaVersion = sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_SCHEMA_VERSION).asInteger
                            inputSchema = "{\"schemaName\":\"" + schemaName + "\"," + "\"schemaUrl\":\"" + schemaUrl + "\"," + "\"schemaVersion\":" +schemaVersion+ "}"
                          } else { 
                            inputSchema = "{\"schemaName\":\"" + schemaName + "\"," + "\"schemaUrl\":\"" + schemaUrl + "\"}"
                          }
                          logger.info("Using schema json " + inputSchema)
                          
                        } else { inputSchema = sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString}
                        /**
                          * create serializers
                          */
                        val deserializer = getSerializer(
                            sparkStreamContext.logislandStreamContext.getPropertyValue(INPUT_SERIALIZER).asString,
                            inputSchema)
                        val serializer = getSerializer(
                            sparkStreamContext.logislandStreamContext.getPropertyValue(OUTPUT_SERIALIZER).asString,
                            sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)
                        val errorSerializer = getSerializer(
                            sparkStreamContext.logislandStreamContext.getPropertyValue(ERROR_SERIALIZER).asString,
                            sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)

                        /**
                          * process events by chaining output records
                          */
                        var firstPass = true
                        var incomingEvents: util.Collection[Record] = Collections.emptyList()
                        var outgoingEvents: util.Collection[Record] = Collections.emptyList()

                        sparkStreamContext.logislandStreamContext.getProcessContexts.foreach(processorContext => {
                            val startTime = System.currentTimeMillis()
                            val processor = processorContext.getProcessor

                            val processorTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix +
                                processorContext.getIdentifier + ".processing_time_ms").time()
                            /**
                              * convert incoming Kafka messages into Records
                              * if there's no serializer we assume that we need to compute a Record from K/V
                              */
                            if (firstPass) {
                                incomingEvents = if (
                                    sparkStreamContext.logislandStreamContext.getPropertyValue(INPUT_SERIALIZER).asString
                                        == NO_SERIALIZER.getValue) {
                                    // parser
                                    partition.map(rawMessage => {
                                        val key = if (rawMessage.key() != null) new String(rawMessage.key()) else ""
                                        val value = if (rawMessage.value() != null) new String(rawMessage.value()) else ""
                                        RecordUtils.getKeyValueRecord(key, value)
                                    }).toList
                                } else {
                                    // processor
                                    deserializeRecords(partition, deserializer)
                                }

                                firstPass = false
                            } else {
                                incomingEvents = outgoingEvents
                            }

                            /**
                              * process incoming events
                              */
                            if (processor.hasControllerService) {
                                val controllerServiceLookup = sparkStreamContext.broadCastedControllerServiceLookupSink.value.getControllerServiceLookup()
                                processorContext.setControllerServiceLookup(controllerServiceLookup)
                            }

                            if (!processor.isInitialized) {
                                processor.init(processorContext)
                            }
                            processor.start()
                            outgoingEvents = processor.process(processorContext, incomingEvents)
                            processor.stop()
                            /**
                              * compute metrics
                              */
                            ProcessorMetrics.computeMetrics(
                                pipelineMetricPrefix + processorContext.getIdentifier + ".",
                                incomingEvents,
                                outgoingEvents,
                                offsetRange.fromOffset,
                                offsetRange.untilOffset,
                                System.currentTimeMillis() - startTime)

                            processorTimerContext.stop()
                        })


                        /**
                          * Do we make records compliant with a given Avro schema ?
                          */
                        if (sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).isSet) {
                            try {
                                val strSchema = sparkStreamContext.logislandStreamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString()
                                val schema = RecordSchemaUtil.compileSchema(strSchema)

                                outgoingEvents = outgoingEvents.map(record => RecordSchemaUtil.convertToValidRecord(record, schema))
                            } catch {
                                case t: Throwable =>
                                    logger.warn("something wrong while converting records " +
                                        "to valid accordingly to provide Avro schema " + t.getMessage)
                            }

                        }

                        /**
                          * push outgoing events and errors to Kafka
                          */
                        kafkaSink.value.produce(
                            sparkStreamContext.logislandStreamContext.getPropertyValue(OUTPUT_TOPICS).asString,
                            outgoingEvents.toList,
                            serializer
                        )

                        kafkaSink.value.produce(
                            sparkStreamContext.logislandStreamContext.getPropertyValue(ERROR_TOPICS).asString,
                            outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERRORS)).toList,
                            errorSerializer
                        )

                        pipelineTimerContext.stop()
                    }
                }
                catch {
                    case ex: OffsetOutOfRangeException =>
                        val inputTopics = sparkStreamContext.logislandStreamContext.getPropertyValue(INPUT_TOPICS).asString
                        val brokerList = sparkStreamContext.logislandStreamContext.getPropertyValue(KAFKA_METADATA_BROKER_LIST).asString
                       /* val latestOffsetsString = zkSink.value.loadOffsetRangesFromZookeeper(
                            brokerList,
                            appName,
                            inputTopics.split(",").toSet)
                            .map(t => s"${t._1.topic}_${t._1.partition}:${t._2}")
                            .mkString(", ")
                        val offestsString = offsetRanges
                            .map(o => s"${o.topic}_${o.partition}:${o.fromOffset}/${o.untilOffset}")
                            .mkString(", ")
                        logger.error(s"unable to process partition. current Offsets $offestsString latest offsets $latestOffsetsString")*/
                        logger.error(s"exception : ${ex.toString}")

                }
            })
            Some(offsetRanges)
        }
        else None
    }

}

