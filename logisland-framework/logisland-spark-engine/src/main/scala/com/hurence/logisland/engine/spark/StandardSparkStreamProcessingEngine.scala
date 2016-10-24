/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.engine.spark

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.behavior.{DynamicProperty, ReadsAttribute, WritesAttribute, WritesAttributes}
import com.hurence.logisland.annotation.documentation.{CapabilityDescription, SeeAlso, Tags}
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.processor.chain.{KafkaRecordStream, StandardProcessorChainInstance}
import com.hurence.logisland.record.{FieldDictionary, Record, RecordUtils}
import com.hurence.logisland.util.kafka.KafkaSink
import com.hurence.logisland.util.spark.ZookeeperSink
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

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
@Tags(Array("engine", "spark", "kafka"))
@CapabilityDescription("This is a spark streaming engine")
class StandardSparkStreamProcessingEngine extends AbstractSparkStreamProcessingEngine {

    def process(rdd: RDD[(Array[Byte], Array[Byte])],
                engineContext: EngineContext,
                processorChainInstance: StandardProcessorChainInstance,
                kafkaSink: Broadcast[KafkaSink],
                zkSink: Broadcast[ZookeeperSink]): Unit = {

        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            val appName = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_APP_NAME).asString
            val batchDuration = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
            val maxRatePerPartition = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION).asInteger().intValue()
            val blockInterval = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL).asInteger().intValue()
            val processorChainContext = new StandardProcessContext(processorChainInstance)
            val inputTopics = processorChainContext.getProperty(KafkaRecordStream.INPUT_TOPICS).asString
            val outputTopics = processorChainContext.getProperty(KafkaRecordStream.OUTPUT_TOPICS).asString
            val errorTopics = processorChainContext.getProperty(KafkaRecordStream.ERROR_TOPICS).asString
            val brokerList = processorChainContext.getProperty(KafkaRecordStream.KAFKA_METADATA_BROKER_LIST).asString

            rdd.foreachPartition(partition => {
                if (partition.nonEmpty) {
                    /**
                      * index to get the correct offset range for the rdd partition we're working on
                      * This is safe because we haven't shuffled or otherwise disrupted partitioning,
                      * and the original input rdd partitions were 1:1 with kafka partitions
                      */
                    val partitionId = TaskContext.get.partitionId()
                    val offsetRange = offsetRanges(TaskContext.get.partitionId)

                    /**
                      * create serializers
                      */
                    val deserializer = getSerializer(
                        processorChainContext.getProperty(KafkaRecordStream.INPUT_SERIALIZER).asString,
                        processorChainContext.getProperty(KafkaRecordStream.AVRO_INPUT_SCHEMA).asString)
                    val serializer = getSerializer(
                        processorChainContext.getProperty(KafkaRecordStream.OUTPUT_SERIALIZER).asString,
                        processorChainContext.getProperty(KafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)
                    val errorSerializer = getSerializer(
                        processorChainContext.getProperty(KafkaRecordStream.ERROR_SERIALIZER).asString,
                        processorChainContext.getProperty(KafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)

                    /**
                      * process events by chaining output records
                      */
                    var firstPass = true
                    var incomingEvents: util.Collection[Record] = Collections.emptyList()
                    var outgoingEvents: util.Collection[Record] = Collections.emptyList()
                    val processingMetrics: util.Collection[Record] = new util.ArrayList[Record]()


                    processorChainInstance.getProcessorInstances.foreach(processorInstance => {
                        val startTime = System.currentTimeMillis()
                        val processorContext = new StandardProcessContext(processorInstance)
                        val processor = processorInstance.getProcessor


                        if (firstPass) {
                            /**
                              * convert incoming Kafka messages into Records
                              * if there's no serializer we assume that we need to compute a Record from K/V
                              */
                            incomingEvents = if (
                                processorChainContext.getProperty(KafkaRecordStream.INPUT_SERIALIZER).asString
                                    == KafkaRecordStream.NO_SERIALIZER.getValue) {
                                // parser
                                partition.map(rawMessage => {
                                    val key = if (rawMessage._1 != null) new String(rawMessage._1) else ""
                                    val value = if (rawMessage._2 != null) new String(rawMessage._2) else ""
                                    RecordUtils.getKeyValueRecord(key, value)
                                }).toList
                            } else {
                                // processor
                                deserializeEvents(partition, deserializer)
                            }

                            firstPass = false
                        } else {
                            incomingEvents = outgoingEvents
                        }

                        /**
                          * process incoming events
                          */
                        outgoingEvents = processor.process(processorContext, incomingEvents)

                        /**
                          * send metrics if requested
                          */
                        processingMetrics.addAll(
                            computeMetrics(maxRatePerPartition, appName, blockInterval, batchDuration, brokerList,
                                processorChainContext, inputTopics, outputTopics, partition, startTime, partitionId,
                                serializer, incomingEvents, outgoingEvents, offsetRange))

                    })


                    /**
                      * push outgoing events and errors to Kafka
                      */
                    kafkaSink.value.produce(
                        processorChainContext.getProperty(KafkaRecordStream.OUTPUT_TOPICS).asString,
                        outgoingEvents.toList,
                        serializer
                    )

                    kafkaSink.value.produce(
                        processorChainContext.getProperty(KafkaRecordStream.ERROR_TOPICS).asString,
                        outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERROR)).toList,
                        errorSerializer
                    )

                    kafkaSink.value.produce(
                        processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).asString,
                        processingMetrics.toList,
                        serializer
                    )


                    /**
                      * save latest offset to Zookeeper
                      */
                    zkSink.value.saveOffsetRangesToZookeeper(appName, offsetRange)
                }
            })
        }
    }
}
