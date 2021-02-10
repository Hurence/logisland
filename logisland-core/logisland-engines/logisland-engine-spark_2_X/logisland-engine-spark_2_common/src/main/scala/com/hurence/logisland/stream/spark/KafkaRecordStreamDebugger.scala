/**
  * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import com.hurence.logisland.record.{FieldDictionary, Record, RecordUtils}
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.util.record.RecordSchemaUtil
import com.hurence.logisland.util.spark.ProcessorMetrics
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.KafkaProperties.{ERROR_SERIALIZER, ERROR_TOPICS, INPUT_SERIALIZER, INPUT_TOPICS, KAFKA_METADATA_BROKER_LIST, OUTPUT_SERIALIZER, OUTPUT_TOPICS}

class KafkaRecordStreamDebugger extends AbstractKafkaRecordStream {
    val logger = LoggerFactory.getLogger(this.getClass.getName)


    /**
      * launch the chain of processing for each partition of the RDD in parallel
      *
      * @param rdd
      */
    override def process(rdd: RDD[ConsumerRecord[Array[Byte], Array[Byte]]]): Option[Array[OffsetRange]] = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            val inputTopics = sparkStreamContext.streamingContext.getPropertyValue(INPUT_TOPICS).asString
            val outputTopics = sparkStreamContext.streamingContext.getPropertyValue(OUTPUT_TOPICS).asString
            val errorTopics = sparkStreamContext.streamingContext.getPropertyValue(ERROR_TOPICS).asString
            val brokerList = sparkStreamContext.streamingContext.getPropertyValue(KAFKA_METADATA_BROKER_LIST).asString


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
                        sparkStreamContext.streamingContext.getPropertyValue(INPUT_SERIALIZER).asString,
                        sparkStreamContext.streamingContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)
                    val serializer = getSerializer(
                        sparkStreamContext.streamingContext.getPropertyValue(OUTPUT_SERIALIZER).asString,
                        sparkStreamContext.streamingContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)
                    val errorSerializer = getSerializer(
                        sparkStreamContext.streamingContext.getPropertyValue(ERROR_SERIALIZER).asString,
                        sparkStreamContext.streamingContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)

                    /**
                      * process events by chaining output records
                      */
                    var firstPass = true
                    var incomingEvents: util.Collection[Record] = Collections.emptyList()
                    var outgoingEvents: util.Collection[Record] = Collections.emptyList()
                    val processingMetrics: util.Collection[Record] = new util.ArrayList[Record]()
                    logger.info("start processing")

                    sparkStreamContext.streamingContext.getProcessContexts.foreach(processorContext => {
                        val startTime = System.currentTimeMillis()
                        val processor = processorContext.getProcessor


                        if (firstPass) {
                            /**
                              * convert incoming Kafka messages into Records
                              * if there's no serializer we assume that we need to compute a Record from K/V
                              */
                            incomingEvents = if (
                                sparkStreamContext.streamingContext.getPropertyValue(INPUT_SERIALIZER).asString
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
                        outgoingEvents = processor.process(processorContext, incomingEvents)


                    })


                    /**
                      * Do we make records compliant with a given Avro schema ?
                      */
                    if (sparkStreamContext.streamingContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).isSet) {
                        try {
                            val strSchema = sparkStreamContext.streamingContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString()
                            val schema = RecordSchemaUtil.compileSchema(strSchema)


                            outgoingEvents = outgoingEvents.map(record => RecordSchemaUtil.convertToValidRecord(record, schema))
                        } catch {
                            case t: Throwable =>
                                logger.warn("something wrong while converting records " +
                                    "to valid accordingly to provide Avro schema " + t.getMessage)
                        }

                    }


                    logger.info("sending to kafka")

                    /**
                      * push outgoing events and errors to Kafka
                      */
                    kafkaSink.value.produce(
                        sparkStreamContext.streamingContext.getPropertyValue(OUTPUT_TOPICS).asString,
                        outgoingEvents.toList,
                        serializer
                    )

                    kafkaSink.value.produce(
                        sparkStreamContext.streamingContext.getPropertyValue(ERROR_TOPICS).asString,
                        outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERRORS)).toList,
                        errorSerializer
                    )

                    logger.info("saving offsets")

                    /**
                      * save latest offset to Zookeeper
                      */
                  //  zkSink.value.saveOffsetRangesToZookeeper(appName, offsetRange)
                    logger.info("processed " + outgoingEvents.size() + " messages")
                }
            })

            return Some(offsetRanges)
        }
        None
    }


}


