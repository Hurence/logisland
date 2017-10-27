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

import com.hurence.logisland.component.{PropertyDescriptor, StandardPropertyValue}
import com.hurence.logisland.processor.AbstractProcessor
import com.hurence.logisland.record.{FieldDictionary, Record, RecordUtils}
import com.hurence.logisland.registry.VariableRegistry
import com.hurence.logisland.serializer.SerializerProvider
import com.hurence.logisland.util.record.RecordSchemaUtil
import com.hurence.logisland.util.spark.ProcessorMetrics
import com.hurence.logisland.validator.StandardValidators
import org.apache.avro.Schema
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.spark.TaskContext
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


object KafkaRecordStreamParallelProcessing {


    val SQL_QUERY = new PropertyDescriptor.Builder()
        .name("sql.query")
        .description("The SQL query to execute")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val MAX_RESULTS_COUNT = new PropertyDescriptor.Builder()
        .name("max.results.count")
        .description("the max number of rows to output. (-1 for no limit)")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("-1")
        .build

}

class KafkaRecordStreamParallelProcessing extends AbstractKafkaRecordStream {
    val logger = LoggerFactory.getLogger(KafkaRecordStreamParallelProcessing.getClass.getName)

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractKafkaRecordStream.ERROR_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.INPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA)
        descriptors.add(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA)
        descriptors.add(AbstractKafkaRecordStream.INPUT_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.ERROR_SERIALIZER)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_AUTOCREATE)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM)
        descriptors.add(AbstractKafkaRecordStream.KAFKA_MANUAL_OFFSET_RESET)

        descriptors.add(KafkaRecordStreamSQLAggregator.MAX_RESULTS_COUNT)
        descriptors.add(KafkaRecordStreamSQLAggregator.SQL_QUERY)
        Collections.unmodifiableList(descriptors)
    }

    /**
      * launch the chain of processing for each partition of the RDD in parallel
      *
      * @param rdd
      */
    override def process(rdd: RDD[(Array[Byte], Array[Byte])]) = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            val inputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_TOPICS).asString
            val outputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_TOPICS).asString

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
                        val pipelineMetricPrefix = streamContext.getIdentifier + "." +
                            "partition" + partitionId + "."
                        val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms" ).time()


                        /**
                          * create serializers
                          */
                        val deserializer = SerializerProvider.getSerializer(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_SERIALIZER).asString,
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA).asString)
                        val serializer = SerializerProvider.getSerializer(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_SERIALIZER).asString,
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)
                        val errorSerializer = SerializerProvider.getSerializer(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.ERROR_SERIALIZER).asString,
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)


                        /**
                          * process events by chaining output records
                          */
                        var firstPass = true
                        var incomingEvents: util.Collection[Record] = Collections.emptyList()
                        var outgoingEvents: util.Collection[Record] = Collections.emptyList()
                        val processingMetrics: util.Collection[Record] = new util.ArrayList[Record]()

                        streamContext.getProcessContexts.foreach(processorContext => {
                            val startTime = System.currentTimeMillis()
                            val processor = processorContext.getProcessor
                            val processorTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix +
                                processorContext.getName + ".processingTime").time()

                            if (firstPass) {
                                /**
                                  * convert incoming Kafka messages into Records
                                  * if there's no serializer we assume that we need to compute a Record from K/V
                                  */
                                incomingEvents = if (
                                    streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_SERIALIZER).asString
                                        == AbstractKafkaRecordStream.NO_SERIALIZER.getValue) {
                                    // parser
                                    partition.map(rawMessage => {
                                        val key = if (rawMessage._1 != null) new String(rawMessage._1) else ""
                                        val value = if (rawMessage._2 != null) new String(rawMessage._2) else ""
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
                                val controllerServiceLookup = controllerServiceLookupSink.value.getControllerServiceLookup()
                                processorContext.addControllerServiceLookup(controllerServiceLookup)
                            }
                            processor.init(processorContext)
                            outgoingEvents = processor.process(processorContext, incomingEvents)

                            /**
                              * compute metrics
                              */
                            val processorMetrics = ProcessorMetrics.computeMetrics(
                                pipelineMetricPrefix + processorContext.getName + ".",
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
                        if (streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA).isSet) {
                            try {
                                val strSchema = streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString()
                                val parser = new Schema.Parser
                                val schema = parser.parse(strSchema)

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
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_TOPICS).asString,
                            outgoingEvents.toList,
                            serializer
                        )

                        kafkaSink.value.produce(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.ERROR_TOPICS).asString,
                            outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERRORS)).toList,
                            errorSerializer
                        )


                        /**
                          * save latest offset to Zookeeper
                          */
                        zkSink.value.saveOffsetRangesToZookeeper(appName, offsetRange)
                        pipelineTimerContext.stop()
                    }
                } catch {
                    case ex: OffsetOutOfRangeException =>
                        val brokerList = streamContext.getPropertyValue(AbstractKafkaRecordStream.KAFKA_METADATA_BROKER_LIST).asString
                        val latestOffsetsString = zkSink.value.loadOffsetRangesFromZookeeper(
                            brokerList,
                            appName,
                            inputTopics.split(",").toSet)
                            .map(t => s"${t._1.topic}_${t._1.partition}:${t._2}")
                            .mkString(", ")
                        val offestsString = offsetRanges
                            .map(o => s"${o.topic}_${o.partition}:${o.fromOffset}/${o.untilOffset}")
                            .mkString(", ")

                        logger.error(s"exception : ${ex.toString}")
                        logger.error(s"unable to process partition. current Offsets $offestsString latest offsets $latestOffsetsString")
                }

            })
        }
    }
}


