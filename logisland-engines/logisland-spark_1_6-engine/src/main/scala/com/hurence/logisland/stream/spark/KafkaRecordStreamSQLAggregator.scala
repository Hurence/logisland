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
package com.hurence.logisland.stream.spark

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.{CapabilityDescription, Tags}
import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.serializer.SerializerProvider
import com.hurence.logisland.util.processor.ProcessorMetrics
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.validator.StandardValidators
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges
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


object KafkaRecordStreamSQLAggregator {


    val SQL_QUERY = new PropertyDescriptor.Builder()
        .name("sql.query")
        .description("The SQL query to execute, " +
            "please note that the table name must exists in input topics names")
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

    val OUTPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
        .name("output.record.type")
        .description("the output type of the record")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("aggregation")
        .build

}

@Tags(Array("stream", "SQL", "query", "record"))
@CapabilityDescription("This is a stream capable of SQL query interpretations")
class KafkaRecordStreamSQLAggregator extends AbstractKafkaRecordStream {

    private val logger = LoggerFactory.getLogger(classOf[KafkaRecordStreamSQLAggregator])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractKafkaRecordStream.ERROR_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.INPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.OUTPUT_TOPICS)
        descriptors.add(AbstractKafkaRecordStream.METRICS_TOPIC)
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
        descriptors.add(KafkaRecordStreamSQLAggregator.OUTPUT_RECORD_TYPE)
        Collections.unmodifiableList(descriptors)
    }

    override def process(rdd: RDD[(Array[Byte], Array[Byte])]) = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            // Get the singleton instance of SQLContext
            val sqlContext = new org.apache.spark.sql.SQLContext(rdd.sparkContext)
            // this is used to implicitly convert an RDD to a DataFrame.

            val deserializer = SerializerProvider.getSerializer(
                streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_SERIALIZER).asString,
                streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA).asString)



            val inputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_TOPICS).asString
            val outputTopics = streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_TOPICS).asString


            val records = rdd.mapPartitions(p => deserializeRecords(p, deserializer).iterator)

            /**
              * get a Dataframe schema (either from an Avro schema or from the first record)
              */
            val schema = try {
                val parser = new Schema.Parser
                val schema = parser.parse(streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA).asString)
                SparkUtils.convertAvroSchemaToDataframeSchema(schema)
            }
            catch {
                case e: Exception => {
                    logger.error("unable to add schema :{}", e.getMessage)
                    SparkUtils.convertFieldsNameToSchema(records.take(1)(0))
                }
            }

            if (!records.isEmpty()) {

                val startTime = System.currentTimeMillis()
                val rows = records.filter(r => !r.hasField(FieldDictionary.RECORD_ERRORS))
                    .map(r => SparkUtils.convertToRow(r, schema))


                sqlContext.createDataFrame(rows, schema).registerTempTable(inputTopics)




                val query = streamContext.getPropertyValue(KafkaRecordStreamSQLAggregator.SQL_QUERY).asString()
                val maxResultsCount = streamContext.getPropertyValue(KafkaRecordStreamSQLAggregator.MAX_RESULTS_COUNT).asInteger()
                val outputRecordType = streamContext.getPropertyValue(KafkaRecordStreamSQLAggregator.OUTPUT_RECORD_TYPE).asString()

                sqlContext.sql(query).rdd
                    .foreachPartition(rows => {
                        val outgoingEvents = rows.map(row => SparkUtils.convertToRecord(row, outputRecordType)).toList
                        /**
                          * create serializers
                          */
                        val serializer = SerializerProvider.getSerializer(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_SERIALIZER).asString,
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)
                        val errorSerializer = SerializerProvider.getSerializer(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.ERROR_SERIALIZER).asString,
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)



                        /**
                          * send metrics if requested
                          */
                        val processingMetrics: util.Collection[Record] = new util.ArrayList[Record]()
                        processingMetrics.addAll(ProcessorMetrics.computeMetrics(
                            appName,
                            streamContext.getName,
                            inputTopics,
                            outputTopics,
                            -1,
                            outgoingEvents,
                            outgoingEvents,
                            0,
                            0,
                            System.currentTimeMillis() - startTime))

                        /**
                          * push outgoing events and errors to Kafka
                          */
                        kafkaSink.value.produce(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.OUTPUT_TOPICS).asString,
                            outgoingEvents,
                            serializer
                        )

                        kafkaSink.value.produce(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.ERROR_TOPICS).asString,
                            outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERRORS)).toList,
                            errorSerializer
                        )

                        kafkaSink.value.produce(
                            streamContext.getPropertyValue(AbstractKafkaRecordStream.METRICS_TOPIC).asString,
                            processingMetrics.toList,
                            serializer
                        )
                    })


            }
        }
    }
}


