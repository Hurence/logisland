package com.hurence.logisland.stream.spark

import java.util
import java.util.Collections

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.processor.chain.{KafkaRecordStream, StandardProcessorChainInstance}
import com.hurence.logisland.record.FieldDictionary
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.util.validator.StandardValidators
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.slf4j.LoggerFactory


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


object KafkaStreamSQLAggregator {


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

class KafkaStreamSQLAggregator(override val appName: String,
                               override val ssc: StreamingContext,
                               override val processorChainInstance: StandardProcessorChainInstance)
    extends AbstractKafkaStream(appName, ssc, processorChainInstance) {

    private val logger = LoggerFactory.getLogger(classOf[KafkaStreamSQLAggregator])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractKafkaStream.ERROR_TOPICS)
        descriptors.add(AbstractKafkaStream.INPUT_TOPICS)
        descriptors.add(AbstractKafkaStream.OUTPUT_TOPICS)
        descriptors.add(AbstractKafkaStream.METRICS_TOPIC)
        descriptors.add(AbstractKafkaStream.AVRO_INPUT_SCHEMA)
        descriptors.add(AbstractKafkaStream.AVRO_OUTPUT_SCHEMA)
        descriptors.add(AbstractKafkaStream.INPUT_SERIALIZER)
        descriptors.add(AbstractKafkaStream.OUTPUT_SERIALIZER)
        descriptors.add(AbstractKafkaStream.ERROR_SERIALIZER)
        descriptors.add(AbstractKafkaStream.KAFKA_TOPIC_AUTOCREATE)
        descriptors.add(AbstractKafkaStream.KAFKA_TOPIC_DEFAULT_PARTITIONS)
        descriptors.add(AbstractKafkaStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
        descriptors.add(AbstractKafkaStream.KAFKA_METADATA_BROKER_LIST)
        descriptors.add(AbstractKafkaStream.KAFKA_ZOOKEEPER_QUORUM)
        descriptors.add(AbstractKafkaStream.KAFKA_MANUAL_OFFSET_RESET)

        descriptors.add(KafkaStreamSQLAggregator.MAX_RESULTS_COUNT)
        descriptors.add(KafkaStreamSQLAggregator.SQL_QUERY)
        Collections.unmodifiableList(descriptors)
    }

    override def process(rdd: RDD[(Array[Byte], Array[Byte])]) = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            // Get the singleton instance of SQLContext
            val sqlContext = new org.apache.spark.sql.SQLContext(rdd.sparkContext)
            // this is used to implicitly convert an RDD to a DataFrame.

            val deserializer = getSerializer(
                processorChainContext.getProperty(KafkaRecordStream.INPUT_SERIALIZER).asString,
                processorChainContext.getProperty(KafkaRecordStream.AVRO_INPUT_SCHEMA).asString)


            val records = rdd.mapPartitions(p => deserializeEvents(p, deserializer).iterator)


            if (!records.isEmpty()) {
                val rows = records.map(r => SparkUtils.convertToRow(r))
                val schema = SparkUtils.convertFieldsNameToSchema(records.take(1)(0))

                sqlContext.createDataFrame(rows, schema).registerTempTable("records")


                val query = processorChainContext.getProperty(KafkaStreamSQLAggregator.SQL_QUERY).asString()
                val maxResultsCount = processorChainContext.getProperty(KafkaStreamSQLAggregator.MAX_RESULTS_COUNT).asInteger()

                sqlContext.sql(query).rdd
                    .foreachPartition(rows => {
                        val outgoingEvents = rows.map(row => SparkUtils.convertToRecord(row))
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

                     /*   kafkaSink.value.produce(
                            processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).asString,
                            processingMetrics.toList,
                            serializer
                        )*/
                    })




            }
        }
    }
}


