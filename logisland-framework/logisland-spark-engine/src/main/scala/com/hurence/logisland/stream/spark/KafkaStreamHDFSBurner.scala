package com.hurence.logisland.stream.spark

import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date}

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.processor.chain.{KafkaRecordStream, StandardProcessorChainInstance}
import com.hurence.logisland.record.FieldDictionary
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.util.validator.StandardValidators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
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


object KafkaStreamHDFSBurner {


    val OUTPUT_FOLDER_PATH = new PropertyDescriptor.Builder()
        .name("output.folder.path")
        .description("the location where to put files : file:///tmp/out")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build


    val OUTPUT_FORMAT = new PropertyDescriptor.Builder()
        .name("output.format")
        .description("can be parquet, orc csv")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .allowableValues("parquet", "orc", "txt", "json")
        .build

    val RECORD_TYPE = new PropertyDescriptor.Builder()
        .name("record.type")
        .description("the type of event to filter")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

}

class KafkaStreamHDFSBurner(override val appName: String,
                            override val ssc: StreamingContext,
                            override val processorChainInstance: StandardProcessorChainInstance)
    extends AbstractKafkaStream(appName, ssc, processorChainInstance) {


    private val logger = LoggerFactory.getLogger(classOf[KafkaStreamHDFSBurner])


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

        descriptors.add(KafkaStreamHDFSBurner.OUTPUT_FOLDER_PATH)
        descriptors.add(KafkaStreamHDFSBurner.OUTPUT_FORMAT)
        descriptors.add(KafkaStreamHDFSBurner.RECORD_TYPE)
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


                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val today = sdf.format(new Date())

                val recordType = processorChainContext.getProperty(KafkaStreamHDFSBurner.RECORD_TYPE).asString()
                val outPath = processorChainContext.getProperty(KafkaStreamHDFSBurner.OUTPUT_FOLDER_PATH).asString() +
                    s"/day=$today" +
                    s"/record_type=$recordType"


                val records = rdd.mapPartitions(p => deserializeEvents(p, deserializer).iterator)
                    .filter(r =>
                        r.hasField(FieldDictionary.RECORD_TYPE) &&
                            r.getField(FieldDictionary.RECORD_TYPE).asString() == recordType
                    )

                if (!records.isEmpty()) {
                    val rows = records.map(r => SparkUtils.convertToRow(r))
                    val schema = SparkUtils.convertFieldsNameToSchema(records.take(1)(0))

                    sqlContext.createDataFrame(rows, schema)
                        .write
                        //      .partitionBy(FieldDictionary.RECORD_TYPE)
                        .mode(SaveMode.Append)
                        // TODO choose output format
                        .parquet(outPath)
                }

            }
        }
    }
}


