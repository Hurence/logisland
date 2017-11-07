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

import java.text.SimpleDateFormat
import java.util
import java.util.Collections

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.record.{FieldDictionary, FieldType}
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.validator.StandardValidators
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory


object KafkaRecordStreamHDFSBurner {

    val FILE_FORMAT_PARQUET = "parquet"
    val FILE_FORMAT_ORC = "orc"
    val FILE_FORMAT_JSON = "json"
    val FILE_FORMAT_TXT = "txt"

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
        .allowableValues(FILE_FORMAT_PARQUET, FILE_FORMAT_TXT, FILE_FORMAT_JSON, FILE_FORMAT_JSON)
        .build

    val RECORD_TYPE = new PropertyDescriptor.Builder()
        .name("record.type")
        .description("the type of event to filter")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val NUM_PARTITIONS = new PropertyDescriptor.Builder()
        .name("num.partitions")
        .description("the numbers of physical files on HDFS")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("4")
        .build

    val EXCLUDE_ERRORS = new PropertyDescriptor.Builder()
        .name("exclude.errors")
        .description("do we include records with errors ?")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build
}

class KafkaRecordStreamHDFSBurner extends AbstractKafkaRecordStream {


    private val logger = LoggerFactory.getLogger(classOf[KafkaRecordStreamHDFSBurner])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

        descriptors.addAll(super.getSupportedPropertyDescriptors())

        descriptors.add(KafkaRecordStreamHDFSBurner.OUTPUT_FOLDER_PATH)
        descriptors.add(KafkaRecordStreamHDFSBurner.OUTPUT_FORMAT)
        descriptors.add(KafkaRecordStreamHDFSBurner.RECORD_TYPE)
        descriptors.add(KafkaRecordStreamHDFSBurner.NUM_PARTITIONS)
        descriptors.add(KafkaRecordStreamHDFSBurner.EXCLUDE_ERRORS)
        Collections.unmodifiableList(descriptors)
    }

    override def process(rdd: RDD[ConsumerRecord[Array[Byte], Array[Byte]]]): Option[Array[OffsetRange]] = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            // Get the singleton instance of SQLContext
            val sqlContext = SparkSession
                .builder()
                .appName(appName)
                .config(ssc.sparkContext.getConf)
                .getOrCreate()


            // this is used to implicitly convert an RDD to a DataFrame.

            val deserializer = getSerializer(
                streamContext.getPropertyValue(AbstractKafkaRecordStream.INPUT_SERIALIZER).asString,
                streamContext.getPropertyValue(AbstractKafkaRecordStream.AVRO_INPUT_SCHEMA).asString)


            val records = rdd.mapPartitions(p => deserializeRecords(p, deserializer).iterator)


            if (!records.isEmpty()) {


                val sdf = new SimpleDateFormat("yyyy-MM-dd")


                val numPartitions = streamContext.getPropertyValue(KafkaRecordStreamHDFSBurner.NUM_PARTITIONS).asInteger()
                val outputFormat = streamContext.getPropertyValue(KafkaRecordStreamHDFSBurner.OUTPUT_FORMAT).asString()
                val doExcludeErrors = streamContext.getPropertyValue(KafkaRecordStreamHDFSBurner.EXCLUDE_ERRORS).asBoolean()
                val recordType = streamContext.getPropertyValue(KafkaRecordStreamHDFSBurner.RECORD_TYPE).asString()
                val outPath = streamContext.getPropertyValue(KafkaRecordStreamHDFSBurner.OUTPUT_FOLDER_PATH).asString()

                val records = rdd.mapPartitions(p => deserializeRecords(p, deserializer).iterator)
                    .filter(r =>
                        r.hasField(FieldDictionary.RECORD_TYPE) &&
                            r.getField(FieldDictionary.RECORD_TYPE).asString() == recordType)
                    .map(r => {
                        try {
                            if (r.hasField(FieldDictionary.RECORD_DAYTIME))
                                r
                            else
                                r.setField(FieldDictionary.RECORD_DAYTIME, FieldType.STRING, sdf.format(r.getTime))
                        }
                        catch {
                            case ex: Throwable => r
                        }
                    })



                if (!records.isEmpty()) {
                    val schema = SparkUtils.convertFieldsNameToSchema(records.take(1)(0))
                    val rows = if (doExcludeErrors) {
                        records
                            .filter(r => !r.hasField(FieldDictionary.RECORD_ERRORS))
                            .map(r => SparkUtils.convertToRow(r, schema))
                    } else {
                        records.map(r => SparkUtils.convertToRow(r, schema))
                    }


                    logger.info(schema.toString())
                    val df = sqlContext.createDataFrame(rows, schema)


                    outputFormat match {
                        case KafkaRecordStreamHDFSBurner.FILE_FORMAT_PARQUET =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .parquet(outPath)
                        case KafkaRecordStreamHDFSBurner.FILE_FORMAT_JSON =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .json(outPath)
                        case KafkaRecordStreamHDFSBurner.FILE_FORMAT_ORC =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .orc(outPath)
                        case KafkaRecordStreamHDFSBurner.FILE_FORMAT_TXT =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .text(outPath)
                        case _ =>
                            throw new IllegalArgumentException(s"$outputFormat not supported yet")
                    }

                    /**
                      * save latest offset to Zookeeper
                      */
                //    offsetRanges.foreach(offsetRange => zkSink.value.saveOffsetRangesToZookeeper(appName, offsetRange))
                }

            }

            return Some(offsetRanges)
        }
        None
    }
}


