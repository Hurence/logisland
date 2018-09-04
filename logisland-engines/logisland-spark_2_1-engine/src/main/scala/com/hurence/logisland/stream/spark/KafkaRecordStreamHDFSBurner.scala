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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory
import com.hurence.logisland.stream.spark._
import com.hurence.logisland.stream.StreamProperties._


class KafkaRecordStreamHDFSBurner extends AbstractKafkaRecordStream {


    private val logger = LoggerFactory.getLogger(classOf[KafkaRecordStreamHDFSBurner])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

        descriptors.addAll(super.getSupportedPropertyDescriptors())

        descriptors.add(OUTPUT_FOLDER_PATH)
        descriptors.add(OUTPUT_FORMAT)
        descriptors.add(RECORD_TYPE)
        descriptors.add(NUM_PARTITIONS)
        descriptors.add(EXCLUDE_ERRORS)
        descriptors.add(DATE_FORMAT)
        descriptors.add(INPUT_FORMAT)
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
                streamContext.getPropertyValue(INPUT_SERIALIZER).asString,
                streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)


            val records = rdd.mapPartitions(p => deserializeRecords(p, deserializer).iterator)


            if (!records.isEmpty()) {


                val sdf = new SimpleDateFormat(streamContext.getPropertyValue(DATE_FORMAT).asString)


                val numPartitions = streamContext.getPropertyValue(NUM_PARTITIONS).asInteger()
                val outputFormat = streamContext.getPropertyValue(OUTPUT_FORMAT).asString()
                val doExcludeErrors = streamContext.getPropertyValue(EXCLUDE_ERRORS).asBoolean()
                val recordType = streamContext.getPropertyValue(RECORD_TYPE).asString()
                val outPath = streamContext.getPropertyValue(OUTPUT_FOLDER_PATH).asString()

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
                    var df: DataFrame = null;
                    val inputFormat = streamContext.getPropertyValue(INPUT_FORMAT).asString()
                    if (inputFormat.isEmpty) {

                        val schema = SparkUtils.convertFieldsNameToSchema(records.take(1)(0))
                        val rows = if (doExcludeErrors) {
                            records
                                .filter(r => !r.hasField(FieldDictionary.RECORD_ERRORS))
                                .map(r => SparkUtils.convertToRow(r, schema))
                        } else {
                            records.map(r => SparkUtils.convertToRow(r, schema))
                        }


                        logger.info(schema.toString())
                        df = sqlContext.createDataFrame(rows, schema)
                    } else {
                        if ("json".equals(inputFormat)) {
                            df = sqlContext.read.json(records.map(record=> record.getField(FieldDictionary.RECORD_VALUE).asString()))
                        } else {
                            throw new IllegalArgumentException(s"Input format $inputFormat is not supported")
                        }
                    }

                    outputFormat match {
                        case FILE_FORMAT_PARQUET =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .parquet(outPath)
                        case FILE_FORMAT_JSON =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .json(outPath)
                        case FILE_FORMAT_ORC =>
                            df.repartition(numPartitions)
                                .write
                                .partitionBy(FieldDictionary.RECORD_DAYTIME, FieldDictionary.RECORD_TYPE)
                                .mode(SaveMode.Append)
                                .orc(outPath)
                        case FILE_FORMAT_TXT =>
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


