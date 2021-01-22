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

import com.hurence.logisland.annotation.documentation.{CapabilityDescription, Tags}
import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.util.spark.SparkUtils
import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.LoggerFactory


@Tags(Array("stream", "SQL", "query", "record"))
@CapabilityDescription("This is a stream capable of SQL query interpretations.")
class KafkaRecordStreamSQLAggregator extends AbstractKafkaRecordStream {

    private val logger = LoggerFactory.getLogger(classOf[KafkaRecordStreamSQLAggregator])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.addAll(super.getSupportedPropertyDescriptors())
        descriptors.add(MAX_RESULTS_COUNT)
        descriptors.add(SQL_QUERY)
        descriptors.add(OUTPUT_RECORD_TYPE)
        Collections.unmodifiableList(descriptors)
    }


    override def process(rdd: RDD[ConsumerRecord[Array[Byte], Array[Byte]]]): Option[Array[OffsetRange]] = {
        if (!rdd.isEmpty()) {
            // Cast the rdd to an interface that lets us get an array of OffsetRange
            // val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            val sqlContext = SparkSession
                .builder()
                .appName(appName)
                .config(ssc.sparkContext.getConf)
                .getOrCreate()

            // this is used to implicitly convert an RDD to a DataFrame.
            @transient lazy val deserializer = getSerializer(
                streamContext.getPropertyValue(INPUT_SERIALIZER).asString,
                streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)

            val inputTopics = streamContext.getPropertyValue(INPUT_TOPICS).asString

            //here how to handle elements that are not successfully deserialized ???
            //currently we lose them !
            //I think we should create an ErrorRecord containing key, value.
            val records: RDD[Record] = rdd.mapPartitions(p => deserializeRecords(p, deserializer).iterator)

            /**
              * get a Dataframe schema (either from an Avro schema or from the first record)
              */
            val schema = try {
                val parser = new Schema.Parser
                val schema = parser.parse(streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)
                SparkUtils.convertAvroSchemaToDataframeSchema(schema)
            }
            catch {
                case e: Exception =>
                    logger.error("unable to add schema :{}", e.getMessage)
                    SparkUtils.convertFieldsNameToSchema(records.take(1)(0))
            }

            if (!records.isEmpty()) {

                val rows = records.filter(r => !r.hasField(FieldDictionary.RECORD_ERRORS))
                    .map(r => SparkUtils.convertToRow(r, schema))


                sqlContext.createDataFrame(rows, schema).createOrReplaceTempView(inputTopics)




                val query = streamContext.getPropertyValue(SQL_QUERY).asString()
                val outputRecordType = streamContext.getPropertyValue(OUTPUT_RECORD_TYPE).asString()

                sqlContext.sql(query).rdd
                    .foreachPartition(rows => {
                        val outgoingEvents = rows.map(row => SparkUtils.convertToRecord(row, outputRecordType)).toList
                        /**
                          * create serializers
                          */
                        val serializer = getSerializer(
                            streamContext.getPropertyValue(OUTPUT_SERIALIZER).asString,
                            streamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)
                        val errorSerializer = getSerializer(
                            streamContext.getPropertyValue(ERROR_SERIALIZER).asString,
                            streamContext.getPropertyValue(AVRO_OUTPUT_SCHEMA).asString)




                        /**
                          * push outgoing events and errors to Kafka
                          */
                        kafkaSink.value.produce(
                            streamContext.getPropertyValue(OUTPUT_TOPICS).asString,
                            outgoingEvents,
                            serializer
                        )

                        kafkaSink.value.produce(
                            streamContext.getPropertyValue(ERROR_TOPICS).asString,
                            outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERRORS)),
                            errorSerializer
                        )

                    })


            }
            return None //Some(offsetRanges)
        }
        None
    }

}


