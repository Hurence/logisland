package com.hurence.logisland.engine.spark

import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.processor.chain.KafkaRecordStream
import com.hurence.logisland.record.{FieldDictionary, RecordUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.HasOffsetRanges

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
class StandardSparkStreamProcessingEngine extends AbstractSparkStreamProcessingEngine {

    override def process(rdd: RDD[(Array[Byte], Array[Byte])], processContext: StandardProcessContext): Unit = {
        // Cast the rdd to an interface that lets us get an array of OffsetRange
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        // Get the singleton instance of SQLContext
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._


        val deserializer = getSerializer(
            processContext.getProperty(KafkaRecordStream.INPUT_SERIALIZER).asString,
            processContext.getProperty(KafkaRecordStream.AVRO_INPUT_SCHEMA).asString)


        // Convert RDD[String] to DataFrame
        val columns = List[String](
            FieldDictionary.RECORD_TYPE,
            FieldDictionary.RECORD_TIME,
            FieldDictionary.RECORD_ID,
            FieldDictionary.RECORD_KEY,
            FieldDictionary.RECORD_VALUE)

        val recordsDF = rdd.mapPartitions(p => {
            p.map(rawMessage => {
                val key = if (rawMessage._1 != null) new String(rawMessage._1) else ""
                val value = if (rawMessage._2 != null) new String(rawMessage._2) else ""
                RecordUtils.getKeyValueRecord(key, value)
            })
        }).map(record => (
            record.getField(FieldDictionary.RECORD_TYPE).asString(),
            record.getField(FieldDictionary.RECORD_TIME).asLong(),
            record.getField(FieldDictionary.RECORD_ID).asString(),
            record.getField(FieldDictionary.RECORD_KEY).asString(),
            record.getField(FieldDictionary.RECORD_VALUE).asString()))
            .toDF(columns: _*)

        if (recordsDF.count() != 0) {
            recordsDF.repartition(1).write
                .partitionBy(FieldDictionary.RECORD_TYPE)
                .mode(SaveMode.Append)
                .parquet("outPath")
        }


        /*wordsDataFrame.write
          //  .partitionBy("date")
            .mode(SaveMode.Append)
            .parquet("outPath")*/
    }
}
