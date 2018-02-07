package com.hurence.logisland.stream.spark.structured.provider

import java.io.ByteArrayInputStream

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.serializer.SerializerProvider
import com.hurence.logisland.stream.StreamContext
import com.hurence.logisland.stream.StreamProperties._
import org.apache.spark.sql.{Dataset, SparkSession}
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
trait StructuredStreamProviderService extends ControllerService {

    val logger = LoggerFactory.getLogger(this.getClass)

    /**
      * create a streaming DataFrame that represents data received
      *
      * @param spark
      * @param streamContext
      * @return DataFrame currently loaded
      */
    def read(spark: SparkSession, streamContext: StreamContext): Dataset[Record]


    /**
      *
      *
      * @param spark
      * @param streamContext
      * @return
      */
    def load(spark: SparkSession, streamContext: StreamContext): Dataset[Record] = {
        import spark.implicits._
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]
        val df = read(spark, streamContext)

        df.mapPartitions(iterator => {

            /**
              * create serializers
              */
            val serializer = SerializerProvider.getSerializer(
                streamContext.getPropertyValue(READ_TOPICS_SERIALIZER).asString,
                streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)


            val myList = iterator.toList
            // In a normal user case, we will do the
            // the initialization(ex : initializing database)
            // before iterating through each element
            myList.flatMap(r => {

                // TODO handle key also
                val incomingEvents = try {
                    val bais = new ByteArrayInputStream(r.getField(FieldDictionary.RECORD_VALUE).getRawValue.asInstanceOf[Array[Byte]])
                    val deserialized = serializer.deserialize(bais)
                    bais.close()

                    Some(deserialized)
                } catch {
                    case t: Throwable =>
                        logger.error(s"exception while deserializing events ${t.getMessage}")
                        None
                }

                incomingEvents
            }).iterator
        })


       /* df.map(r => {


            /**
              * create serializers
              */
            val serializer = SerializerProvider.getSerializer(
                streamContext.getPropertyValue(READ_TOPICS_SERIALIZER).asString,
                streamContext.getPropertyValue(AVRO_INPUT_SCHEMA).asString)
            // TODO handle key also
            val incomingEvents = try {
                val bais = new ByteArrayInputStream(r.getField(FieldDictionary.RECORD_VALUE).getRawValue.asInstanceOf[Array[Byte]])
                val deserialized = serializer.deserialize(bais)
                bais.close()

                Some(deserialized)
            } catch {
                case t: Throwable =>
                    logger.error(s"exception while deserializing events ${t.getMessage}")
                    None
            }

            incomingEvents.get
        })*/
    }


    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    def write(df: Dataset[Record], streamContext: StreamContext)

}
