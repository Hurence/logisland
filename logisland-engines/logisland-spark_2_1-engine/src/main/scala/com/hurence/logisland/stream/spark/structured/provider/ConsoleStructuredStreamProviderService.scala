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
package com.hurence.logisland.stream.spark.structured.provider

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, Record}
import com.hurence.logisland.stream.StreamContext
import org.apache.spark.sql.{Dataset, SparkSession}


class ConsoleStructuredStreamProviderService extends AbstractControllerService with StructuredStreamProviderService {


    @OnEnabled
    @throws[InitializationException]
    override def init(context: ControllerServiceInitializationContext): Unit = {
        this.synchronized {
            try {


            } catch {
                case e: Exception =>
                    throw new InitializationException(e)
            }
        }
    }

    /**
      * Allows subclasses to register which property descriptor objects are
      * supported.
      *
      * @return PropertyDescriptor objects this processor currently supports
      */
    override def getSupportedPropertyDescriptors() = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]

        Collections.unmodifiableList(descriptors)
    }

    /**
      * create a streaming DataFrame that represents data received
      *
      * @param spark
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def read(spark: SparkSession, streamContext: StreamContext) = {
        null

    }


    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    override def write(df: Dataset[Record], streamContext: StreamContext) = {

        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        //  implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]

        df
            .writeStream
            .format("console")
            .start()
    }
}
