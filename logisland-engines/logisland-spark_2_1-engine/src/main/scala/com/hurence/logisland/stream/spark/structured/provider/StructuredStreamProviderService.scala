package com.hurence.logisland.stream.spark.structured.provider

import com.hurence.logisland.controller.ControllerService
import com.hurence.logisland.record.Record
import com.hurence.logisland.stream.StreamContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
        implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Record]
        val df = read(spark, streamContext)
        df.map( r =>{

            val id = streamContext.getIdentifier
            r
        })
    }


    /**
      * create a streaming DataFrame that represents data received
      *
      * @param streamContext
      * @return DataFrame currently loaded
      */
    def write(df: Dataset[Record], streamContext: StreamContext)

}
