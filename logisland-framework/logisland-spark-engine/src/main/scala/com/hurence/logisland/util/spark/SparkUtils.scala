/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
/*
 Copyright 2016 Hurence

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.hurence.logisland.util.spark

import java.text.SimpleDateFormat

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tom on 11/06/15.
 */

object SparkUtils extends LazyLogging {


  def customizeLogLevels: Unit = {
    // Logging verbosity lowered
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.scheduler").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)


    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop.ipc.Client").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
    Logger.getLogger("org.elasticsearch").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

    Logger.getLogger("org.apache.hadoop.ipc.ProtobufRpcEngine").setLevel(Level.WARN)
    Logger.getLogger("parquet.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.hurence").setLevel(Level.DEBUG)
  }

  def initContext(appName: String,
                  blockInterval: String = "",
                  maxRatePerPartition: String = "",
                  master: String = ""): SparkContext = {

    // job configuration
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (maxRatePerPartition.nonEmpty) {
      conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
    }
    if (blockInterval.nonEmpty) {
      conf.set("spark.streaming.blockInterval", blockInterval)
    }
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.unpersist", "false")
    conf.set("spark.ui.port", "4050")
    conf.setAppName(appName)

    if (master.nonEmpty) {
      conf.setMaster(master)
    }

    val sc = new SparkContext(conf)

    logger.info(s"spark context initialized with master:$master, appName:$appName, " +
        s"blockInterval:$blockInterval, maxRatePerPartition:$maxRatePerPartition")

    sc
  }



  /**
   * Get a file and a schema and convert this to a dataframe
   *
   * @param schema
   * @param filePath
   * @param tableName
   */
  def registerDataFrame(
                         schema: String,
                         filePath: String,
                         tableName: String,
                         sc: SparkContext,
                         sqlContext: SQLContext,
                         separator: String = "\u0001"): DataFrame = {
    // Generate the schema based on the string of schema
    val parsedSchema = StructType(schema.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val schemaLength = schema.split(" ").length
    val rawRDD = sc.textFile(filePath)
      .map(_.split(separator))
      .filter(_.length == schemaLength)
      .map(tokens => Row.fromSeq(tokens))

    // Apply the schema to the RDD.
    val dataFrame = sqlContext.createDataFrame(rawRDD, parsedSchema)

    // Register the DataFrames as a table.
    dataFrame.registerTempTable(tableName)
    dataFrame
  }


  def registerUdfs(sqlContext: SQLContext) = {


    sqlContext.udf.register("timestamp", (date: String) => {
      try {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

        sdf.parse(date).getTime
      } catch {
        case e: Exception => 0
      }
    })

  }
}
