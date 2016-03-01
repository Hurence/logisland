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

package com.hurence.logisland.integration

import java.text.SimpleDateFormat

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by tom on 11/06/15.
 */
object SparkUtils {

  def initContext(appName: String, blockInterval : String = null, maxRatePerPartition : String = null): SparkContext = {
    // job configuration
    val conf = new SparkConf().setAppName(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (maxRatePerPartition != null) {
      conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
    }
    if (blockInterval != null) {
      conf.set("spark.streaming.blockInterval", blockInterval)
    }
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.unpersist", "false")
    //conf.set("spark.executor.memory", "4g")
    //conf.set("spark.driver.memory", "4g")
    //conf.set("spark.cleaner.ttl", "900")
    conf.set("spark.ui.port", "4050")

    val sc = new SparkContext(conf)

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
