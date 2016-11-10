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
import java.util
import java.util.Date

import com.hurence.logisland.record._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
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

    /**
      * convert a Record to a SQL Row
      *
      * @param record the Record to convert
      * @return the Spar SQL row
      */
    def convertToRow(record: Record, schema: StructType): Row = {

        Row.fromSeq(schema.map(structField => {
            val fieldName = structField.name

            if (record.hasField(fieldName)) {
                structField.dataType match {
                    case DataTypes.StringType =>
                        if (record.getField(fieldName).getType == FieldType.ARRAY)
                            record.getField(fieldName).getRawValue.asInstanceOf[util.ArrayList[String]].toArray.mkString
                        else
                            record.getField(fieldName).asString()
                    case DataTypes.IntegerType => record.getField(fieldName).asInteger()
                    case DataTypes.LongType => record.getField(fieldName).asLong()
                    case DataTypes.FloatType => record.getField(fieldName).asFloat()
                    case DataTypes.DoubleType => record.getField(fieldName).asDouble()
                    case _ => record.getField(fieldName).asString()
                }
            } else {
                null
            }


        }))
    }

    /**
      * convert a SQL Row to a Record to
      *
      * @param row the Row to convert
      * @return the Record
      */
    def convertToRecord(row: Row, inRecordType: String = "logisland_record"): Record = {

        var recordType = inRecordType
        var recordTime = new Date().getTime
        val fields = row.schema.map(structField => {
            val fieldName = structField.name

            structField.dataType match {
                case DataTypes.StringType =>
                    if (fieldName == FieldDictionary.RECORD_TYPE) {
                        recordType = row.getAs[String](fieldName)
                    }
                    new Field(fieldName, FieldType.STRING, row.getAs[String](fieldName))
                case DataTypes.IntegerType => new Field(fieldName, FieldType.INT, row.getAs[Int](fieldName))
                case DataTypes.LongType =>
                    if (fieldName == FieldDictionary.RECORD_TIME) {
                        recordTime = row.getAs[Long](fieldName)
                    }
                    new Field(fieldName, FieldType.LONG, row.getAs[Long](fieldName))
                case DataTypes.FloatType => new Field(fieldName, FieldType.FLOAT, row.getAs[Float](fieldName))
                case DataTypes.DoubleType => new Field(fieldName, FieldType.DOUBLE, row.getAs[Double](fieldName))
                case _ => new Field(fieldName, FieldType.STRING, row.getAs[String](fieldName))
            }

        })

        // construct new Record with type and time from the row
        val outputRecord = new StandardRecord()
            .setType(recordType)
            .setTime(new Date(recordTime))
        fields.foreach(field => outputRecord.setField(field))
        outputRecord
    }

    /**
      * create a dataframe schema from a Record
      *
      * @param record the Record to infer schema
      * @return th schema
      */
    def convertFieldsNameToSchema(record: Record): StructType = {
        StructType(
            record.getAllFieldsSorted.toArray(Array[Field]()).map(f => {
                f.getType match {
                    case FieldType.INT => StructField(f.getName, DataTypes.IntegerType, nullable = true)
                    case FieldType.LONG => StructField(f.getName, DataTypes.LongType, nullable = true)
                    case FieldType.FLOAT => StructField(f.getName, DataTypes.FloatType, nullable = true)
                    case FieldType.DOUBLE => StructField(f.getName, DataTypes.DoubleType, nullable = true)
                    case FieldType.STRING => StructField(f.getName, DataTypes.StringType, nullable = true)
                    case _ => StructField(f.getName, DataTypes.StringType, nullable = true)
                }
            })
        )
    }

    /**
      * create a dataframe schema from an Avro one
      *
      * @param avroSchema the Avro Schema
      * @return th schema
      */
    def convertAvroSchemaToDataframeSchema(avroSchema: Schema): StructType = {
        val types = avroSchema.getFields.toArray(Array[Schema.Field]())
            .map(s => {
                (s.name(),
                    s.schema()
                        .getTypes
                        .toArray(Array[Schema]())
                        .filter(t => t.getType != Type.NULL)
                        .toList
                        .head)
            })

        StructType(types.map(f => {
            f._2.getType match {
                case Type.INT => StructField(f._1, DataTypes.IntegerType, nullable = true)
                case Type.LONG => StructField(f._1, DataTypes.LongType, nullable = true)
                case Type.FLOAT => StructField(f._1, DataTypes.FloatType, nullable = true)
                case Type.DOUBLE => StructField(f._1, DataTypes.DoubleType, nullable = true)
                case Type.STRING => StructField(f._1, DataTypes.StringType, nullable = true)
                case _ => StructField(f._1, DataTypes.StringType, nullable = true)
            }
        })
        )
    }
}
