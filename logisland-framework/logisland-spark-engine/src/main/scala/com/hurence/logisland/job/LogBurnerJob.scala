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

package com.hurence.logisland.job


import com.hurence.logisland.utils.spark.SparkUtils
import kafka.serializer.StringDecoder
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.spark.Logging
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * This jobs takes kafka topics full of logs and burn them into an hdfs folder
  *
  *
  * @see https://databricks.com/blog/2015/03/30/improvements-to-kafka-integration-of-spark-streaming.html
  *
  */
object LogBurnerJob extends Logging {

    //private val logger = Logger.getLogger(this.getClass.getName)

    /**
      *
      * @param ssc
      * @param topicList for example "apache-sd234,cisco-AE"
      * @param brokerList for example "localhost:9092,anotherhost:9092"
      */
    def burnLogTopics(ssc: StreamingContext, brokerList: String, topicList: String, hdfsOutputPath: String) = {
        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> "LogBurner")

        // Define which topics to read from
        val topics = topicList.split(",").toSet

        // Create the direct stream with the Kafka parameters and topics
        val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        // Get the lines, split them into words, count the words and print
        val logs = kafkaStream
            .map(kv => kv._2)
            .saveAsTextFiles(hdfsOutputPath, "csv")

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }

    def main(args: Array[String]) {

        /**
          * Commande lien management
          */
        val parser = new DefaultParser()
        val options = new Options()
        options.addOption("out", "output-path", true, "hdfs path")
        options.addOption("w", "time-window", true, "window time for micro batch")
        options.addOption("b", "broker-list", true, "kafka broker list :localhost:9092,anotherhost:9092")
        options.addOption("in", "input-topics", true, "kafka topic list log-island1,log-island2")
        options.addOption("h", "help", false, "print usage")

        val line = parser.parse(options, args)
        val output = line.getOptionValue("o", "debug")
        val windowTime = line.getOptionValue("w", "10").toLong
        val brokerList = line.getOptionValue("b", "sandbox:9092")
        val topicList = line.getOptionValue("in", "log-island")
        val hdfsPath = line.getOptionValue("out", "/user/log-island/data/raw/logs")

        /**
          * Start spark context
          */
        val sc = SparkUtils.initContext(this.getClass.getName)
        val ssc = new StreamingContext(sc, Seconds(windowTime))

        logDebug("start burning log to hdfs from Kafka")

        /**
          * Start spark context
          */
        burnLogTopics(ssc, brokerList, topicList, hdfsPath)

    }
}
