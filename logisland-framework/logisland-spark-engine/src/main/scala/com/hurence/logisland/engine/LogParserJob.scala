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

package com.hurence.logisland.engine

import com.hurence.logisland.log.LogParser
import com.hurence.logisland.utils.kafka.KafkaEventProducer
import com.hurence.logisland.utils.spark.SparkUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.admin.AdminUtils
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.commons.cli
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * This is a Spark Streaming job that reads all the topics given
  * parse the log convert them to Event and publish them to kafka
  */
object LogParserJob extends LazyLogging {

    def main(args: Array[String]) {

        //////////////////////////////////////////
        // Command line management
        val parser = new DefaultParser()
        val options = new Options()
        options.addOption(new cli.Option("b", "kafka-brokers", true, "kafka broker list :localhost:9092, anotherhost:9092") {
            setRequired(true)
        })
        options.addOption(new cli.Option("out", "output-topics", true, "the kafka output topic list") {
            setRequired(true)
        })
        options.addOption(new cli.Option("in", "input-topics", true, "the kafka input topic list") {
            setRequired(true)
        })
        options.addOption(new cli.Option("parser", "log-parser", true, "the full class name of the logparser") {
            setRequired(true)
        })
        options.addOption("bd", "batch-duration", true, "window time (in milliseconds) for micro batch (default: 2000)")
        options.addOption("bi", "block-interval", true, "window time (in milliseconds) for determining the number of partitions per batch (default: 350)")
        options.addOption("mrp", "max-rate-per-partition", true, "maximum rate (in messages per second) at which each Kafka partition will be read (default: unlimited)")
        options.addOption("h", "help", false, "print usage")
        options.addOption("a1", "first-argument", true, "first argument for constructing the parser")
        options.addOption("a2", "second-argument", true, "second argument for constructing the parser")
        options.addOption("zk", "zk-quorum", true, "quorum zookeper  ex: zkServer1:2181, zkServ2:2181")
        //kafka options for creating a new topic (in case)
        options.addOption("kpart", "kafka-partitions", true, "number of partition for the topics")
        options.addOption("krepl", "kafka-replication", true, "number of replication for the topics")
        //spark option
        options.addOption("master", "spark-master", true, "master URI")

        options.addOption("name", "app-name", true, "spark application name")

        // parse the command line arguments
        val line = parser.parse(options, args)
        val inputTopicList = line.getOptionValue("in", "log-island-cisco")
        val outputTopicList = line.getOptionValue("out", "log-island-events")
        val batchDuration = line.getOptionValue("bd", "2000").toLong
        val blockInterval = line.getOptionValue("bi", "350")
        val maxRatePerPartition = line.getOptionValue("mrp", null)
        val brokerList = line.getOptionValue("b", "sandbox:9092")

        val logParserClass = line.getOptionValue("parser", "com.hurence.logisland.plugin.cisco.CiscoLogParser")
        val firstParserArgument = line.getOptionValue("a1", null)
        val secondParserArgument = line.getOptionValue("a2", null)
        val zkQuorum = line.getOptionValue("zk", "sandbox:2181")
        //kafka topics option
        val kpart = line.getOptionValue("kpart", "1").toInt
        val krepl = line.getOptionValue("krepl", "1").toInt
        //spark option
        val sm = line.getOptionValue("master", "")
        val appName = line.getOptionValue("name", this.getClass.getName)
        // Define topics sets
        val inputTopics: Set[String] = inputTopicList.split(",").toSet
        val outputTopic: Set[String] = outputTopicList.split(",").toSet
        val allTopic: Set[String] = inputTopics ++ outputTopic

        // set up context
        val sc = SparkUtils.initContext(appName, blockInterval, maxRatePerPartition, sm)
        val ssc = new StreamingContext(sc, Milliseconds(batchDuration))

        val zkUtils: ZkUtils = ZkUtils.apply(zkQuorum, 30000, 30000, JaasUtils.isZkSecurityEnabled)



        // create kafka topics if needed
        createTopics(allTopic, zkUtils, kpart, krepl)


        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList,
            "group.id" -> "LogParserJob")

        logger.debug("======== Sparkstreaming Arguments ========")
        logger.debug("brokerList: " + brokerList)
        logger.debug("inputTopicList: " + inputTopicList)
        logger.debug("outputTopicList: " + outputTopicList)
        logger.debug("batchDuration: " + batchDuration)
        logger.debug("blockInterval: " + blockInterval)
        logger.debug("maxRatePerPartition: " + maxRatePerPartition)
        logger.debug("logParserClass: " + logParserClass)
        logger.debug("firstParserArgument: " + firstParserArgument)
        logger.debug(s"secondParserArgument: $secondParserArgument")
        logger.debug("==========================================")

        // Create the direct stream with the Kafka parameters and topics
        val kafkaInputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, inputTopics)

        // convert incoming logs to events and push them to Kafka
        defineLogParserJob(
            kafkaInputStream,
            firstParserArgument,
            secondParserArgument,
            logParserClass,
            brokerList,
            outputTopic
        )

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }

    def createTopics(topics: Set[String], zkUtils: ZkUtils, kpart: Int, krepl: Int) = {
        topics.foreach(topic => {
            if (!AdminUtils.topicExists(zkUtils, topic)) {
                AdminUtils.createTopic(zkUtils, topic, kpart, krepl)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with replication $krepl and partition $kpart")
            }
        })
    }

    def defineLogParserJob(kafkaInputStream: InputDStream[(String, String)],
                           firstParserArgument: String,
                           secondParserArgument: String,
                           logParserClass: String,
                           brokerList: String,
                           outputTopic: Set[String]) = {
        kafkaInputStream
            .foreachRDD(rdd => {
                rdd.cache
                rdd.foreachPartition(partition => {
                    if (partition.nonEmpty) {
                        // Dynamic loading of parser class on constructor with or without parameters
                        val logParser = if (firstParserArgument == null && secondParserArgument == null) {
                            Class.forName(logParserClass).newInstance.asInstanceOf[LogParser]
                        }
                        else {
                            val args = Array[String](firstParserArgument, secondParserArgument)
                            Class.forName(logParserClass).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[LogParser]
                        }

                        val events = partition.flatMap(log => {
                            logParser.parse(log._2).toSeq
                        })
                        outputTopic foreach (topic => {
                            val kafkaProducer = new KafkaEventProducer(brokerList, topic)
                            kafkaProducer.produce(events)
                        })
                    }
                })
                rdd.unpersist(true)
            })
    }
}
