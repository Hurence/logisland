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

import com.hurence.logisland.integration.{KafkaEventProducer, SparkUtils}
import com.hurence.logisland.log.LogParser
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.admin.AdminUtils
import kafka.serializer.StringDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.cli
import org.apache.commons.cli.{GnuParser, Options}
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
        val parser = new GnuParser()
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

        // set up context
        val sc = SparkUtils.initContext(this.getClass.getName, blockInterval, maxRatePerPartition)
        val ssc = new StreamingContext(sc, Milliseconds(batchDuration))

        // Define which topics to read from
        val topics = inputTopicList.split(",").toSet
        // create kafka topic if needed
        val zkClient = new ZkClient(zkQuorum, 30000, 30000, ZKStringSerializer)
        topics.foreach(topic => {
            if(!AdminUtils.topicExists(zkClient,topic)){
                AdminUtils.createTopic(zkClient,topic,1,1)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with replication 1 and partition 1 => should be changed in production")
            }
        })

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
        logger.debug("secondParserArgument: " + secondParserArgument)
        logger.debug("==========================================")

        // Create the direct stream with the Kafka parameters and topics
        val kafkaInputStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        // convert incoming logs to events and push them to Kafka
        kafkaInputStream
            .foreachRDD(rdd => {
                rdd.cache
                rdd.foreachPartition(partition => {
                    if (!partition.isEmpty) {
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

                        val kafkaProducer = new KafkaEventProducer(brokerList, outputTopicList)
                        kafkaProducer.produce(events)
                    }
                })
                rdd.unpersist(true)
            })

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }

}
