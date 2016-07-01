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

import java.io.ByteArrayInputStream

import com.hurence.logisland.event.serializer.EventKryoSerializer
import com.hurence.logisland.utils.elasticsearch.ElasticsearchEventIndexer
import com.hurence.logisland.utils.spark.SparkUtils
import com.typesafe.scalalogging.slf4j.LazyLogging
import kafka.admin.AdminUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.utils.ZkUtils
import org.apache.commons.cli
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.kafka.common.security.JaasUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.JavaConversions._

/**
  * This jobs takes kafka topics full of logs and index them into an ES index
  *
  * @see https://databricks.com/blog/2015/03/30/improvements-to-kafka-integration-of-spark-streaming.html
  *
  */
object EventIndexerJob extends LazyLogging {

    def main(args: Array[String]) {

        //////////////////////////////////////////
        // Commande lien management
        val parser = new DefaultParser()
        val options = new Options()


        options.addOption(new cli.Option("b", "kafka-brokers", true, "kafka broker list :localhost:9092, anotherhost:9092") {
            setRequired(true)
        })
        options.addOption(new cli.Option("in", "input-topics", true, "the kafka input topic list") {
            setRequired(true)
        })
        options.addOption(new cli.Option("mapper", "event-mapper", true, "the full class name of the event mapper") {
            setRequired(true)
        })
        options.addOption(new cli.Option("es", "es-config", true, "elasticsearch config name : sandbox") {
            setRequired(true)
        })
        options.addOption(new cli.Option("index", "index-name", true, "elasticsearch index") {
            setRequired(true)
        })
        options.addOption(new cli.Option("escluster", "es-cluster", true, "es clustern name ('log-island' by default"))
        options.addOption("bd", "batch-duration", true, "window time (in milliseconds) for micro batch (default: 2000)")
        options.addOption("bi", "block-interval", true, "window time (in milliseconds) for determining the number of partitions per batch (default: 350)")
        options.addOption("mrp", "max-rate-per-partition", true, "maximum rate (in messages per second) at which each Kafka partition will be read (default: unlimited)")
        options.addOption("h", "help", false, "print usage")
        options.addOption("zk", "zk-quorum", true, "quorum zookeper  zkServer1:2181,zkServ2:2181")
        options.addOption("name", "app-name", true, "spark application name")


        // parse the command line arguments
        val line = parser.parse(options, args)
        val batchDuration = line.getOptionValue("bd", "2000").toLong
        val blockInterval = line.getOptionValue("bi", "350")
        val maxRatePerPartition = line.getOptionValue("mrp", null)
        val brokerList = line.getOptionValue("b", "sandbox:9092")
        val topicList = line.getOptionValue("in", "log-island")
        val zkQuorum = line.getOptionValue("zk", "sandbox:2181")
        //elasticsearch info
        val esConfig = line.getOptionValue("es", "sandbox")
        val esIndex = line.getOptionValue("index", "logisland")
        val appName = line.getOptionValue("name", this.getClass.getName)

        // set up context
        val sc = SparkUtils.initContext(appName, blockInterval, maxRatePerPartition)
        val ssc = new StreamingContext(sc, Milliseconds(batchDuration))

        // Define which topics to read from
        val topics = topicList.split(",").toSet
        // create kafka topic if needed
        val zkUtils: ZkUtils = ZkUtils.apply(zkQuorum, 30000, 30000, JaasUtils.isZkSecurityEnabled)

        topics.foreach(topic => {
            if (!AdminUtils.topicExists(zkUtils, topic)) {
                AdminUtils.createTopic(zkUtils, topic, 1, 1)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with replication 1 and partition 1 => should be changed in production")
            }
        })

        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> "LogIndexer")

        logger.debug("line: " + line)
        logger.debug("batchDuration: " + batchDuration)
        logger.debug("blockInterval: " + blockInterval)
        logger.debug("maxRatePerPartition: " + maxRatePerPartition)
        logger.debug("brokerList: " + brokerList)
        logger.debug("topicList: " + topicList)
        logger.debug("esConfig: " + esConfig)
        logger.debug("esIndex: " + esIndex)

        // Create the direct stream with the Kafka parameters and topics
        val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)

        /**
          * get events from Kafka and index them
          */
        kafkaStream
            .foreachRDD(rdd => {
                rdd.cache
                rdd.foreachPartition(partition => {

                    // create a deserializer once per partition
                    val deserializer = new EventKryoSerializer(true)


                    // convert byte[] to events
                    val events = partition.map(rawEvent => {
                        val bais: ByteArrayInputStream = new ByteArrayInputStream(rawEvent._2)
                        val deserialized = deserializer.deserialize(bais)
                        bais.close()

                        deserialized
                    })


                    // launch indexation to es
                    val esIndexer = new ElasticsearchEventIndexer(esConfig, esIndex)
                    esIndexer.bulkLoad(events.toList, bulkSize = 10000)
                })
                rdd.unpersist(true)
            })

        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }
}
