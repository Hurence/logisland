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

import java.io.ByteArrayInputStream

import com.hurence.logisland.components.ComponentsFactory
import com.hurence.logisland.config.LogislandSessionConfigReader
import com.hurence.logisland.event.Event
import com.hurence.logisland.serializer.EventKryoSerializer
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.cli
import org.apache.commons.cli.{DefaultParser, Options}

/**
  * This jobs takes kafka topics full of logs and index them into an ES index
  *
  * @see https://databricks.com/blog/2015/03/30/improvements-to-kafka-integration-of-spark-streaming.html
  *
  */
object EventProcessorJob extends LazyLogging {

    def main(args: Array[String]) {

        //////////////////////////////////////////
        // Commande lien management
        val parser = new DefaultParser()
        val options = new Options()


        options.addOption(new cli.Option("conf", "config-file", true, "config file path") {
            setRequired(true)
        })


        // parse the command line arguments
        val line = parser.parse(options, args)
        val configFile = line.getOptionValue("conf")

        val sessionConf = new LogislandSessionConfigReader().loadConfig(configFile)
        val processors = ComponentsFactory.getAllProcessorInstances(sessionConf)

        val engineInstance = ComponentsFactory.getEngineInstance(sessionConf)
        if (engineInstance.isPresent) {
            val engineContext = new StandardEngineContext(engineInstance.get)


            engineInstance.get.getEngine.start(engineContext)
        }



        /*

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


                val logislandContext = new LogislandContext()

                /**
                  * get events from Kafka and index them
                  */
                kafkaStream
                    .foreachRDD(rdd => {
                        rdd.cache
                        rdd.foreachPartition(partition => {

                            // convert partition to events
                            val events = deserializeEvents(partition)


                            // launch indexation to es
                            val esIndexer = new ElasticsearchEventIndexer(esConfig, esIndex)
                            esIndexer.bulkLoad(events.toList, bulkSize = 10000)
                        })
                        rdd.unpersist(true)
                    })

                // Start the computation
                ssc.start()
                ssc.awaitTermination()*/
    }


    def deserializeEvents(partition: Iterator[(String, Array[Byte])]): Iterator[Event] = {
        partition.map(rawEvent => {
            // create a deserializer once per partition
            val deserializer = new EventKryoSerializer(true)
            val bais: ByteArrayInputStream = new ByteArrayInputStream(rawEvent._2)
            val deserialized = deserializer.deserialize(bais)
            bais.close()

            deserialized
        })
    }
}
