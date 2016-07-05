package com.hurence.logisland.engine

import java.util
import java.util.Collections

import com.hurence.logisland.components.PropertyDescriptor
import com.hurence.logisland.validators.StandardValidators
import kafka.admin.AdminUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.utils.{ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by tom on 05/07/16.
  */
class SparkStreamProcessingEngine extends AbstractStreamProcessingEngine {

    val SPARK_MASTER = new PropertyDescriptor.Builder()
        .name("spark.master")
        .description("the url to Spark Master")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("local[2]")
        .build

    val SPARK_APP_NAME = new PropertyDescriptor.Builder()
        .name("spark.appName")
        .description("application name")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("local[2]")
        .build

    val SPARK_STREAMING_BLOCK_INTERVAL = new PropertyDescriptor.Builder()
        .name("spark.streaming.blockInterval")
        .description("the block interval")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("350")
        .build

    val SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION = new PropertyDescriptor.Builder()
        .name("spark.streaming.kafka.maxRatePerPartition")
        .description("")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("1")
        .build

    val SPARK_STREAMING_BATCH_DURATION = new PropertyDescriptor.Builder()
        .name("spark.streaming.batchDuration")
        .description("")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("200")
        .build

    val KAFKA_METADATA_BROKER_LIST = new PropertyDescriptor.Builder()
        .name("kafka.metadata.broker.list")
        .description("")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:9092")
        .build

    val KAFKA_ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
        .name("kafka.zookeeper.quorum")
        .description("")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("sandbox:2181")
        .build


    private val logger = LoggerFactory.getLogger(classOf[SparkStreamProcessingEngine])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(SPARK_MASTER)
        descriptors.add(SPARK_STREAMING_BLOCK_INTERVAL)
        descriptors.add(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(SPARK_APP_NAME)
        descriptors.add(SPARK_STREAMING_BATCH_DURATION)
        descriptors.add(KAFKA_METADATA_BROKER_LIST)
        descriptors.add(KAFKA_ZOOKEEPER_QUORUM)

        Collections.unmodifiableList(descriptors)
    }

    var sc: SparkContext = null
    var ssc: StreamingContext = null

    override def start(context: EngineContext): Unit = {

        logger.info("starting Spark Engine")
        val sparkMaster = context.getProperty(SPARK_MASTER).getValue
        val maxRatePerPartition = context.getProperty(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION).getValue
        val appName = context.getProperty(SPARK_APP_NAME).getValue
        val blockInterval = context.getProperty(SPARK_STREAMING_BLOCK_INTERVAL).getValue
        val batchDuration = context.getProperty(SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
        val brokerList = context.getProperty(KAFKA_METADATA_BROKER_LIST).getValue
        val zkQuorum = context.getProperty(KAFKA_ZOOKEEPER_QUORUM).getValue
        val topicList = "test"

        // job configuration
        val conf = new SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
        conf.set("spark.streaming.blockInterval", blockInterval)
        conf.set("spark.streaming.backpressure.enabled", "true")
        conf.set("spark.streaming.unpersist", "false")
        conf.set("spark.ui.port", "4050")
        conf.setAppName(appName)
        conf.setMaster(sparkMaster)

        sc = new SparkContext(conf)

        logger.info(s"spark context initialized with master:$sparkMaster, appName:$appName, " +
            s"blockInterval:$blockInterval, maxRatePerPartition:$maxRatePerPartition")
        ssc = new StreamingContext(sc, Milliseconds(batchDuration))



        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> "LogIndexer")

        logger.debug("batchDuration: " + batchDuration)
        logger.debug("blockInterval: " + blockInterval)
        logger.debug("maxRatePerPartition: " + maxRatePerPartition)
        logger.debug("brokerList: " + brokerList)
        logger.debug("topicList: " + topicList)

        // Define which topics to read from
        val topics = topicList.split(",").toSet



        /*
       // v0.9.0.1
       ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
               30000, 30000, JaasUtils.isZkSecurityEnabled());
       TopicCommand.createTopic(zkUtils, opts);
       */
        // v0.8.2.1
        val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)

        topics.foreach(topic => {
            if (!AdminUtils.topicExists(zkClient, topic)) {
                AdminUtils.createTopic(zkClient, topic, 1, 1)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with replication 1 and partition 1 => should be changed in production")
            }
        })

        // Create the direct stream with the Kafka parameters and topics
        val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
            ssc,
            kafkaParams,
            topics
        )

        kafkaStream
            .foreachRDD(rdd => {

                rdd.foreachPartition(partition => {

                    logger.debug("bim")
                })

            })
        // Start the computation
        ssc.start()
        ssc.awaitTermination()
    }

    override def shutdown(context: EngineContext) = {
        ssc.stop(true)
    }

    override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String) = {
        logger.info(s"property ${descriptor.getName} value changed from $oldValue to $newValue")
    }

    override def getIdentifier: String = {
        "none"
    }

}
