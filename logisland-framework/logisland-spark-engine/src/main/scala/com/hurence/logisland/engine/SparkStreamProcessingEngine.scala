package com.hurence.logisland.engine

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections

import com.hurence.logisland.components.PropertyDescriptor
import com.hurence.logisland.event.Event
import com.hurence.logisland.processor.{AbstractEventProcessor, StandardProcessContext, StandardProcessorInstance}
import com.hurence.logisland.serializer.EventKryoSerializer
import com.hurence.logisland.validators.StandardValidators
import kafka.admin.AdminUtils
import kafka.serializer.DefaultDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

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

    val SPARK_STREAMING_BACKPRESSURE_ENABLED = new PropertyDescriptor.Builder()
        .name("spark.streaming.backpressure.enabled")
        .description("")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build

    val SPARK_STREAMING_UNPERSIST = new PropertyDescriptor.Builder()
        .name("spark.streaming.unpersist")
        .description("")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build

    val SPARK_UI_PORT = new PropertyDescriptor.Builder()
        .name("spark.ui.port")
        .description("")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("4050")
        .build


    val SPARK_STREAMING_TIMEOUT = new PropertyDescriptor.Builder()
        .name("spark.streaming.timeout")
        .description("")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("-1")
        .build

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(SPARK_MASTER)
        descriptors.add(SPARK_STREAMING_BLOCK_INTERVAL)
        descriptors.add(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(SPARK_APP_NAME)
        descriptors.add(SPARK_STREAMING_BATCH_DURATION)
        descriptors.add(KAFKA_METADATA_BROKER_LIST)
        descriptors.add(KAFKA_ZOOKEEPER_QUORUM)
        descriptors.add(SPARK_STREAMING_BACKPRESSURE_ENABLED)
        descriptors.add(SPARK_STREAMING_UNPERSIST)
        descriptors.add(SPARK_UI_PORT)
        descriptors.add(SPARK_STREAMING_TIMEOUT)

        Collections.unmodifiableList(descriptors)
    }


    private val logger = LoggerFactory.getLogger(classOf[SparkStreamProcessingEngine])

    override def start(engineContext: EngineContext, processorInstances: util.List[StandardProcessorInstance]) = {

        logger.info("starting Spark Engine")
        val sparkMaster = engineContext.getProperty(SPARK_MASTER).getValue
        val maxRatePerPartition = engineContext.getProperty(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION).getValue
        val appName = engineContext.getProperty(SPARK_APP_NAME).getValue
        val blockInterval = engineContext.getProperty(SPARK_STREAMING_BLOCK_INTERVAL).getValue
        val batchDuration = engineContext.getProperty(SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
        val brokerList = engineContext.getProperty(KAFKA_METADATA_BROKER_LIST).getValue
        val zkQuorum = engineContext.getProperty(KAFKA_ZOOKEEPER_QUORUM).getValue
        val backPressureEnabled = engineContext.getProperty(SPARK_STREAMING_BACKPRESSURE_ENABLED).getValue
        val streamingUnpersist = engineContext.getProperty(SPARK_STREAMING_UNPERSIST).getValue
        val timeout = engineContext.getProperty(SPARK_STREAMING_TIMEOUT).asInteger().intValue()

        /**
          * job configuration
          */
        val conf = new SparkConf()
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
        conf.set("spark.streaming.blockInterval", blockInterval)
        conf.set("spark.streaming.backpressure.enabled", backPressureEnabled)
        conf.set("spark.streaming.unpersist", streamingUnpersist)
        conf.set("spark.ui.port", "4050")
        conf.setAppName(appName)
        conf.setMaster(sparkMaster)

        @transient val sc = new SparkContext(conf)
        @transient val ssc = new StreamingContext(sc, Milliseconds(batchDuration))
        logger.info(s"spark context initialized with master:$sparkMaster, " +
            s"appName:$appName, " +
            s"blockInterval:$blockInterval, " +
            s"maxRatePerPartition:$maxRatePerPartition")


        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> "LogIndexer")
        val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)
        logger.debug("batchDuration: " + batchDuration)
        logger.debug("blockInterval: " + blockInterval)
        logger.debug("maxRatePerPartition: " + maxRatePerPartition)
        logger.debug("brokerList: " + brokerList)

        /*
       // v0.9.0.1
       ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()),
               30000, 30000, JaasUtils.isZkSecurityEnabled());
       TopicCommand.createTopic(zkUtils, opts);
       */


        /**
          * loop over processContext
          */
        processorInstances.toList.foreach(processorInstance => {

            val processorContext = new StandardProcessContext(processorInstance)

            // create topics if needed
            val inputTopics = processorContext.getProperty(AbstractEventProcessor.INPUT_TOPICS).getValue.split(",").toSet
            val outputTopics = processorContext.getProperty(AbstractEventProcessor.OUTPUT_TOPICS).getValue.split(",").toSet
            val errorTopics = processorContext.getProperty(AbstractEventProcessor.ERROR_TOPICS).getValue.split(",").toSet

            createTopicsIfNeeded(zkClient, inputTopics)
            createTopicsIfNeeded(zkClient, outputTopics)
            createTopicsIfNeeded(zkClient, errorTopics)

            // Create the direct stream with the Kafka parameters and topics
            val kafkaStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                ssc,
                kafkaParams,
                inputTopics
            )

            // setup the stream processing
            kafkaStream.foreachRDD(rdd => {

                rdd.foreachPartition(partition => {
                    // convert partition to events
                    val events = deserializeEvents(partition)
                    processorInstance.getProcessor.process(processorContext, events.toList)
                    logger.debug("bim")
                })

            })

        })


        // Start the computation
        ssc.start()

        if (timeout != -1)
            ssc.awaitTerminationOrTimeout(timeout)
        else
            ssc.awaitTermination()
    }

    def createTopicsIfNeeded(zkClient: ZkClient, inputTopics: Set[String]): Unit = {
        inputTopics.foreach(topic => {
            if (!AdminUtils.topicExists(zkClient, topic)) {
                AdminUtils.createTopic(zkClient, topic, 1, 1)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with replication 1 and partition 1 => should be changed in production")
            }
        })
    }

    def deserializeEvents(partition: Iterator[(Array[Byte], Array[Byte])]): Iterator[Event] = {
        partition.map(rawEvent => {
            // create a deserializer once per partition
            val deserializer = new EventKryoSerializer(true)
            val bais = new ByteArrayInputStream(rawEvent._2)
            val deserialized = deserializer.deserialize(bais)
            bais.close()

            deserialized
        })
    }

    override def shutdown(context: EngineContext) = {
    }

    override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String) = {
        logger.info(s"property ${descriptor.getName} value changed from $oldValue to $newValue")
    }

    override def getIdentifier: String = {
        "none"
    }

}
