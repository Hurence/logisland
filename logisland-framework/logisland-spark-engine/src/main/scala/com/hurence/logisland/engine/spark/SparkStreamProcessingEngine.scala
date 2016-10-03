package com.hurence.logisland.engine.spark

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections
import java.util.regex.Pattern

import com.hurence.logisland.processor.chain.KafkaRecordStream
import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.{AbstractStreamProcessingEngine, EngineContext}
import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.record.{Field, FieldType, Record, RecordUtils}
import com.hurence.logisland.serializer._
import com.hurence.logisland.utils.kafka.KafkaSerializedEventProducer
import com.hurence.logisland.utils.processor.ProcessorMetrics
import com.hurence.logisland.validator.StandardValidators
import kafka.admin.AdminUtils
import kafka.serializer.DefaultDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.avro.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Created by tom on 05/07/16.
  */
class SparkStreamProcessingEngine extends AbstractStreamProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[SparkStreamProcessingEngine])

    val SPARK_MASTER = new PropertyDescriptor.Builder()
        .name("spark.master")
        .description("The url to Spark Master")
        .required(true)
        // The regex allows "local[K]" with K as an integer,  "local[*]", "yarn", "yarn-client", "yarn-cluster" and "spark://HOST[:PORT]"
        // there is NO support for "mesos://HOST:PORT"
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^(yarn(-(client|cluster))?|local\\[[0-9\\*]+\\]|spark:\\/\\/([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+|[a-z][a-z0-9\\.\\-]+)(:[0-9]+)?)$")))
        .defaultValue("local[2]")
        .build


    val SPARK_YARN_DEPLOYMODE = new PropertyDescriptor.Builder()
        .name("spark.yarn.deploy-mode")
        .description("The yarn deploy mode")
        .required(false)
        .allowableValues("client", "cluster")
        .build

    val SPARK_YARN_QUEUE = new PropertyDescriptor.Builder()
        .name("spark.yarn.queue")
        .description("The name of the YARN queue")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val SPARK_APP_NAME = new PropertyDescriptor.Builder()
        .name("spark.appName")
        .description("Tha application name")
        .required(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[a-zA-z0-9-_\\.]+$")))
        .defaultValue("log-island")
        .build

    val memorySizePattern = Pattern.compile("^[0-9]+[mMgG]$");
    val SPARK_DRIVER_MEMORY = new PropertyDescriptor.Builder()
        .name("spark.driver.memory")
        .description("The memory size for Spark driver")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(memorySizePattern))
        .build

    val SPARK_EXECUTOR_MEMORY = new PropertyDescriptor.Builder()
        .name("spark.executor.memory")
        .description("The memory size for Spark executors")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(memorySizePattern))
        .build

    val SPARK_DRIVER_CORES = new PropertyDescriptor.Builder()
        .name("spark.driver.cores")
        .description("The number of cores for Spark driver")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build

    val SPARK_EXECUTOR_CORES = new PropertyDescriptor.Builder()
        .name("spark.executor.cores")
        .description("The number of cores for Spark driver")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build

    val SPARK_EXECUTOR_INSTANCES = new PropertyDescriptor.Builder()
        .name("spark.executor.instances")
        .description("The number of instances for Spark app")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build

    val SPARK_SERIALIZER = new PropertyDescriptor.Builder()
        .name("spark.serializer")
        .description("Class to use for serializing objects that will be sent over the network or need to be cached in serialized form")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("org.apache.spark.serializer.KryoSerializer")
        .build

    val SPARK_STREAMING_BLOCK_INTERVAL = new PropertyDescriptor.Builder()
        .name("spark.streaming.blockInterval")
        .description("The block interval")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("350")
        .build

    val SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION = new PropertyDescriptor.Builder()
        .name("spark.streaming.kafka.maxRatePerPartition")
        .description("")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .build

    val SPARK_STREAMING_BATCH_DURATION = new PropertyDescriptor.Builder()
        .name("spark.streaming.batchDuration")
        .description("")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("200")
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
        .addValidator(StandardValidators.PORT_VALIDATOR)
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
        descriptors.add(SPARK_YARN_DEPLOYMODE)
        descriptors.add(SPARK_YARN_QUEUE)
        descriptors.add(SPARK_DRIVER_MEMORY)
        descriptors.add(SPARK_EXECUTOR_MEMORY)
        descriptors.add(SPARK_DRIVER_CORES)
        descriptors.add(SPARK_EXECUTOR_CORES)
        descriptors.add(SPARK_EXECUTOR_INSTANCES)
        descriptors.add(SPARK_SERIALIZER)
        descriptors.add(SPARK_STREAMING_BLOCK_INTERVAL)
        descriptors.add(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(SPARK_APP_NAME)
        descriptors.add(SPARK_STREAMING_BATCH_DURATION)
        descriptors.add(SPARK_STREAMING_BACKPRESSURE_ENABLED)
        descriptors.add(SPARK_STREAMING_UNPERSIST)
        descriptors.add(SPARK_UI_PORT)
        descriptors.add(SPARK_STREAMING_TIMEOUT)

        Collections.unmodifiableList(descriptors)
    }

    /**
      * start the engine
      *
      * @param engineContext
      */
    override def start(engineContext: EngineContext) = {


        // Logging verbosity lowered
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
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

        logger.info("starting Spark Engine")

        val sparkMaster = engineContext.getProperty(SPARK_MASTER).asString
        val sparkSerializer = engineContext.getProperty(SPARK_SERIALIZER).asString
        val maxRatePerPartition = engineContext.getProperty(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION).asString
        val appName = engineContext.getProperty(SPARK_APP_NAME).asString
        val blockInterval = engineContext.getProperty(SPARK_STREAMING_BLOCK_INTERVAL).asString
        val batchDuration = engineContext.getProperty(SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
        val backPressureEnabled = engineContext.getProperty(SPARK_STREAMING_BACKPRESSURE_ENABLED).asString
        val streamingUnpersist = engineContext.getProperty(SPARK_STREAMING_UNPERSIST).asString
        val timeout = engineContext.getProperty(SPARK_STREAMING_TIMEOUT).asInteger().intValue()





        /**
          * job configuration
          */
        val conf = new SparkConf()
        conf.set("spark.serializer", sparkSerializer)
        conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
        conf.set("spark.streaming.blockInterval", blockInterval)
        conf.set("spark.streaming.backpressure.enabled", backPressureEnabled)
        conf.set("spark.streaming.unpersist", streamingUnpersist)
        conf.set("spark.ui.port", "4050")
        conf.setAppName(appName)
        conf.setMaster(sparkMaster)



        if (sparkMaster startsWith "yarn") {
            // Note that SPARK_YARN_DEPLOYMODE is not used by spark itself but only by spark-submit CLI
            // That's why we do not need to propagate it here
            if (engineContext.getProperty(SPARK_YARN_QUEUE).isSet) {
                conf.set("spark.yarn.queue", engineContext.getProperty(SPARK_YARN_QUEUE).asString)
            }
        }

        // Need to check if the properties are set because those properties are not "requires"
        if (engineContext.getProperty(SPARK_DRIVER_MEMORY).isSet) {
            conf.set("spark.driver.memory", engineContext.getProperty(SPARK_DRIVER_MEMORY).asString)
        }
        if (engineContext.getProperty(SPARK_EXECUTOR_MEMORY).isSet) {
            conf.set("spark.executor.memory", engineContext.getProperty(SPARK_EXECUTOR_MEMORY).asString)
        }
        if (engineContext.getProperty(SPARK_DRIVER_CORES).isSet) {
            conf.set("spark.driver.cores", engineContext.getProperty(SPARK_DRIVER_CORES).asString)
        }
        if (engineContext.getProperty(SPARK_EXECUTOR_CORES).isSet) {
            conf.set("spark.executor.cores", engineContext.getProperty(SPARK_EXECUTOR_CORES).asString)
        }
        if (engineContext.getProperty(SPARK_EXECUTOR_INSTANCES).isSet) {
            conf.set("spark.executor.instances", engineContext.getProperty(SPARK_EXECUTOR_INSTANCES).asString)
        }

        @transient val sc = new SparkContext(conf)
        @transient val ssc = new StreamingContext(sc, Milliseconds(batchDuration))
        logger.info(s"spark context initialized with master:$sparkMaster, " +
            s"appName:$appName, " +
            s"batchDuration:$batchDuration " +
            s"blockInterval:$blockInterval, " +
            s"maxRatePerPartition:$maxRatePerPartition")

        /**
          * loop over processContext
          */

        engineContext.getProcessorChainInstances.foreach(processorChainInstance => {
            try{
                // Define the Kafka parameters, broker list must be specified
                val processorChainContext = new StandardProcessContext(processorChainInstance)
                val inSerializerClass = processorChainContext.getProperty(KafkaRecordStream.INPUT_SERIALIZER).asString
                val outSerializerClass = processorChainContext.getProperty(KafkaRecordStream.OUTPUT_SERIALIZER).asString
                val inputTopics = processorChainContext.getProperty(KafkaRecordStream.INPUT_TOPICS).asString.split(",").toSet
                val outputTopics = processorChainContext.getProperty(KafkaRecordStream.OUTPUT_TOPICS).asString.split(",").toSet
                val errorTopics = processorChainContext.getProperty(KafkaRecordStream.ERROR_TOPICS).asString.split(",").toSet
                val topicAutocreate = processorChainContext.getProperty(KafkaRecordStream.KAFKA_TOPIC_AUTOCREATE).asBoolean().booleanValue()
                val topicDefaultPartitions = processorChainContext.getProperty(KafkaRecordStream.KAFKA_TOPIC_DEFAULT_PARTITIONS).asInteger().intValue()
                val topicDefaultReplicationFactor = processorChainContext.getProperty(KafkaRecordStream.KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR).asInteger().intValue()
                val brokerList = processorChainContext.getProperty(KafkaRecordStream.KAFKA_METADATA_BROKER_LIST).asString
                val zkQuorum = processorChainContext.getProperty(KafkaRecordStream.KAFKA_ZOOKEEPER_QUORUM).asString
                val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)
                val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> appName)

                if (topicAutocreate) {
                    createTopicsIfNeeded(zkClient, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                    createTopicsIfNeeded(zkClient, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                    createTopicsIfNeeded(zkClient, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                    if (processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).isSet) {
                        createTopicsIfNeeded(
                            zkClient,
                            Set(processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).asString),
                            topicDefaultPartitions,
                            topicDefaultReplicationFactor)
                    }
                }

                // Create the direct stream with the Kafka parameters and topics
                val kafkaStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                    ssc,
                    kafkaParams,
                    inputTopics
                )

                // setup the stream processing
                kafkaStream.foreachRDD(rdd => {
                    rdd.foreachPartition(partition => {
                        if (partition.nonEmpty) {

                            val partitionId = TaskContext.get.partitionId()

                            /**
                              * create serializers
                              */
                            val parser = new Schema.Parser
                            val deserializer = inSerializerClass match {
                                case c if c == KafkaRecordStream.AVRO_SERIALIZER.getValue =>
                                    val inSchemaContent =
                                        processorChainContext.getProperty(KafkaRecordStream.AVRO_INPUT_SCHEMA).asString
                                    val inSchema = parser.parse(inSchemaContent)
                                    new AvroSerializer(inSchema)
                                case c if c == KafkaRecordStream.JSON_SERIALIZER.getValue => new JsonSerializer()
                                case _ => new KryoSerializer(true)
                            }
                            val serializer = outSerializerClass match {
                                case c if c == KafkaRecordStream.AVRO_SERIALIZER.getValue =>
                                    val outSchemaContent =
                                        processorChainContext.getProperty(KafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString
                                    val outSchema = parser.parse(outSchemaContent)
                                    new AvroSerializer(outSchema)
                                case c if c == KafkaRecordStream.JSON_SERIALIZER.getValue => new JsonSerializer()
                                case _ => new KryoSerializer(true)
                            }


                            /**
                              * process events by chaining output records
                              */
                            var isFirstProcessor = true
                            var incomingEvents: util.Collection[Record] = null
                            var outgoingEvents: util.Collection[Record] = null

                            processorChainInstance.getProcessorInstances.foreach(processorInstance => {
                                val startTime = System.currentTimeMillis()
                                val processorContext = new StandardProcessContext(processorInstance)
                                val processor = processorInstance.getProcessor


                                if (isFirstProcessor) {
                                    /**
                                      * convert incoming Kafka messages into Records
                                      * if there's no serializer we assume that we need to compute a Record from K/V
                                      */
                                    incomingEvents = if (inSerializerClass == KafkaRecordStream.NO_SERIALIZER.getValue) {
                                        // parser
                                        partition.map(rawMessage => {
                                            RecordUtils.getKeyValueRecord(new String(rawMessage._1), new String(rawMessage._2))
                                        }).toList
                                    } else {
                                        // processor
                                        deserializeEvents(partition, deserializer)
                                    }

                                    isFirstProcessor = false
                                }else{
                                    incomingEvents = outgoingEvents
                                }

                                // do the processing
                                outgoingEvents = processor.process(processorContext, incomingEvents)

                                sendMetrics(maxRatePerPartition, appName, blockInterval, batchDuration, brokerList,
                                    processorChainContext, inputTopics, outputTopics, partition, startTime, partitionId,
                                    serializer, incomingEvents, outgoingEvents)
                            })



                            /**
                              * push outgoing events to Kafka
                              */
                            val kafkaProducer = new KafkaSerializedEventProducer(
                                brokerList,
                                processorChainContext.getProperty(KafkaRecordStream.OUTPUT_TOPICS).asString,
                                serializer)
                            kafkaProducer.produce(outgoingEvents.toList)



                        }


                    })
                })
            }catch {
                case ex:Exception => logger.error("something bad happened, please check Kafka or cluster health : {}",
                    ex.getMessage)
            }

        })


        // Start the computation
        ssc.start()

        if (timeout != -1)
            ssc.awaitTerminationOrTimeout(timeout)
        else
            ssc.awaitTermination()
    }

    def sendMetrics(maxRatePerPartition: String, appName: String, blockInterval: String, batchDuration: Int, brokerList: String, processorChainContext: StandardProcessContext, inputTopics: Set[String], outputTopics: Set[String], partition: Iterator[(Array[Byte], Array[Byte])], startTime: Long, partitionId: Int, serializer: RecordSerializer, incomingEvents: util.Collection[Record], outgoingEvents: util.Collection[Record]): Unit = {
        if (processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).isSet) {
            val processorFields = new util.HashMap[String, Field]()
            processorFields.put("spark_app_name", new Field("spark_app_name", FieldType.STRING, appName))
            processorFields.put("input_topics", new Field("input_topics", FieldType.STRING, inputTopics.toString()))
            processorFields.put("output_topics", new Field("output_topics", FieldType.STRING, outputTopics.toString()))
            processorFields.put("component_name", new Field("component_name", FieldType.STRING, processorChainContext.getName))
            processorFields.put("component_type", new Field("component_type", FieldType.STRING, "parser"))

            try {
                processorFields.put("batch_duration", new Field("batch_duration", FieldType.INT, batchDuration.toInt))
                processorFields.put("block_interval", new Field("block_interval", FieldType.INT, blockInterval.toInt))
                processorFields.put("max_rate_per_partition", new Field("max_rate_per_partition", FieldType.INT, maxRatePerPartition.toInt))
            }
            processorFields.put("spark_partition_id", new Field("spark_partition_id", FieldType.INT, partitionId))
            processorFields.put("num_incoming_messages", new Field("num_incoming_messages", FieldType.INT, partition.size))
            processorFields.put("num_incoming_events", new Field("num_incoming_events", FieldType.INT, incomingEvents.size))
            processorFields.put("num_outgoing_events", new Field("num_outgoing_events", FieldType.INT, outgoingEvents.size))



            val processorMetrics = ProcessorMetrics.computeMetrics(incomingEvents, processorFields, System.currentTimeMillis() - startTime).toList

            val kafkaProducer4Metrics = new KafkaSerializedEventProducer(
                brokerList,
                processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).asString,
                serializer)

            kafkaProducer4Metrics.produce(processorMetrics)
        }
    }

    /**
      * Topic creation
      *
      * @param zkClient
      * @param inputTopics
      * @param topicDefaultPartitions
      * @param topicDefaultReplicationFactor
      */
    def createTopicsIfNeeded(zkClient: ZkClient,
                             inputTopics: Set[String],
                             topicDefaultPartitions: Int,
                             topicDefaultReplicationFactor: Int): Unit = {

        inputTopics.foreach(topic => {
            if (!AdminUtils.topicExists(zkClient, topic)) {
                AdminUtils.createTopic(zkClient, topic, topicDefaultPartitions, topicDefaultReplicationFactor)
                Thread.sleep(1000)
                logger.info(s"created topic $topic with" +
                    s" $topicDefaultPartitions partitions and" +
                    s" $topicDefaultReplicationFactor replicas")
            }
        })
    }

    def deserializeEvents(partition: Iterator[(Array[Byte], Array[Byte])], serializer: RecordSerializer): List[Record] = {
        partition.map(rawEvent => {
            val bais = new ByteArrayInputStream(rawEvent._2)
            val deserialized = serializer.deserialize(bais)
            bais.close()

            deserialized
        }).toList
    }

    override def shutdown(context: EngineContext) = {
    }

    override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String) = {
        logger.info(s"property ${descriptor.getName} value changed from $oldValue to $newValue")
    }

}
