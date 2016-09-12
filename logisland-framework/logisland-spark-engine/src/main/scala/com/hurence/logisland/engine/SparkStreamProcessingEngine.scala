package com.hurence.logisland.engine

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections
import java.util.regex.Pattern

import com.hurence.logisland.components.PropertyDescriptor
import com.hurence.logisland.event.{Event, EventField}
import com.hurence.logisland.log.{StandardParserContext, StandardParserInstance}
import com.hurence.logisland.processor.{AbstractEventProcessor, StandardProcessContext, StandardProcessorInstance}
import com.hurence.logisland.serializer.{EventAvroSerializer, EventKryoSerializer, EventSerializer}
import com.hurence.logisland.utils.event.ProcessorMetrics
import com.hurence.logisland.utils.kafka.KafkaSerializedEventProducer
import com.hurence.logisland.validators.StandardValidators
import kafka.admin.AdminUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
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

    val KAFKA_TOPIC_AUTOCREATE = new PropertyDescriptor.Builder()
        .name("kafka.topic.autoCreate")
        .description("define wether a topic should be created automatically if not already exists")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build

    val KAFKA_TOPIC_DEFAULT_PARTITIONS = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.partitions")
        .description("if autoCreate is set to true, this will set the number of partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("8")
        .build

    val KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR = new PropertyDescriptor.Builder()
        .name("kafka.topic.default.replicationFactor")
        .description("if autoCreate is set to true, this will set the number of replica for each partition at topic creation time")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("1")
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

    val INPUT_EVENT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("input.event.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("com.hurence.logisland.serializer.EventAvroSerializer")
        .build

    val OUTPUT_EVENT_SERIALIZER = new PropertyDescriptor.Builder()
        .name("output.event.serializer")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("com.hurence.logisland.serializer.EventAvroSerializer")
        .build

    val METRICS_TOPIC = new PropertyDescriptor.Builder()
        .name("kafka.metrics.topic")
        .description("")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
        descriptors.add(SPARK_SERIALIZER)
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
        descriptors.add(INPUT_EVENT_SERIALIZER)
        descriptors.add(OUTPUT_EVENT_SERIALIZER)
        descriptors.add(KAFKA_TOPIC_AUTOCREATE)
        descriptors.add(KAFKA_TOPIC_DEFAULT_PARTITIONS)
        descriptors.add(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR)
        descriptors.add(METRICS_TOPIC)

        Collections.unmodifiableList(descriptors)
    }

    /**
      * start the engine
      *
      * @param engineContext
      * @param processorInstances
      * @param parserInstances
      */
    override def start(engineContext: EngineContext,
                       processorInstances: util.List[StandardProcessorInstance],
                       parserInstances: util.List[StandardParserInstance]) = {


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
        //
        val sparkMaster = engineContext.getProperty(SPARK_MASTER).getValue
        //val sparkYarnDeploymode = engineContext.getProperty(SPARK_YARN_DEPLOYMODE).getValue
        val sparkDriverMemory = engineContext.getProperty(SPARK_DRIVER_MEMORY).getValue
        val sparkExecutorMemory = engineContext.getProperty(SPARK_EXECUTOR_MEMORY).getValue
        val sparkDriverCores = engineContext.getProperty(SPARK_DRIVER_CORES).getValue
        val sparkExecutorCores = engineContext.getProperty(SPARK_EXECUTOR_CORES).getValue
        val sparkSerializer = engineContext.getProperty(SPARK_SERIALIZER).getValue

        val maxRatePerPartition = engineContext.getProperty(SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION).getValue
        val appName = engineContext.getProperty(SPARK_APP_NAME).getValue
        val blockInterval = engineContext.getProperty(SPARK_STREAMING_BLOCK_INTERVAL).getValue
        val batchDuration = engineContext.getProperty(SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
        val backPressureEnabled = engineContext.getProperty(SPARK_STREAMING_BACKPRESSURE_ENABLED).getValue
        val streamingUnpersist = engineContext.getProperty(SPARK_STREAMING_UNPERSIST).getValue
        val timeout = engineContext.getProperty(SPARK_STREAMING_TIMEOUT).asInteger().intValue()

        // log-island stuff
        val inSerializerClass = engineContext.getProperty(INPUT_EVENT_SERIALIZER).getValue
        val outSerializerClass = engineContext.getProperty(OUTPUT_EVENT_SERIALIZER).getValue

        // Kafka stuff
        val brokerList = engineContext.getProperty(KAFKA_METADATA_BROKER_LIST).getValue
        val zkQuorum = engineContext.getProperty(KAFKA_ZOOKEEPER_QUORUM).getValue
        val topicAutocreate = engineContext.getProperty(KAFKA_TOPIC_AUTOCREATE).asBoolean().booleanValue()
        val topicDefaultPartitions = engineContext.getProperty(KAFKA_TOPIC_DEFAULT_PARTITIONS).asInteger().intValue()
        val topicDefaultReplicationFactor = engineContext.getProperty(KAFKA_TOPIC_DEFAULT_REPLICATION_FACTOR).asInteger().intValue()

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
                conf.set("spark.yarn.queue", engineContext.getProperty(SPARK_YARN_QUEUE).getValue)
            }
        }

        // Need to check if the properties are set because those properties are not "requires"
        if (engineContext.getProperty(SPARK_DRIVER_MEMORY).isSet) {
            conf.set("spark.driver.memory", engineContext.getProperty(SPARK_DRIVER_MEMORY).getValue)
        }
        if (engineContext.getProperty(SPARK_EXECUTOR_MEMORY).isSet) {
            conf.set("spark.executor.memory", engineContext.getProperty(SPARK_EXECUTOR_MEMORY).getValue)
        }
        if (engineContext.getProperty(SPARK_DRIVER_CORES).isSet) {
            conf.set("spark.driver.cores", engineContext.getProperty(SPARK_DRIVER_CORES).getValue)
        }
        if (engineContext.getProperty(SPARK_EXECUTOR_CORES).isSet) {
            conf.set("spark.executor.cores", engineContext.getProperty(SPARK_EXECUTOR_CORES).getValue)
        }


        @transient val sc = new SparkContext(conf)
        @transient val ssc = new StreamingContext(sc, Milliseconds(batchDuration))
        logger.info(s"spark context initialized with master:$sparkMaster, " +
            s"appName:$appName, " +
            s"batchDuration:$batchDuration " +
            s"blockInterval:$blockInterval, " +
            s"maxRatePerPartition:$maxRatePerPartition")


        // Define the Kafka parameters, broker list must be specified
        val kafkaParams = Map("metadata.broker.list" -> brokerList, "group.id" -> appName)
        val zkClient = new ZkClient(zkQuorum, 3000, 3000, ZKStringSerializer)

        if (topicAutocreate && engineContext.getProperty(METRICS_TOPIC).isSet) {

            createTopicsIfNeeded(
                zkClient,
                Set(engineContext.getProperty(METRICS_TOPIC).getValue),
                topicDefaultPartitions,
                topicDefaultReplicationFactor)
        }


        /**
          * loop over processContext
          */
        parserInstances.toList.foreach(parserInstance => {
            val parseContext = new StandardParserContext(parserInstance)
            parserInstance.getParser.init(parseContext)

            // create topics if needed
            val inputTopics = parseContext.getProperty(AbstractEventProcessor.INPUT_TOPICS).getValue.split(",").toSet
            val outputTopics = parseContext.getProperty(AbstractEventProcessor.OUTPUT_TOPICS).getValue.split(",").toSet
            val errorTopics = parseContext.getProperty(AbstractEventProcessor.ERROR_TOPICS).getValue.split(",").toSet

            if (topicAutocreate) {
                createTopicsIfNeeded(zkClient, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
            }

            // Create the direct stream with the Kafka parameters and topics

            val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                ssc,
                kafkaParams,
                inputTopics
            )

            // setup the stream processing
            kafkaStream.foreachRDD(rdd => {

                rdd.foreachPartition(partition => {

                    if (partition.nonEmpty) {
                        val startTime = System.currentTimeMillis()
                        val partitionId = TaskContext.get.partitionId()
                        // use this uniqueId to transactionally commit the data in partitionIterator


                        val outgoingEvents = partition.flatMap(rawEvent => {
                            parserInstance.getParser.parse(parseContext, rawEvent._1, rawEvent._2).toList

                        }).toList


                        // convert partition to events
                        val parser = new Schema.Parser
                        val serializer = outSerializerClass match {
                            case "com.hurence.logisland.serializer.EventAvroSerializer" =>
                                val parser = new Schema.Parser
                                val outSchemaContent = parseContext.getProperty(AbstractEventProcessor.OUTPUT_SCHEMA).getValue
                                val outSchema = parser.parse(outSchemaContent)
                                new EventAvroSerializer(outSchema)
                            case _ =>
                                new EventKryoSerializer(true)
                        }


                        val kafkaProducer = new KafkaSerializedEventProducer(
                            brokerList,
                            parseContext.getProperty(AbstractEventProcessor.OUTPUT_TOPICS).getValue,
                            serializer)
                        kafkaProducer.produce(outgoingEvents)


                        /**
                          * send metrics to topic if needed
                          */
                        if (engineContext.getProperty(METRICS_TOPIC).isSet) {
                            val processorFields = new util.HashMap[String, EventField]()
                            processorFields.put("spark_app_name", new EventField("spark_app_name", "string", appName))
                            processorFields.put("input_topics", new EventField("input_topics", "string", inputTopics.toString()))
                            processorFields.put("output_topics", new EventField("output_topics", "string", outputTopics.toString()))
                            processorFields.put("component_name", new EventField("component_name", "string", parseContext.getName))
                            processorFields.put("component_type", new EventField("component_type", "string", "parser"))
                            try {
                                processorFields.put("batch_duration", new EventField("batch_duration", "int", batchDuration.toInt))
                                processorFields.put("block_interval", new EventField("block_interval", "int", blockInterval.toInt))
                                processorFields.put("max_rate_per_partition", new EventField("max_rate_per_partition", "int", maxRatePerPartition.toInt))
                            }

                            processorFields.put("spark_partition_id", new EventField("spark_partition_id", "int", partitionId))
                            processorFields.put("num_incoming_messages", new EventField("num_incoming_messages", "int", partition.size))
                            processorFields.put("num_outgoing_events", new EventField("num_outgoing_events", "int", outgoingEvents.size))

                            val processorMetrics = ProcessorMetrics.computeMetrics(outgoingEvents, processorFields, System.currentTimeMillis() - startTime).toList

                            val kafkaProducer4Metrics = new KafkaSerializedEventProducer(
                                brokerList,
                                engineContext.getProperty(METRICS_TOPIC).getValue,
                                serializer)

                            kafkaProducer4Metrics.produce(processorMetrics)
                        }
                    }
                })
            })
        })


        /**
          * loop over processContext
          */
        processorInstances.toList.foreach(processorInstance => {
            val processorContext = new StandardProcessContext(processorInstance)
            processorInstance.getProcessor.init(processorContext)

            // create topics if needed
            val inputTopics = processorContext.getProperty(AbstractEventProcessor.INPUT_TOPICS).getValue.split(",").toSet
            val outputTopics = processorContext.getProperty(AbstractEventProcessor.OUTPUT_TOPICS).getValue.split(",").toSet
            //  val errorTopics = processorContext.getProperty(AbstractEventProcessor.ERROR_TOPICS).getValue.split(",").toSet

            if (topicAutocreate) {
                createTopicsIfNeeded(zkClient, inputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                createTopicsIfNeeded(zkClient, outputTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
                //      createTopicsIfNeeded(zkClient, errorTopics, topicDefaultPartitions, topicDefaultReplicationFactor)
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
                        val startTime = System.currentTimeMillis()
                        val partitionId = TaskContext.get.partitionId()
                        // use this uniqueId to transactionally commit the data in partitionIterator

                        // convert partition to events
                        val parser = new Schema.Parser

                        val deserializer = inSerializerClass match {
                            case "com.hurence.logisland.serializer.EventAvroSerializer" =>
                                val inSchemaContent = processorContext.getProperty(AbstractEventProcessor.INPUT_SCHEMA).getValue
                                val inSchema = parser.parse(inSchemaContent)
                                new EventAvroSerializer(inSchema)
                            case _ =>
                                new EventKryoSerializer(true)
                        }


                        val incomingEvents = deserializeEvents(partition, deserializer)

                        val outgoingEvents = processorInstance.getProcessor.process(processorContext, incomingEvents)


                        val serializer = outSerializerClass match {
                            case "com.hurence.logisland.serializer.EventAvroSerializer" =>
                                val outSchemaContent = processorContext.getProperty(AbstractEventProcessor.OUTPUT_SCHEMA).getValue
                                val outSchema = parser.parse(outSchemaContent)
                                new EventAvroSerializer(outSchema)
                            case _ =>
                                new EventKryoSerializer(true)
                        }


                        val kafkaProducer = new KafkaSerializedEventProducer(
                            brokerList,
                            processorContext.getProperty(AbstractEventProcessor.OUTPUT_TOPICS).getValue,
                            serializer)
                        kafkaProducer.produce(outgoingEvents.toList)

                        /**
                          * send metrics to topic if needed
                          */
                        if (engineContext.getProperty(METRICS_TOPIC).isSet) {
                            val processorFields = new util.HashMap[String, EventField]()
                            processorFields.put("spark_app_name", new EventField("spark_app_name", "string", appName))
                            processorFields.put("input_topics", new EventField("input_topics", "string", inputTopics.toString()))
                            processorFields.put("output_topics", new EventField("output_topics", "string", outputTopics.toString()))
                            processorFields.put("component_name", new EventField("component_name", "string", processorContext.getName))
                            processorFields.put("component_type", new EventField("component_type", "string", "parser"))

                            try {
                                processorFields.put("batch_duration", new EventField("batch_duration", "int", batchDuration.toInt))
                                processorFields.put("block_interval", new EventField("block_interval", "int", blockInterval.toInt))
                                processorFields.put("max_rate_per_partition", new EventField("max_rate_per_partition", "int", maxRatePerPartition.toInt))
                            }
                            processorFields.put("spark_partition_id", new EventField("spark_partition_id", "int", partitionId))
                            processorFields.put("num_incoming_messages", new EventField("num_incoming_messages", "int", partition.size))
                            processorFields.put("num_incoming_events", new EventField("num_incoming_events", "int", incomingEvents.size))
                            processorFields.put("num_outgoing_events", new EventField("num_outgoing_events", "int", outgoingEvents.size))



                            val processorMetrics = ProcessorMetrics.computeMetrics(incomingEvents, processorFields, System.currentTimeMillis() - startTime).toList

                            val kafkaProducer4Metrics = new KafkaSerializedEventProducer(
                                brokerList,
                                engineContext.getProperty(METRICS_TOPIC).getValue,
                                serializer)

                            kafkaProducer4Metrics.produce(processorMetrics)
                        }
                    }


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

    def deserializeEvents(partition: Iterator[(Array[Byte], Array[Byte])], serializer: EventSerializer): List[Event] = {
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

    override def getIdentifier: String = {
        "SparkStreamProcessingEngine"
    }

}
