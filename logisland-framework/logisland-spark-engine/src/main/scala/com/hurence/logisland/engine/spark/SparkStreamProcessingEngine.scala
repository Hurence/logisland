package com.hurence.logisland.engine.spark

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections
import java.util.regex.Pattern

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.{AbstractStreamProcessingEngine, EngineContext}
import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.processor.chain.KafkaRecordStream
import com.hurence.logisland.record._
import com.hurence.logisland.serializer._
import com.hurence.logisland.util.kafka.KafkaSink
import com.hurence.logisland.util.processor.ProcessorMetrics
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.util.validator.StandardValidators
import kafka.admin.AdminUtils
import kafka.serializer.DefaultDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.avro.Schema.Parser
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


object SparkStreamProcessingEngine {

    val SPARK_MASTER = new PropertyDescriptor.Builder()
        .name("spark.master")
        .description("The url to Spark Master")
        .required(true)
        // The regex allows "local[K]" with K as an integer,  "local[*]", "yarn", "yarn-client", "yarn-cluster" and "spark://HOST[:PORT]"
        // there is NO support for "mesos://HOST:PORT"
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^(yarn(-(client|cluster))?|local\\[[0-9\\*]+\\]|spark:\\/\\/([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+|[a-z][a-z0-9\\.\\-]+)(:[0-9]+)?)$")))
        .defaultValue("local[2]")
        .build

    val SPARK_APP_NAME = new PropertyDescriptor.Builder()
        .name("spark.app.name")
        .description("Tha application name")
        .required(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[a-zA-z0-9-_\\.]+$")))
        .defaultValue("log-island")
        .build

    val SPARK_STREAMING_BATCH_DURATION = new PropertyDescriptor.Builder()
        .name("spark.streaming.batchDuration")
        .description("")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("2000")
        .build

    val SPARK_YARN_DEPLOYMODE = new PropertyDescriptor.Builder()
        .name("spark.yarn.deploy-mode")
        .description("The yarn deploy mode")
        .required(false)
        // .allowableValues("client", "cluster")
        .build

    val SPARK_YARN_QUEUE = new PropertyDescriptor.Builder()
        .name("spark.yarn.queue")
        .description("The name of the YARN queue")
        .required(false)
        //   .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("default")
        .build

    val memorySizePattern = Pattern.compile("^[0-9]+[mMgG]$");
    val SPARK_DRIVER_MEMORY = new PropertyDescriptor.Builder()
        .name("spark.driver.memory")
        .description("The memory size for Spark driver")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(memorySizePattern))
        .defaultValue("512m")
        .build

    val SPARK_EXECUTOR_MEMORY = new PropertyDescriptor.Builder()
        .name("spark.executor.memory")
        .description("The memory size for Spark executors")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(memorySizePattern))
        .defaultValue("1g")
        .build

    val SPARK_DRIVER_CORES = new PropertyDescriptor.Builder()
        .name("spark.driver.cores")
        .description("The number of cores for Spark driver")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("4")
        .build

    val SPARK_EXECUTOR_CORES = new PropertyDescriptor.Builder()
        .name("spark.executor.cores")
        .description("The number of cores for Spark driver")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .build

    val SPARK_EXECUTOR_INSTANCES = new PropertyDescriptor.Builder()
        .name("spark.executor.instances")
        .description("The number of instances for Spark app")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build

    val SPARK_SERIALIZER = new PropertyDescriptor.Builder()
        .name("spark.serializer")
        .description("Class to use for serializing objects that will be sent over the network " +
            "or need to be cached in serialized form")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("org.apache.spark.serializer.KryoSerializer")
        .build

    val SPARK_STREAMING_BLOCK_INTERVAL = new PropertyDescriptor.Builder()
        .name("spark.streaming.blockInterval")
        .description("Interval at which data received by Spark Streaming receivers is chunked into blocks " +
            "of data before storing them in Spark. Minimum recommended - 50 ms")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("350")
        .build

    val SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION = new PropertyDescriptor.Builder()
        .name("spark.streaming.kafka.maxRatePerPartition")
        .description("Maximum rate (number of records per second) at which data will be read from each Kafka partition")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("5000")
        .build

    val SPARK_STREAMING_BACKPRESSURE_ENABLED = new PropertyDescriptor.Builder()
        .name("spark.streaming.backpressure.enabled")
        .description("This enables the Spark Streaming to control the receiving rate based on " +
            "the current batch scheduling delays and processing times so that the system " +
            "receives only as fast as the system can process.")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("true")
        .build

    val SPARK_STREAMING_UNPERSIST = new PropertyDescriptor.Builder()
        .name("spark.streaming.unpersist")
        .description("Force RDDs generated and persisted by Spark Streaming to be automatically unpersisted " +
            "from Spark's memory. The raw input data received by Spark Streaming is also automatically cleared." +
            " Setting this to false will allow the raw data and persisted RDDs to be accessible outside " +
            "the streaming application as they will not be cleared automatically. " +
            "But it comes at the cost of higher memory usage in Spark.")
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

    val SPARK_STREAMING_KAFKA_MAXRETRIES = new PropertyDescriptor.Builder()
        .name("spark.streaming.kafka.maxRetries")
        .description("Maximum rate (number of records per second) at which data will be read from each Kafka partition")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("3")
        .build

    val SPARK_STREAMING_UI_RETAINED_BATCHES = new PropertyDescriptor.Builder()
        .name("spark.streaming.ui.retainedBatches")
        .description("How many batches the Spark Streaming UI and status APIs remember before garbage collecting.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("200")
        .build

    val SPARK_STREAMING_RECEIVER_WAL_ENABLE = new PropertyDescriptor.Builder()
        .name("spark.streaming.receiver.writeAheadLog.enable")
        .description("Enable write ahead logs for receivers. " +
            "All the input data received through receivers will be saved to write ahead logs " +
            "that will allow it to be recovered after driver failures.")
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build

    val SPARK_STREAMING_CHECKPOINT_DIRECTORY = new PropertyDescriptor.Builder()
        .name("spark.streaming.checkpoint.directory")
        .description("Checkpointing can be enabled by setting a directory in a fault-tolerant, " +
            "reliable file system (e.g., HDFS, S3, etc.) to which the checkpoint information will be saved. " +
            "This will allow you to use the aforementioned stateful transformations")
        .required(false)
        .defaultValue("checkpoints")
        .build
}

class SparkStreamProcessingEngine extends AbstractStreamProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[SparkStreamProcessingEngine])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(SparkStreamProcessingEngine.SPARK_APP_NAME)
        descriptors.add(SparkStreamProcessingEngine.SPARK_MASTER)
        descriptors.add(SparkStreamProcessingEngine.SPARK_YARN_DEPLOYMODE)
        descriptors.add(SparkStreamProcessingEngine.SPARK_YARN_QUEUE)
        descriptors.add(SparkStreamProcessingEngine.SPARK_DRIVER_MEMORY)
        descriptors.add(SparkStreamProcessingEngine.SPARK_EXECUTOR_MEMORY)
        descriptors.add(SparkStreamProcessingEngine.SPARK_DRIVER_CORES)
        descriptors.add(SparkStreamProcessingEngine.SPARK_EXECUTOR_CORES)
        descriptors.add(SparkStreamProcessingEngine.SPARK_EXECUTOR_INSTANCES)
        descriptors.add(SparkStreamProcessingEngine.SPARK_SERIALIZER)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_BACKPRESSURE_ENABLED)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_UNPERSIST)
        descriptors.add(SparkStreamProcessingEngine.SPARK_UI_PORT)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_UI_RETAINED_BATCHES)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_RECEIVER_WAL_ENABLE)
        descriptors.add(SparkStreamProcessingEngine.SPARK_STREAMING_CHECKPOINT_DIRECTORY)
        Collections.unmodifiableList(descriptors)
    }


    def setConfProperty(conf: SparkConf, engineContext: EngineContext, propertyDescriptor: PropertyDescriptor) = {

        // Need to check if the properties are set because those properties are not "requires"
        if (engineContext.getProperty(propertyDescriptor).isSet) {
            conf.set(propertyDescriptor.getName, engineContext.getProperty(propertyDescriptor).asString)
        }
    }

    /**
      * start the engine
      *
      * @param engineContext
      */
    override def start(engineContext: EngineContext) = {
        logger.info("starting Spark Engine")
        val timeout = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT).asInteger().intValue()
        val checkpointDirectory = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_CHECKPOINT_DIRECTORY).asString
        val context = StreamingContext.getOrCreate(checkpointDirectory, () => createContext(engineContext))
        context.start()

        if (timeout != -1) context.awaitTerminationOrTimeout(timeout)
        else context.awaitTermination()

        logger.info("stream processing done")
    }


    def createContext(engineContext: EngineContext): StreamingContext = {

        val sparkMaster = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_MASTER).asString
        val appName = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_APP_NAME).asString
        val batchDuration = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
        val timeout = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT).asInteger().intValue()
        val maxRatePerPartition = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION).asInteger().intValue()
        val blockInterval = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL).asInteger().intValue()
        val checkpointDirectory = engineContext.getProperty(SparkStreamProcessingEngine.SPARK_STREAMING_CHECKPOINT_DIRECTORY).asString


        /**
          * job configuration
          */
        val conf = new SparkConf()

        conf.setAppName(engineContext.getProperty(SparkStreamProcessingEngine.SPARK_MASTER).asString)
        conf.setMaster(engineContext.getProperty(SparkStreamProcessingEngine.SPARK_MASTER).asString)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_UI_RETAINED_BATCHES)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_RECEIVER_WAL_ENABLE)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_UI_PORT)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_UNPERSIST)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_BACKPRESSURE_ENABLED)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_SERIALIZER)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_DRIVER_MEMORY)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_EXECUTOR_MEMORY)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_DRIVER_CORES)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_EXECUTOR_CORES)
        setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_EXECUTOR_INSTANCES)

        if (sparkMaster startsWith "yarn") {
            // Note that SPARK_YARN_DEPLOYMODE is not used by spark itself but only by spark-submit CLI
            // That's why we do not need to propagate it here
            setConfProperty(conf, engineContext, SparkStreamProcessingEngine.SPARK_YARN_QUEUE)
        }

        SparkUtils.customizeLogLevels
        @transient val sc = new SparkContext(conf)
        @transient val context = new StreamingContext(sc, Milliseconds(batchDuration))
        context.checkpoint(checkpointDirectory) // set checkpoint directory
        logger.info(s"spark context initialized with master:$sparkMaster, " +
            s"appName:$appName, " +
            s"batchDuration:$batchDuration ")
        logger.info(s"conf : ${conf.toDebugString}")


        /**
          * loop over processContext
          */
        engineContext.getProcessorChainInstances.foreach(processorChainInstance => {
            try {
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

                val kafkaSinkParams = Map(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
                    ProducerConfig.CLIENT_ID_CONFIG -> appName,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getCanonicalName,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
                    ProducerConfig.ACKS_CONFIG -> "1",
                    ProducerConfig.BATCH_SIZE_CONFIG -> "5000",
                    ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "1000",
                    ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "1000")


                val kafkaStreamsParams = Map(
                    "metadata.broker.list" -> brokerList,
                    "bootstrap.servers" -> brokerList,
                    "group.id" -> appName,
                    "refresh.leader.backoff.ms" -> "1000")

                @transient val kafkaSink = context.sparkContext.broadcast(KafkaSink(kafkaSinkParams))

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
                @transient val kafkaStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
                    context,
                    kafkaStreamsParams,
                    inputTopics
                )

                // setup the stream processing
                kafkaStream.foreachRDD(rdd => {
                    // Cast the rdd to an interface that lets us get an array of OffsetRange
                    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

                    rdd.foreachPartition(partition => {
                        SparkUtils.customizeLogLevels
                        if (partition.nonEmpty) {

                            val partitionId = TaskContext.get.partitionId()

                            /**
                              * create serializers
                              */
                            val deserializer = getSerializer(inSerializerClass,
                                processorChainContext.getProperty(KafkaRecordStream.AVRO_INPUT_SCHEMA).asString)
                            val serializer = getSerializer(inSerializerClass,
                                processorChainContext.getProperty(KafkaRecordStream.AVRO_OUTPUT_SCHEMA).asString)


                            /**
                              * process events by chaining output records
                              */
                            var isFirstProcessor = true
                            var incomingEvents: util.Collection[Record] = null
                            var outgoingEvents: util.Collection[Record] = null
                            var processingMetrics: util.Collection[Record] = new util.ArrayList[Record]()

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
                                            val key = if (rawMessage._1 != null) new String(rawMessage._1) else ""
                                            val value = if (rawMessage._2 != null) new String(rawMessage._2) else ""
                                            RecordUtils.getKeyValueRecord(key, value)
                                        }).toList
                                    } else {
                                        // processor
                                        deserializeEvents(partition, deserializer)
                                    }

                                    isFirstProcessor = false
                                } else {
                                    incomingEvents = outgoingEvents
                                }

                                /**
                                  * process incoming events
                                  */
                                outgoingEvents = processor.process(processorContext, incomingEvents)

                                /**
                                  * send metrics if requested
                                  */
                                processingMetrics.addAll(
                                    computeMetrics(maxRatePerPartition, appName, blockInterval, batchDuration, brokerList,
                                        processorChainContext, inputTopics, outputTopics, partition, startTime, partitionId,
                                        serializer, incomingEvents, outgoingEvents))

                            })


                            /**
                              * push outgoing events and errors to Kafka
                              */
                            kafkaSink.value.produce(
                                processorChainContext.getProperty(KafkaRecordStream.OUTPUT_TOPICS).asString,
                                outgoingEvents.toList,
                                serializer)

                        /*    kafkaSink.value.produce(
                                processorChainContext.getProperty(KafkaRecordStream.ERROR_TOPICS).asString,
                                outgoingEvents.filter(r => r.hasField(FieldDictionary.RECORD_ERROR)).toList,
                                serializer)

                            kafkaSink.value.produce(
                                processorChainContext.getProperty(KafkaRecordStream.METRICS_TOPIC).asString,
                                processingMetrics.toList,
                                serializer)
*/



                            // index to get the correct offset range for the rdd partition we're working on
                            // This is safe because we haven't shuffled or otherwise disrupted partitioning,
                            // and the original input rdd partitions were 1:1 with kafka partitions
                            val osr: OffsetRange = offsetRanges(TaskContext.get.partitionId)
                            logger.info(s"offset ${osr.topic} ${osr.partition} ${osr.fromOffset} -> ${osr.untilOffset}")
                        }
                    })
                })
            } catch {
                case ex: Exception => logger.error("something bad happened, please check Kafka or cluster health : {}",
                    ex.getMessage)
            }

        })
        context
    }


    def getSerializer(inSerializerClass:String, schemaContent:String):RecordSerializer = {
        inSerializerClass match {
            case c if c == KafkaRecordStream.AVRO_SERIALIZER.getValue =>
                val parser = new Parser
                val inSchema = parser.parse(schemaContent)
                new AvroSerializer(inSchema)
            case c if c == KafkaRecordStream.JSON_SERIALIZER.getValue => new JsonSerializer()
            case _ => new KryoSerializer(true)
        }
    }

    def computeMetrics(maxRatePerPartition: Int,
                       appName: String,
                       blockInterval: Int,
                       batchDuration: Int,
                       brokerList: String,
                       processorChainContext: StandardProcessContext,
                       inputTopics: Set[String],
                       outputTopics: Set[String],
                       partition: Iterator[(Array[Byte], Array[Byte])],
                       startTime: Long,
                       partitionId: Int,
                       serializer: RecordSerializer,
                       incomingEvents: util.Collection[Record],
                       outgoingEvents: util.Collection[Record]): util.Collection[Record] = {

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

        ProcessorMetrics.computeMetrics(incomingEvents, processorFields, System.currentTimeMillis() - startTime).toList

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
