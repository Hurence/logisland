/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
  * Copyright (C) 2016 Hurence (support@hurence.com)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.hurence.logisland.engine.spark

import java.util
import java.util.concurrent.Executors
import java.util.regex.Pattern
import java.util.{Collections, Map, UUID}
import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine.SPARK_CUSTOM_CONFIG_PREFIX
import com.hurence.logisland.engine.spark.remote.PipelineConfigurationBroadcastWrapper
import com.hurence.logisland.engine.{AbstractProcessingEngine, EngineContext}
import com.hurence.logisland.stream.spark.{SparkRecordStream, SparkStreamContext}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


object KafkaStreamProcessingEngine {

    val SPARK_PROPERTIES_FILE_PATH: PropertyDescriptor = new PropertyDescriptor.Builder()//Not used in code but in logisland.sh script. Si it must be present !
      .name("spark.properties.file.path")
      .description("for using --properties-file option while submitting spark job")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

    val SPARK_MONITORING_DRIVER_PORT: PropertyDescriptor = new PropertyDescriptor.Builder()//Not used in code but in logisland.sh script. Si it must be present !
        .name("spark.monitoring.driver.port")
        .description("The port for exposing monitoring metrics")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
        .build

    val SPARK_MASTER = new PropertyDescriptor.Builder()
        .name("spark.master")
        .description("The url to Spark Master")
        .required(false)
        // The regex allows "local[K]" with K as an integer,  "local[*]", "yarn", "yarn-client", "yarn-cluster" and "spark://HOST[:PORT]"
        // there is NO support for "mesos://HOST:PORT"
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile(
        "^(yarn|" +
          "local(\\[([0-9]+|\\*)(,[0-9]+)?\\])?|" +
          "spark:\\/\\/[a-z0-9\\.\\-]+(:[0-9]+)?(,[a-z0-9\\.\\-]+(:[0-9]+)?)*|" +
          "mesos:\\/\\/((zk:\\/\\/[a-z0-9\\.\\-]+:[0-9]+(,[a-z0-9\\.\\-]+:[0-9]+)*\\/mesos)|(([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+|[a-z][a-z0-9\\.\\-]+)(:[0-9]+)?))|" +
          "k8s:\\/\\/.+)$")))
        .defaultValue("local[2]")
        .build

    val SPARK_APP_NAME = new PropertyDescriptor.Builder()
        .name("spark.app.name")
        .description("Tha application name")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[a-zA-z0-9-_\\.]+$")))
        .defaultValue("logisland")
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
        .allowableValues("client", "cluster")
        .build

    val SPARK_DEPLOYMODE = new PropertyDescriptor.Builder()
        .name("spark.deploy-mode")
        .description("The spark standalone cluster deploy mode")
        .required(false)
        .allowableValues("client", "cluster")
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
        .defaultValue("false")
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


    val SPARK_YARN_MAX_APP_ATTEMPTS = new PropertyDescriptor.Builder()
        .name("spark.yarn.maxAppAttempts")
        .description("Because Spark driver and Application Master share a single JVM," +
            " any error in Spark driver stops our long-running job. " +
            "Fortunately it is possible to configure maximum number of attempts " +
            "that will be made to re-run the application. " +
            "It is reasonable to set higher value than default 2 " +
            "(derived from YARN cluster property yarn.resourcemanager.am.max-attempts). " +
            "4 works quite well, higher value may cause unnecessary restarts" +
            " even if the reason of the failure is permanent.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("4")
        .build

    val SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL = new PropertyDescriptor.Builder()
        .name("spark.yarn.am.attemptFailuresValidityInterval")
        .description("If the application runs for days or weeks without restart " +
            "or redeployment on highly utilized cluster, " +
            "4 attempts could be exhausted in few hours. " +
            "To avoid this situation, the attempt counter should be reset on every hour of so.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("1h")
        .build

    val SPARK_YARN_MAX_EXECUTOR_FAILURES = new PropertyDescriptor.Builder()
        .name("spark.yarn.max.executor.failures")
        .description("a maximum number of executor failures before the application fails. " +
            "By default it is max(2 * num executors, 3), " +
            "well suited for batch jobs but not for long-running jobs." +
            " The property comes with corresponding validity interval which also should be set." +
            "8 * num_executors")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("20")
        .build


    val SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL = new PropertyDescriptor.Builder()
        .name("spark.yarn.executor.failuresValidityInterval")
        .description("If the application runs for days or weeks without restart " +
            "or redeployment on highly utilized cluster, " +
            "x attempts could be exhausted in few hours. " +
            "To avoid this situation, the attempt counter should be reset on every hour of so.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("1h")
        .build

    val SPARK_TASK_MAX_FAILURES = new PropertyDescriptor.Builder()
        .name("spark.task.maxFailures")
        .description("For long-running jobs you could also consider to boost maximum" +
            " number of task failures before giving up the job. " +
            "By default tasks will be retried 4 times and then job fails.")
        .required(false)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .defaultValue("8")
        .build

    val SPARK_MEMORY_STORAGE_FRACTION = new PropertyDescriptor.Builder()
        .name("spark.memory.storageFraction")
        .description("expresses the size of R as a fraction of M (default 0.5). " +
            "R is the storage space within M where cached blocks immune to being evicted by execution.")
        .required(false)
        .addValidator(StandardValidators.FLOAT_VALIDATOR)
        .defaultValue("0.5")
        .build

    val SPARK_MEMORY_FRACTION = new PropertyDescriptor.Builder()
        .name("spark.memory.fraction")
        .description("expresses the size of M as a fraction of the (JVM heap space - 300MB) (default 0.75). " +
            "The rest of the space (25%) is reserved for user data structures, internal metadata in Spark, " +
            "and safeguarding against OOM errors in the case of sparse and unusually large records.")
        .required(false)
        .addValidator(StandardValidators.FLOAT_VALIDATOR)
        .defaultValue("0.6")
        .build

    val FAIR = new AllowableValue("FAIR", "FAIR", "fair sharing")
    val FIFO = new AllowableValue("FIFO", "FIFO", "queueing jobs one after another")

    val SPARK_SCHEDULER_MODE = new PropertyDescriptor.Builder()
        .name("spark.scheduler.mode")
        .description("The scheduling mode between jobs submitted to the same SparkContext. " +
            "Can be set to FAIR to use fair sharing instead of queueing jobs one after another. " +
            "Useful for multi-user services.")
        .required(false)
        .allowableValues(FAIR, FIFO)
        .defaultValue(FAIR.getValue)
        .build

    val JAVA_MESOS_LIBRARY_PATH = new PropertyDescriptor.Builder()
      .name("java.library.path")
      .description("The java library path to use with mesos.")
      .required(false)
      .build

    val SPARK_MESOS_CORE_MAX = new PropertyDescriptor.Builder()
      .name("spark.cores.max")
      .description("The maximum number of total executor core with mesos.")
      .required(false)
      .build
    //SPARK STANDALONE CLUSTER OPTIONS
    val SPARK_TOTAL_EXECUTOR_CORES = new PropertyDescriptor.Builder()
      .name("spark.total.executor.cores")
      .description("The total number of core of all executors")
      .required(false)
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .build

    val SPARK_SUPERVISE = new PropertyDescriptor.Builder()
      .name("spark.supervise")
      .description("activate supervise option or not of spark standalone cluster (see documentation)")
      .required(false)
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .build

    val OVERWRITE_EXISTING = new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist")

    val KEEP_OLD_FIELD = new AllowableValue("keep_old_field", "keep only old field value", "keep only old field")

    val SPARK_CONF_POLICY = new PropertyDescriptor.Builder()
            .name("spark.conf.properties.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(OVERWRITE_EXISTING.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();

    val SPARK_CUSTOM_CONFIG_PREFIX = "spark.custom.config."
}

class KafkaStreamProcessingEngine extends AbstractProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[KafkaStreamProcessingEngine])
    private val conf = new SparkConf()
    private var running = false
    protected var batchDurationMs: Int = 1000
    protected var controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink] = null
    private var sparkStreamingTimeout: Int = _

    /**
      * Provides subclasses the ability to perform initialization logic
      */
    override def init(context: EngineContext): Unit  = {
        super.init(context)
        val engineContext = context.asInstanceOf[EngineContext]
        batchDurationMs = engineContext.getPropertyValue(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()
        sparkStreamingTimeout = engineContext.getPropertyValue(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT).asInteger().toInt
        val conflictPolicy = engineContext.getPropertyValue(KafkaStreamProcessingEngine.SPARK_CONF_POLICY).asString

        def setConfProperty(conf: SparkConf, engineContext: EngineContext, propertyDescriptor: PropertyDescriptor) = {
            // Need to check if the properties are set because those properties are not "requires"
            if (engineContext.getPropertyValue(propertyDescriptor).isSet) {
                if (conf.contains(propertyDescriptor.getName)) {
                    logger.warn("Property " + propertyDescriptor.getName + " already present in the sparkConfiguration with value " + conf.get(propertyDescriptor.getName))
                }

                if (conflictPolicy.equals(KafkaStreamProcessingEngine.OVERWRITE_EXISTING.getValue)) {
                    conf.set(propertyDescriptor.getName, engineContext.getPropertyValue(propertyDescriptor).asString)
                    logger.info("Property " + propertyDescriptor.getName + " set in the sparkConfiguration with value " + conf.get(propertyDescriptor.getName))
                }
            }
        }

        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_APP_NAME)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_MASTER)

        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_UI_RETAINED_BATCHES)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_RECEIVER_WAL_ENABLE)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_UI_PORT)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_UNPERSIST)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_BACKPRESSURE_ENABLED)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_SERIALIZER)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_DRIVER_MEMORY)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_EXECUTOR_MEMORY)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_DRIVER_CORES)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_EXECUTOR_CORES)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_EXECUTOR_INSTANCES)

        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_YARN_MAX_APP_ATTEMPTS)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_YARN_MAX_EXECUTOR_FAILURES)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_TASK_MAX_FAILURES)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_MEMORY_FRACTION)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_MEMORY_STORAGE_FRACTION)
        setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_SCHEDULER_MODE)

        conf.set("spark.kryo.registrator", "com.hurence.logisland.util.spark.ProtoBufRegistrator")

        if (conf.get(KafkaStreamProcessingEngine.SPARK_MASTER.getName) startsWith "yarn") {
            // Note that SPARK_YARN_DEPLOYMODE is not used by spark itself but only by spark-submit CLI
            // That's why we do not need to propagate it here
            setConfProperty(conf, engineContext, KafkaStreamProcessingEngine.SPARK_YARN_QUEUE)
        }

        handleDynamicProperties(context.getProperties)

        logger.info("Configuration from logisland main")
        logger.info(conf.toDebugString)
        
        val sparkContext = getCurrentSparkContext()

        UserMetricsSystem.initialize(sparkContext, "LogislandMetrics")

        /**
          * shutdown context gracefully
          */
        sys.ShutdownHookThread {
            logger.info("Gracefully stopping Spark Streaming Application")
            stop(engineContext)
            logger.info("Application stopped")
        }

        PipelineConfigurationBroadcastWrapper.getInstance().refresh(engineContext, sparkContext)

        SQLContext.getOrCreate(getCurrentSparkContext()).streams.addListener(new StreamingQueryListener {

            val runMap = scala.collection.mutable.Map[UUID, String]()
            val executor = Executors.newSingleThreadExecutor()
            //force early initialization of this pool
            executor.submit(new Runnable {
                override def run(): Unit = {}
            })

            override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
                logger.info(s"Streaming query for stream ${event.name} has been started")
                runMap.put(event.id, event.name)
            }

            override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
            }

            override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
                if (event.exception.isDefined && !getCurrentSparkContext().isStopped) {
                    val currentStreamId = runMap.get(event.id)
                    logger.warn(s"Streaming query for stream $currentStreamId terminated with exception ${event.exception}. " +
                        s"The engine will be reset")

                    executor.submit(new Runnable {
                        override def run(): Unit = {
                            Thread.sleep(1000);
                            engineContext.getEngine.stop(engineContext)
                        }
                    })
                }
            }
        })

        val sparkMaster = conf.get(KafkaStreamProcessingEngine.SPARK_MASTER.getName)
        val appName = conf.get(KafkaStreamProcessingEngine.SPARK_APP_NAME.getName)
        logger.info(s"spark context initialized with master:$sparkMaster, " +
            s"appName:$appName, " +
            s"batchDuration:$batchDurationMs ")
        logger.info(s"conf : ${conf.toDebugString}")
    }

    protected def handleDynamicProperties(properties : Map[PropertyDescriptor, String]) = {

      properties.foreach(propertyAndValue => {

        val propertyDescriptor: PropertyDescriptor = propertyAndValue._1
        val propertyName: String = propertyDescriptor.getName
        // Handle any dynamic property of the form 'spark.custom.config.XXX: someValue' properties
        // which will be passed to spark configuration as property 'XXX=someValue'
        if (propertyDescriptor.isDynamic && propertyName.startsWith(SPARK_CUSTOM_CONFIG_PREFIX)) {
          val customSparkConfigKey: String = propertyName.substring(SPARK_CUSTOM_CONFIG_PREFIX.length)
          if (customSparkConfigKey.length > 0) { // Ignore silly 'spark.custom.config.: missing_custom_key' property
            val customSparkConfigValue: String = propertyAndValue._2
            conf.set(customSparkConfigKey, customSparkConfigValue)
          }
        }
      })
    }

    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(KafkaStreamProcessingEngine.SPARK_APP_NAME)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_MASTER)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_MONITORING_DRIVER_PORT)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_YARN_DEPLOYMODE)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_DEPLOYMODE)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_YARN_QUEUE)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_DRIVER_MEMORY)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_EXECUTOR_MEMORY)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_DRIVER_CORES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_EXECUTOR_CORES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_EXECUTOR_INSTANCES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_SERIALIZER)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_BACKPRESSURE_ENABLED)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_UNPERSIST)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_UI_PORT)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_UI_RETAINED_BATCHES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_STREAMING_RECEIVER_WAL_ENABLE)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_YARN_MAX_APP_ATTEMPTS)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_YARN_MAX_EXECUTOR_FAILURES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_TASK_MAX_FAILURES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_MEMORY_FRACTION)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_MEMORY_STORAGE_FRACTION)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_SCHEDULER_MODE)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_PROPERTIES_FILE_PATH)
        descriptors.add(KafkaStreamProcessingEngine.JAVA_MESOS_LIBRARY_PATH)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_MESOS_CORE_MAX)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_TOTAL_EXECUTOR_CORES)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_SUPERVISE)
        descriptors.add(KafkaStreamProcessingEngine.SPARK_CONF_POLICY)

        Collections.unmodifiableList(descriptors)
    }

    override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {

      // Support any custom spark configuration.
      // The property name must start with SPARK_CUSTOM_CONFIG_PREFIX, the rest
      // of the key will be the real key used to pass to spark configuration
      if (propertyDescriptorName.startsWith(SPARK_CUSTOM_CONFIG_PREFIX)) {
        return new PropertyDescriptor.Builder()
          .name(propertyDescriptorName)
          .expressionLanguageSupported(false)
          .required(false)
          .dynamic(true)
          .build
      }
      return null
    }


    /**
      * start the engine
      *
      * @param engineContext
      */
    override def start(engineContext: EngineContext) = {
        logger.info("starting Spark Engine")
        val spark = getCurrentSparkSession()
        logger.info("broadCasting services")
        controllerServiceLookupSink = spark.sparkContext.broadcast(
            ControllerServiceLookupSink(engineContext.getControllerServiceConfigurations)
        )
        getLogger.info("Will start streams")
        /**
          * loop over processContext
          */
        engineContext.getStreamContexts.foreach(logislandStreamContext => {
            try {
                val kafkaStream = logislandStreamContext.getStream.asInstanceOf[SparkRecordStream]
                val sparkStreamContext = new SparkStreamContext(
                    logislandStreamContext,
                    batchDurationMs,
                    spark,
                    controllerServiceLookupSink
                )
                kafkaStream.init(sparkStreamContext)
                kafkaStream.start()
            } catch {
                case ex: Exception =>
                    throw new IllegalStateException("something bad happened, please check Kafka or cluster health", ex)
            }
        })

        if (!engineContext.getStreamContexts.isEmpty) {
            val streamsIds = engineContext.getStreamContexts
              .map(a => a.getIdentifier).toList;
            logger.info("started all following streams : {}", streamsIds)
        } else {
            logger.error("There is no stream to start ! This should never happen as configuration should be considered" +
            " wrong in this case, therefore code should never reach this code");
        }
        running = true
    }

    protected def getCurrentSparkStreamingContext(sparkContext: SparkContext): StreamingContext = {
        StreamingContext.getActiveOrCreate(() =>
            return new StreamingContext(getCurrentSparkContext(), Milliseconds(batchDurationMs))
        )
    }

    protected def getCurrentSparkContext(): SparkContext = {
        getCurrentSparkSession().sparkContext
    }

    protected def getCurrentSparkSession(): SparkSession = {
        SparkSession.builder()
          .config(conf)
          .getOrCreate()
    }


    override def stop(engineContext: EngineContext) = {
        running = false
        logger.info(s"stopping Spark engine")
        doStop(engineContext, true)
    }

    override def softStop(engineContext: EngineContext): Unit = {
        running = false
        logger.info(s"stopping Spark engine")
        doStop(engineContext, false)
    }

    protected def doStop(engineContext: EngineContext, doStopSparkContext: Boolean) = {
        synchronized {
            val sc = getCurrentSparkContext();
            if (!sc.isStopped) {

                engineContext.getStreamContexts.foreach(streamingContext => {
                    try {
                        val kafkaStream = streamingContext.getStream.asInstanceOf[SparkRecordStream]
                        kafkaStream.stop()
                    } catch {
                        case ex: Exception =>
                            logger.error("something bad happened, please check Kafka or cluster health : {}", ex.getMessage)
                    }
                })

                try {
                    if (!sc.isStopped) {
                        val ssc = getCurrentSparkStreamingContext(sc);
                        ssc.stop(stopSparkContext = false, stopGracefully = true)
                    }

                } finally {
                    if (doStopSparkContext && !sc.isStopped) {
                        try {
                            sc.stop();
                        } catch {
                            case ex: Exception =>
                                logger.error("something bad while stopping the spark context. Please check cluster health : {}", ex.getMessage)
                        }
                    }
                }

            }
        }
    }

    override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String) = {
        logger.info(s"property ${
            descriptor.getName
        } value changed from $oldValue to $newValue")
    }

    /**
      * Await for termination.
      *
      */
    override def awaitTermination(engineContext: EngineContext): Unit = {
        val sc = getCurrentSparkContext()

        while (!sc.isStopped) {
            try {
                if (sparkStreamingTimeout < 0) {
                    Thread.sleep(200)
                } else {
                    val toSleep = Math.min(200, sparkStreamingTimeout);
                    Thread.sleep(toSleep)
                    sparkStreamingTimeout -= toSleep
                }
            } catch {
                case e: InterruptedException => return
                case unknown: Throwable => throw unknown
            }
        }
    }
}