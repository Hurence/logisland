package com.hurence.logisland.engine.spark

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util
import java.util.Collections
import java.util.regex.Pattern

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.{AbstractProcessingEngine, EngineContext}
import com.hurence.logisland.processor.StandardProcessContext
import com.hurence.logisland.stream.spark.ParallelProcessingKafkaStream
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._


object KafkaStreamProcessingEngine {

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

}

class KafkaStreamProcessingEngine extends AbstractProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[AbstractSparkStreamProcessingEngine])


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_APP_NAME)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_MASTER)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_YARN_DEPLOYMODE)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_YARN_QUEUE)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_DRIVER_MEMORY)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_EXECUTOR_MEMORY)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_DRIVER_CORES)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_EXECUTOR_CORES)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_EXECUTOR_INSTANCES)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_SERIALIZER)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_BACKPRESSURE_ENABLED)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_UNPERSIST)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_UI_PORT)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_UI_RETAINED_BATCHES)
        descriptors.add(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_RECEIVER_WAL_ENABLE)
        Collections.unmodifiableList(descriptors)
    }


    /**
      * start the engine
      *
      * @param engineContext
      */
    override def start(engineContext: EngineContext) = {
        logger.info("starting Spark Engine")
        val timeout = engineContext.getProperty(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_TIMEOUT).asInteger().intValue()
        val context = createStreamingContext(engineContext)

        /**
          * shutdown context gracefully
          */
        sys.ShutdownHookThread {
            logger.info("Gracefully stopping Spark Streaming Application")
            context.stop(stopSparkContext = true, stopGracefully = true)
            logger.info("Application stopped")
        }

        context.start()

        if (timeout != -1) context.awaitTerminationOrTimeout(timeout)
        else context.awaitTermination()

        logger.info("stream processing done")
    }


    def createStreamingContext(engineContext: EngineContext): StreamingContext = {

        val sparkMaster = engineContext.getProperty(AbstractSparkStreamProcessingEngine.SPARK_MASTER).asString
        val appName = engineContext.getProperty(AbstractSparkStreamProcessingEngine.SPARK_APP_NAME).asString
        val batchDuration = engineContext.getProperty(AbstractSparkStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION).asInteger().intValue()

        /**
          * job configuration
          */
        val conf = new SparkConf()

        conf.setAppName(appName)
        conf.setMaster(sparkMaster)
        def setConfProperty(conf: SparkConf, engineContext: EngineContext, propertyDescriptor: PropertyDescriptor) = {

            // Need to check if the properties are set because those properties are not "requires"
            if (engineContext.getProperty(propertyDescriptor).isSet) {
                conf.set(propertyDescriptor.getName, engineContext.getProperty(propertyDescriptor).asString)
            }
        }

        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_UI_RETAINED_BATCHES)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_RECEIVER_WAL_ENABLE)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAXRETRIES)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_UI_PORT)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_UNPERSIST)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_BACKPRESSURE_ENABLED)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_BLOCK_INTERVAL)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_SERIALIZER)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_DRIVER_MEMORY)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_EXECUTOR_MEMORY)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_DRIVER_CORES)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_EXECUTOR_CORES)
        setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_EXECUTOR_INSTANCES)

        if (sparkMaster startsWith "yarn") {
            // Note that SPARK_YARN_DEPLOYMODE is not used by spark itself but only by spark-submit CLI
            // That's why we do not need to propagate it here
            setConfProperty(conf, engineContext, AbstractSparkStreamProcessingEngine.SPARK_YARN_QUEUE)
        }

        SparkUtils.customizeLogLevels
        @transient val sc = new SparkContext(conf)
        @transient val ssc = new StreamingContext(sc, Milliseconds(batchDuration))

        logger.info(s"spark context initialized with master:$sparkMaster, " +
            s"appName:$appName, " +
            s"batchDuration:$batchDuration ")
        logger.info(s"conf : ${conf.toDebugString}")


        /**
          * loop over processContext
          */
        engineContext.getProcessorChainInstances.foreach(processorChainInstance => {
            try {

                val kafkaStream = new ParallelProcessingKafkaStream(appName, ssc, processorChainInstance)
                kafkaStream.start()
            } catch {
                case ex: Exception => logger.error("something bad happened, please check Kafka or cluster health : {}",
                    ex.getMessage)
            }

        })
        ssc
    }


    override def shutdown(context: EngineContext) = {
        logger.info(s"shuting down Spark engine")
    }

    override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String) = {
        logger.info(s"property ${
            descriptor.getName
        } value changed from $oldValue to $newValue")
    }

}


