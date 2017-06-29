/**
  * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import java.io.File
import java.util
import java.util.Collections
import java.util.regex.Pattern

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.engine.{EngineContext, ModelTrainingEngineImpl}
import com.hurence.logisland.model.{MLNModel, Model}
import com.hurence.logisland.serializer.{AvroSerializer, JsonSerializer, KryoSerializer}
import com.hurence.logisland.util.spark.SparkUtils
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.lossfunctions.LossFunctions
import org.slf4j.LoggerFactory

import scala.collection.mutable


object ModelTrainingProcessing {

  val AVRO_SERIALIZER = new AllowableValue(classOf[AvroSerializer].getName, "avro serialization", "serialize events as avro blocs")
  val JSON_SERIALIZER = new AllowableValue(classOf[JsonSerializer].getName, "avro serialization", "serialize events as json blocs")
  val KRYO_SERIALIZER = new AllowableValue(classOf[KryoSerializer].getName, "kryo serialization", "serialize events as json blocs")
  val NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes")



  val SPARK_EXECUTOR_EJO = new PropertyDescriptor.Builder()
    .name("spark.executor.extraJavaOptions")
    .description("executor extra java options")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val SPARK_DRIVER_EJO = new PropertyDescriptor.Builder()
    .name("spark.driver.extraJavaOptions")
    .description("driver extra java options")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

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
}

class ModelTrainingProcessing extends ModelTrainingEngineImpl {
  var engineContext: EngineContext = null

  protected var appName: String = ""
  @transient protected var jsc: JavaSparkContext = null

  private val logger = LoggerFactory.getLogger(ModelTrainingProcessing.getClass.getName)

  // TODO should be a parameter
  private val batchSizePerWorker: Int = 16

  // TODO should be a parameter
  private val numEpochs: Int = 1


  def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    Collections.unmodifiableList(descriptors)
  }

  def createJavaSparkContext(engineContext: EngineContext): JavaSparkContext = {

    val sparkMaster = engineContext.getPropertyValue(ModelTrainingProcessing.SPARK_MASTER).asString
    val appName = engineContext.getPropertyValue(ModelTrainingProcessing.SPARK_APP_NAME).asString

    /**
      * job configuration
      */
    val conf = new SparkConf()
    conf.setAppName(appName)
    conf.setMaster(sparkMaster)

    def setConfProperty(conf: SparkConf, engineContext: EngineContext, propertyDescriptor: PropertyDescriptor) = {

      // Need to check if the properties are set because those properties are not "requires"
      if (engineContext.getPropertyValue(propertyDescriptor).isSet) {
        conf.set(propertyDescriptor.getName, engineContext.getPropertyValue(propertyDescriptor).asString)
      }
    }

    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_UI_PORT)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_SERIALIZER)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_DRIVER_MEMORY)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_EXECUTOR_MEMORY)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_DRIVER_CORES)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_EXECUTOR_CORES)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_EXECUTOR_INSTANCES)

    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_YARN_MAX_APP_ATTEMPTS)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_YARN_AM_ATTEMPT_FAILURES_VALIDITY_INTERVAL)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_YARN_MAX_EXECUTOR_FAILURES)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_YARN_EXECUTOR_FAILURES_VALIDITY_INTERVAL)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_TASK_MAX_FAILURES)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_DRIVER_EJO)
    setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_EXECUTOR_EJO)

    if (sparkMaster startsWith "yarn") {
      // Note that SPARK_YARN_DEPLOYMODE is not used by spark itself but only by spark-submit CLI
      // That's why we do not need to propagate it here
      setConfProperty(conf, engineContext, ModelTrainingProcessing.SPARK_YARN_QUEUE)
    }

    SparkUtils.customizeLogLevels
    @transient val sc = new SparkContext(conf)
    @transient val jsc = new JavaSparkContext(sc)
    logger.info(s"spark context initialized with master:$sparkMaster, " +
      s"appName:$appName, ")
    logger.info(s"conf : ${conf.toDebugString}")

    jsc
  }


  def setup(appName: String, jsc: JavaSparkContext, engineContext: EngineContext) = {
    this.appName = appName
    this.jsc = jsc
    this.engineContext = engineContext
    SparkUtils.customizeLogLevels
  }

  def start(engineContext: EngineContext) = {
    jsc = createJavaSparkContext(engineContext)

    if (jsc == null)
      throw new IllegalStateException("JavaSparkContext not initialized")
    /**
      * shutdown context gracefully
      */



    sys.ShutdownHookThread {
      logger.info("Gracefully stopping Spark Streaming Application")
      jsc.stop()
      logger.info("Application stopped")
    }

  }

  def train(untrainedModel: Model): Model = {
    val iterTrain = new MnistDataSetIterator(batchSizePerWorker, true, 12345)
    val iterTest = new MnistDataSetIterator(batchSizePerWorker, true, 12345)
    val trainDataList = mutable.ArrayBuffer.empty[DataSet]
    val testDataList = mutable.ArrayBuffer.empty[DataSet]
    while (iterTrain.hasNext) {
      trainDataList += iterTrain.next
    }

    while (iterTest.hasNext) {
      testDataList += iterTest.next
    }

    val trainData = jsc.parallelize(trainDataList)
    val testData = jsc.parallelize(testDataList)


    //----------------------------------
    //Create network configuration and conduct network training
    // TODO: the conf should be defined as an higher level abstraction and not defined
    // in the processing code.

    val conf = new NeuralNetConfiguration.Builder()
      .seed(12345)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1)
      .activation(Activation.LEAKYRELU)
      .weightInit(WeightInit.XAVIER)
      .learningRate(0.02)
      .updater(Updater.NESTEROVS)
      .momentum(0.9)
      .regularization(true)
      .l2(1e-4)
      .list
      .layer(0, new DenseLayer.Builder().nIn(28 * 28).nOut(500).build)
      .layer(1, new DenseLayer.Builder().nIn(500).nOut(100).build)
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
        .activation(Activation.SOFTMAX).nIn(100).nOut(10).build)
      .pretrain(false).backprop(true)
      .build

    //Configuration for Spark training: see http://deeplearning4j.org/spark for explanation of these configuration options
    val tm = new ParameterAveragingTrainingMaster.Builder(batchSizePerWorker) //Each DataSet object: contains (by default) 32 examples
      .averagingFrequency(5)
      .workerPrefetchNumBatches(2) //Async prefetching: 2 examples per worker
      .batchSizePerWorker(batchSizePerWorker)
      .build

    //Create the Spark network
    val sparkNet = new SparkDl4jMultiLayer(jsc, conf, tm)

    //Execute training:
    var i: Int = 0

    for (i <- 0 until numEpochs) {
      sparkNet.fit(trainData)
      logger.info("Completed Epoch {}", i)
    }

    val  mln : MLNModel = new MLNModel()
    mln.setMLNModel(sparkNet.getNetwork())
    // TODO : hardcoded for now, need to be a parameter
    val locationToSave: File = new File("/tmp/MyComputationGraph.zip")
    ModelSerializer.writeModel(sparkNet.getNetwork, locationToSave, true)
    return mln
  }



  override def shutdown(engineContext: EngineContext) = {
    logger.info(s"shuting down Spark engine")
  }
}


