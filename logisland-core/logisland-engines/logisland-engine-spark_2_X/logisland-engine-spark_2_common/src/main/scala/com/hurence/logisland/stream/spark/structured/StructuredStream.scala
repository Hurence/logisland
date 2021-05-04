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
package com.hurence.logisland.stream.spark.structured

import java.util
import java.util.Collections
import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.spark.remote.PipelineConfigurationBroadcastWrapper
import com.hurence.logisland.record.Record
import com.hurence.logisland.runner.GlobalOptions
import com.hurence.logisland.stream.AbstractRecordStream
import com.hurence.logisland.stream.spark.structured.StructuredStream._
import com.hurence.logisland.stream.spark.structured.provider.{StructuredStreamProviderServiceReader, StructuredStreamProviderServiceWriter}
import com.hurence.logisland.stream.spark.{SparkRecordStream, SparkStreamContext}
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SQLContext}

class StructuredStream extends AbstractRecordStream with SparkRecordStream {

  @transient protected var sparkStreamContext: SparkStreamContext = _
  private var isReady = false
  private var groupByField: String = _
  @transient protected var outputMode: OutputMode = _


  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.add(READ_STREAM_SERVICE_PROVIDER)
    descriptors.add(WRITE_STREAM_SERVICE_PROVIDER)
    descriptors.add(GROUP_BY_FIELDS)
//    descriptors.add(STATE_TIMEOUT_DURATION_MS)
//    descriptors.add(STATE_TIMEOUT_DURATION_MS)
//    descriptors.add(STATEFULL_OUTPUT_MODE)
    Collections.unmodifiableList(descriptors)
  }

  override def init(sparkStreamContext: SparkStreamContext) = {
    super.init(sparkStreamContext.logislandStreamContext)
    this.sparkStreamContext = sparkStreamContext
    if (context.getPropertyValue(GROUP_BY_FIELDS).isSet) {
      groupByField = context.getPropertyValue(GROUP_BY_FIELDS).asString()
    }
//    context.getPropertyValue(STATEFULL_OUTPUT_MODE).asString() match {
//      case "append" => outputMode = OutputMode.Append()
//      case "update" => outputMode = OutputMode.Update()
//      case "complete" => outputMode = OutputMode.Complete()
//    }
    isReady = true;
  }

  private def context = sparkStreamContext.logislandStreamContext

  private def sparkSession = sparkStreamContext.spark

  override def start() = {
    if (!isReady)
      throw new IllegalStateException("stream not initialized")
    try {

      val pipelineMetricPrefix = context.getIdentifier /*+ ".partition" + partitionId*/ + "."
      val pipelineTimerContext = UserMetricsSystem.timer(pipelineMetricPrefix + "Pipeline.processing_time_ms").time()

      sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", "4")//TODO make this configurable

      //TODO Je pense que ces deux ligne ne servent a rien
      val controllerServiceLookup = sparkStreamContext.broadCastedControllerServiceLookupSink.value.getControllerServiceLookup()
      context.setControllerServiceLookup(controllerServiceLookup)

      //TODO stange way to update streamcontext, should'nt it be broadcasted ?
      // moreover the streamcontext should always be the last updated one in this function for me.
      // If driver wants to change it, it should call setup which would use a broadcast value for example ?
      // The remote api engines use this strange behaviour here
      // to change config on the fly when it should use the setup method (maybe using broadcast as well).
      // In this method start, the config should be considered already up to date in my opinion.
      // So currently this stream is not compatible with remoteApi change conf on the fly...
      // Anyway modification of a stream should be done at engine level !!!! stopping specific stream then init and restarting it with new StreamContext/ ProcessContext
      sparkStreamContext.logislandStreamContext.getProcessContexts.clear()
      sparkStreamContext.logislandStreamContext.getProcessContexts.addAll(
        PipelineConfigurationBroadcastWrapper.getInstance().get(sparkStreamContext.logislandStreamContext.getIdentifier))

      //Here we support multi source by making an union of each output dataset
      val readDF = context.getPropertyValue(READ_STREAM_SERVICE_PROVIDER)
        .asString().split(",").toSet
        .map(_.trim)
        .map(serviceId => controllerServiceLookup.getControllerService(serviceId))
        .map(_.asInstanceOf[StructuredStreamProviderServiceReader])
        .map(_.read(sparkSession))
        .reduce((source1, source2) => source1.union(source2))

      val transformedInputData: Dataset[Record] = transformInputData(readDF)

      val writerService = context.getPropertyValue(WRITE_STREAM_SERVICE_PROVIDER)
        .asControllerService()
        .asInstanceOf[StructuredStreamProviderServiceWriter]
      val dataStreamWriter = writerService
        .write(transformedInputData, sparkStreamContext.broadCastedControllerServiceLookupSink)

      var checkpointLocation : String = "checkpoints"
      if (GlobalOptions.checkpointLocation != null) {
        checkpointLocation = GlobalOptions.checkpointLocation
        getLogger.info(s"Checkpoint using checkpointLocation: $checkpointLocation")
      }

      getLogger.info(s"Starting structured stream sink ${writerService.getIdentifier} from stream ${identifier} with checkpointLocation: $checkpointLocation")
      dataStreamWriter
        .option("checkpointLocation", checkpointLocation + "/" + identifier + "/" + writerService.getIdentifier)
        .queryName(identifier + "#" + writerService.getIdentifier)
        .start()

      pipelineTimerContext.stop()
    }
    catch {
      case ex: Throwable =>
        getLogger.error("Error while processing the streaming query. ", ex)
        throw new IllegalStateException("Error while processing the streaming query", ex)
    }
  }

  override def stop(): Unit = {
    super.stop()
    //stop the source

    val thisStream = SQLContext.getOrCreate(sparkSession.sparkContext).streams.active.find(stream => context.getIdentifier.equals(stream.name));
    if (thisStream.isDefined) {
      if (!sparkSession.sparkContext.isStopped && thisStream.get.isActive) {
        try {
          thisStream.get.stop()
          thisStream.get.awaitTermination()
        } catch {
          case ex: Throwable => getLogger.warn(s"Stream ${context.getIdentifier} may not have been correctly stopped")
        }
      }
    } else {
      getLogger.warn(s"Unable to find an active streaming query for stream ${context.getIdentifier}")
    }
    this.isReady = false
  }

  def transformInputData(readDF: Dataset[Record]): Dataset[Record] = {
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]
    val pipelineMethods = new SparkPipeLineMethods(sparkStreamContext)
    if (context.getPropertyValue(GROUP_BY_FIELDS).isSet) {
      import readDF.sparkSession.implicits._
      readDF
        .groupByKey(_.getField(groupByField).asString())
        .flatMapGroups((key, iterator) => {
          pipelineMethods.executePipeline(key, iterator)
        })
    } else {
      readDF.mapPartitions(iterator => {
        pipelineMethods.executePipeline(iterator)
      })
    }
  }
}

object StructuredStream {
  //  StructuredStream props
  val READ_STREAM_SERVICE_PROVIDER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.stream.service.provider")
    .description("the controller service that gives connection information. " +
      "(can be a comma separeted list for multisource)")
    .required(true)
    .identifiesControllerService(classOf[StructuredStreamProviderServiceReader])
    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
    .build

  val WRITE_STREAM_SERVICE_PROVIDER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.stream.service.provider")
    .description("the controller service that gives connection information " +
      "(multi sink not supported yet)")
    .required(true)
    .identifiesControllerService(classOf[StructuredStreamProviderServiceWriter])
    .build

  val GROUP_BY_FIELDS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("group.by.fields")
    .description("comma separated list of fields to group the partition by")
    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
    .required(false)
    .build

  val IS_STATE_FULL: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("is.state.full")
    .description("If the stream should be state full or not")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .required(true)
    .defaultValue("false")
    .build

  val PROCESSING_STATE_TIMEOUT_TYPE = "processing"

  val STATE_TIMEOUT_TYPE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("state.timeout.type")
    .description("the time out strategy to use for evicting cache.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(PROCESSING_STATE_TIMEOUT_TYPE)
    .required(false)
    .defaultValue(PROCESSING_STATE_TIMEOUT_TYPE)
    .build

  val STATE_TIMEOUT_DURATION_MS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("state.timeout.ms")
    .description("the time in ms without data for specific key(s) before we invalidate the microbatch state when using "+
      STATE_TIMEOUT_TYPE + ": " + PROCESSING_STATE_TIMEOUT_TYPE + ". Default to 300000 (5 minutes)")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .defaultValue("300000")
    .build

  val STATEFULL_OUTPUT_MODE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("output.mode")
    .description("output mode when using statefull stream. By default will use append")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .allowableValues(StructuredStreamProviderServiceWriter.APPEND_MODE, StructuredStreamProviderServiceWriter.COMPLETE_MODE)//TODO
    .defaultValue(StructuredStreamProviderServiceWriter.APPEND_MODE)
    .build

  val STATE_WINDOW_FIELD: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("state.window.field")
    .description("the field to use to calcul records to keep in cache (should be a timestamp field as long)")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val STATE_WINDOW_TIMEOUT_MS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("state.window.timeout.ms")
    .description("the number of milliseconds for the window. We will save in cache only records in this timerange, " +
      "the greater timestamp is used as upper bound. All record out of range will not be stored in cache for next batch." +
      "In statefull mode, we transfer record in the given time range between batches.")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

}