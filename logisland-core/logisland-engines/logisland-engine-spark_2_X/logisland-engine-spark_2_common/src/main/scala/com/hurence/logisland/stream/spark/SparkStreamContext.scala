package com.hurence.logisland.stream.spark

import com.hurence.logisland.stream.{StandardStreamContext, StreamContext}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

case class SparkStreamContext(logislandStreamContext: StreamContext,
                              defaultBatchDurationMs: Int,
                              @transient spark: SparkSession,
                              broadCastedControllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]) {

}