package com.hurence.logisland.stream.spark

import com.hurence.logisland.stream.{RecordStream, StandardStreamContext, StreamContext}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

class SparkStreamContext(val streamingContext: StreamContext,
                         val appName: String,
                         val ssc: StreamingContext,
                         val broadCastedControllerServiceLookupSink: Broadcast[ControllerServiceLookupSink])
  extends StandardStreamContext(streamingContext.getStream, streamingContext.getIdentifier) with StreamContext {}