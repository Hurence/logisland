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
package com.hurence.logisland.stream.spark.structured.provider

import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.Record
import com.hurence.logisland.stream.StreamProperties.OUTPUT_MODE
import com.hurence.logisland.stream.spark.structured.provider.ConsoleStructuredStreamProviderService.{NUM_ROWS_TO_SHOW, TRUNCATE_OUTPUT}
import com.hurence.logisland.util.spark.ControllerServiceLookupSink
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, Encoders}

@CapabilityDescription("Provide a ways to print output in console in a StructuredStream streams")
class ConsoleStructuredStreamProviderService extends AbstractControllerService
  with StructuredStreamProviderServiceWriter {

    var  numRows: Option[Long] = _
    var  truncate: Option[Boolean] = _
    var  outputMode: Option[String] = _

    @OnEnabled
    @throws[InitializationException]
    override def init(context: ControllerServiceInitializationContext): Unit = {
        super.init(context)
        this.synchronized {
            try {
                if (context.getPropertyValue(NUM_ROWS_TO_SHOW).isSet) {
                    numRows = Some(context.getPropertyValue(NUM_ROWS_TO_SHOW).asLong())
                } else {
                    numRows = None
                }
                if (context.getPropertyValue(TRUNCATE_OUTPUT).isSet) {
                    truncate = Some(context.getPropertyValue(TRUNCATE_OUTPUT).asBoolean())
                } else {
                    truncate = None
                }
                if (context.getPropertyValue(OUTPUT_MODE).isSet) {
                    outputMode = Some(context.getPropertyValue(OUTPUT_MODE).asString())
                } else {
                    outputMode = None
                }
            } catch {
                case e: Exception =>
                    throw new InitializationException(e)
            }
        }
    }

    /**
      * Allows subclasses to register which property descriptor objects are
      * supported.
      *
      * @return PropertyDescriptor objects this processor currently supports
      */
    override def getSupportedPropertyDescriptors() = {
        val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
        descriptors.add(OUTPUT_MODE)
        descriptors.add(TRUNCATE_OUTPUT)
        descriptors.add(NUM_ROWS_TO_SHOW)
        Collections.unmodifiableList(descriptors)
    }

    /**
      * create a streaming DataFrame that represents data received
      *
      * @return DataFrame currently loaded
      */
    override def write(df: Dataset[Record], controllerServiceLookupSink: Broadcast[ControllerServiceLookupSink]): StreamingQuery = {
        val dataStreamWriter = df
          .map(_.toString())(Encoders.STRING)
          .writeStream
          .format("console")
        if (numRows.isDefined) {
            dataStreamWriter.option("numRows", numRows.get)
        }
        if (truncate.isDefined) {
            dataStreamWriter.option("truncate", truncate.get)
        }
        if (outputMode.isDefined) {
            dataStreamWriter.outputMode(outputMode.get)
        }
        dataStreamWriter
          .queryName(getIdentifier)
          .start()
    }
}

object ConsoleStructuredStreamProviderService {
    val NUM_ROWS_TO_SHOW: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("rows")
      .description("Number of rows to print every trigger (default: 20 see spark documentation)")
      .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
      .required(false)
      .build

    val TRUNCATE_OUTPUT: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("truncate")
      .description("Whether to truncate the output if too long (default: true see spark documentation) ")
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .required(false)
      .build
}
