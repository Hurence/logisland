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
package com.hurence.logisland.stream.spark.structured.provider

import java.io.ByteArrayInputStream
import java.util
import java.util.Collections

import com.hurence.logisland.annotation.documentation.CapabilityDescription
import com.hurence.logisland.annotation.lifecycle.OnEnabled
import com.hurence.logisland.component.{InitializationException, PropertyDescriptor}
import com.hurence.logisland.controller.{AbstractControllerService, ControllerServiceInitializationContext}
import com.hurence.logisland.record.{FieldDictionary, FieldType, Record, StandardRecord}
import com.hurence.logisland.serializer.{NoopSerializer, RecordSerializer, SerializerProvider}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.stream.spark.structured.provider.LocalFileStructuredStreamProviderService.{AVRO_READ_LINE_SCHEMA, READ_LINE_SERIALIZER}
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.sql.SparkSession

/**
  *  You can look at spark documentation for detail on some options :
  * @author bailett
  */
@CapabilityDescription("Provide a way to read a local file as input in StructuredStream streams")
class LocalFileStructuredStreamProviderService extends AbstractControllerService
  with StructuredStreamProviderServiceReader {

  val LOCAL_INPUT_PATH: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("local.input.path")
    .description("the location of the directory of files to be loaded. All files inside the directory will be taked as input")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)//TODO directory validator
    .required(true)
    .build

  val MAX_FILES_PER_TRIGGER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("max.files.per.trigger")
    .description(" maximum number of new files to be considered in every trigger (default: no max) ")
    .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
    .required(false)
    .build

  val LATEST_FIRST: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("latest.first")
    .description("whether to processs the latest new files first, useful when there is a large backlog of files (default: false)")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .required(false)
    .build

  val FILENAME_ONLY: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("filename.only")
    .description("whether to check new files based on only the filename instead of on the full path (default: false). " +
      "With this set to `true`, the following files would be considered as the same file, because their filenames, \"dataset.txt\", " +
      "are the same:\n\"file:///dataset.txt\"\n\"s3://a/dataset.txt\"\n\"s3n://a/b/dataset.txt\"\n\"s3a://a/b/c/dataset.txt\"")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .required(false)
    .build

  var path: String = _
  var maxFilesPerTrigger: Option[Long] = _
  var latestFirst: Option[Boolean] = _
  var fileNameOnly: Option[Boolean] = _
  var readValueSerializer: RecordSerializer = null

  @OnEnabled
  @throws[InitializationException]
  override def init(context: ControllerServiceInitializationContext): Unit = {
    super.init(context)
    path = context.getPropertyValue(LOCAL_INPUT_PATH).asString()
    if (context.getPropertyValue(MAX_FILES_PER_TRIGGER).isSet) {
      maxFilesPerTrigger = Some(context.getPropertyValue(MAX_FILES_PER_TRIGGER).asLong())
    } else {
      maxFilesPerTrigger = None
    }
    if (context.getPropertyValue(LATEST_FIRST).isSet) {
      latestFirst = Some(context.getPropertyValue(LATEST_FIRST).asBoolean())
    } else {
      latestFirst = None
    }
    if (context.getPropertyValue(FILENAME_ONLY).isSet) {
      fileNameOnly = Some(context.getPropertyValue(FILENAME_ONLY).asBoolean())
    } else {
      fileNameOnly = None
    }
    readValueSerializer = SerializerProvider.getSerializer(
      context.getPropertyValue(READ_LINE_SERIALIZER).asString,
      context.getPropertyValue(AVRO_READ_LINE_SCHEMA).asString)
  }

  /**
    * Allows subclasses to register which property descriptor objects are
    * supported.
    *
    * @return PropertyDescriptor objects this processor currently supports
    */
  override def getSupportedPropertyDescriptors() = {
    val descriptors: util.List[PropertyDescriptor] = new util.ArrayList[PropertyDescriptor]
    descriptors.add(LOCAL_INPUT_PATH)
    descriptors.add(MAX_FILES_PER_TRIGGER)
    descriptors.add(LATEST_FIRST)
    descriptors.add(FILENAME_ONLY)
    descriptors.add(READ_LINE_SERIALIZER)
    descriptors.add(AVRO_READ_LINE_SCHEMA)
    Collections.unmodifiableList(descriptors)
  }

  /**
    * create a streaming DataFrame that represents data received
    *
    * @param spark
    * @return DataFrame currently loaded
    */
  override def read(spark: SparkSession) = {
    import spark.implicits._
    implicit val recordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    val dataStreamReader =  spark.readStream
      .format("text")
    if (maxFilesPerTrigger.isDefined) {
      dataStreamReader.option("maxFilesPerTrigger", maxFilesPerTrigger.get)
    }
    if (latestFirst.isDefined) {
      dataStreamReader.option("latestFirst", latestFirst.get)
    }
    if (fileNameOnly.isDefined) {
      dataStreamReader.option("fileNameOnly", fileNameOnly.get)
    }
    dataStreamReader.load(path)
      .as[String]
      .flatMap(line => {
        readValueSerializer match {
          case sr: NoopSerializer => Some(new StandardRecord("line")
            .setField(FieldDictionary.RECORD_VALUE, FieldType.STRING, line))
          case _ => SerializingTool.deserializeRecords(readValueSerializer, line.getBytes())
        }
      })
  }
}
object LocalFileStructuredStreamProviderService {
  val READ_LINE_SERIALIZER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("line.serializer")
    .description("the serializer to use to deserialize lines of files as record")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, EXTENDED_JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, STRING_SERIALIZER, NO_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER)
    .defaultValue(NO_SERIALIZER.getValue)
    .build

  val AVRO_READ_LINE_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.value.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

}
