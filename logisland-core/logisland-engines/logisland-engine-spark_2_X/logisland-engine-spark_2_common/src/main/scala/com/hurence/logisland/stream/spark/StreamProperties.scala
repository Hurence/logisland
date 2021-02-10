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
package com.hurence.logisland.stream

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.serializer._
import com.hurence.logisland.stream.spark.structured.provider.{StructuredStreamProviderServiceReader, StructuredStreamProviderServiceWriter}
import com.hurence.logisland.validator.{StandardValidators, ValidationResult, Validator}

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
object StreamProperties {
  //  StructuredStream props
  val READ_STREAM_SERVICE_PROVIDER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("read.stream.service.provider")
    .description("the controller service that gives connection information")
    .required(true)
    .identifiesControllerService(classOf[StructuredStreamProviderServiceReader])
    .build

  val WRITE_STREAM_SERVICE_PROVIDER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("write.stream.service.provider")
    .description("the controller service that gives connection information")
    .required(true)
    .identifiesControllerService(classOf[StructuredStreamProviderServiceWriter])
    .build
  //  General props
  val AVRO_INPUT_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.input.schema")
    .description("the avro schema definition")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val AVRO_OUTPUT_SCHEMA: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("avro.output.schema")
    .description("the avro schema definition for the output serialization")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val AVRO_SERIALIZER = new AllowableValue(classOf[AvroSerializer].getName,
    "avro serialization", "serialize events as avro blocs")
  val JSON_SERIALIZER = new AllowableValue(classOf[JsonSerializer].getName,
    "json serialization", "serialize events as json blocs")
  val EXTENDED_JSON_SERIALIZER = new AllowableValue(classOf[ExtendedJsonSerializer].getName,
    "extended json serialization", "serialize events as json blocs supporting nested objects/arrays")
  val KRYO_SERIALIZER = new AllowableValue(classOf[KryoSerializer].getName,
    "kryo serialization", "serialize events as binary blocs")
  val STRING_SERIALIZER = new AllowableValue(classOf[StringSerializer].getName,
    "string serialization", "serialize events as string")
  val BYTESARRAY_SERIALIZER = new AllowableValue(classOf[BytesArraySerializer].getName,
    "byte array serialization", "serialize events as byte arrays")
  val KURA_PROTOCOL_BUFFER_SERIALIZER = new AllowableValue(classOf[KuraProtobufSerializer].getName,
    "Kura Protobuf serialization", "serialize events as Kura protocol buffer")
  val NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes")

  val LATEST_OFFSET = new AllowableValue("latest", "latest", "the offset to the latest offset")
  val EARLIEST_OFFSET = new AllowableValue("earliest", "earliest offset", "the offset to the earliest offset")
  val NONE_OFFSET = new AllowableValue("none", "none offset", "the latest saved  offset")

  val WINDOW_DURATION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("window.duration")
    .description("all the elements in seen in a sliding window of time over. windowDuration = width of the window; must be a multiple of batching interval")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val SLIDE_DURATION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("slide.duration")
    .description("sliding interval of the window (i.e., the interval after which  the new DStream will generate RDDs); must be a multiple of batching interval")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val GROUPBY: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("groupby")
    .description("comma separated list of fields to group the partition by")
    .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
    .required(false)
    .build

  val OUTPUT_MODE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("output.mode")
    .description("output mode for the streaming sink. By default will use output mode by default of the sink (see sink doc)")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)//TODO outputmode validator
    .required(false)
    .build

  val STATE_TIMEOUT_MS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("state.timeout.ms")
    .description("the time in ms before we invalidate the microbatch state")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .defaultValue("2000")
    .build

  val CHUNK_SIZE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("chunk.size")
    .description("the number of records to group into chunks")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .required(false)
    .defaultValue("100")
    .build

  //////////////////////////////////////
  // HDFS options
  //////////////////////////////////////
  val FILE_FORMAT_PARQUET = "parquet"
  val FILE_FORMAT_ORC = "orc"
  val FILE_FORMAT_JSON = "json"
  val FILE_FORMAT_TXT = "txt"

}
