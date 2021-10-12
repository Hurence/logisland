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

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.validator.{StandardValidators, ValidationResult, Validator}

object AzureEventHubProperties {
  val EVENTHUBS_NAMESPACE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.nameSpace")
    .description("EventHubs namespace.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val EVENTHUBS_MAX_EVENTS_PER_TRIGGER: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.maxEventsPerTrigger")
    .description("Rate limit on maximum number of events processed per trigger interval. The specified total number" +
      " of events will be proportionally split across partitions of different volume.")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_OPERATION_TIMEOUT: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.operationTimeout")
    .description("The amount of time (in milliseconds) Event Hub API calls will be retried before throwing an exception.")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_THREAD_POOL_SIZE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.threadPoolSize")
    .description("Sets the size of thread pool.")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_READ_EVENT_HUB: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.eventHub")
    .description("EventHub to read from.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_READ_SAS_KEY_NAME: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.sasKeyName")
    .description("SAS key name for read eventhub.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_READ_SAS_KEY: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.sasKey")
    .description("SAS key for read eventhub.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_READ_CONSUMER_GROUP: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.consumerGroup")
    .description("Consumer group name.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_READ_POSITION_START_OF_STREAM = "start-of-stream"
  val EVENTHUBS_READ_POSITION_END_OF_STREAM = "end-of-stream"
  val EVENTHUBS_READ_POSITION_INSTANT_NOW = "instant-now"

  // Validator for EVENTHUBS_READ_POSITION
  val EVENTHUBS_READ_POSITION_VALIDATOR: Validator = new Validator() {
    override def validate(subject: String, value: String): ValidationResult =  {

      // Accepted values are long, or start-of-stream or end-of-stream
      var ok : Boolean  = false
      // Recognized string?
      if ( (value == EVENTHUBS_READ_POSITION_START_OF_STREAM) || (value == EVENTHUBS_READ_POSITION_END_OF_STREAM)
        || (value == EVENTHUBS_READ_POSITION_INSTANT_NOW) ) {
        ok = true
      }
      // Long value?
      try {
        value.toLong
        ok = true
      } catch {
        case e: Exception => // Not a long;
      }
      new ValidationResult.Builder().subject(subject).input(value).valid(ok)
        .explanation(subject + " should be a long or " + EVENTHUBS_READ_POSITION_START_OF_STREAM + " or " +
          EVENTHUBS_READ_POSITION_END_OF_STREAM + " or " + EVENTHUBS_READ_POSITION_INSTANT_NOW).build}
  }

  val EVENTHUBS_READ_POSITION_TYPE_NAME = "eventhubs.read.positionType"
  val EVENTHUBS_READ_POSITION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.position")
    .description("Start event position. This may be either " + EVENTHUBS_READ_POSITION_START_OF_STREAM + ", " +
      EVENTHUBS_READ_POSITION_END_OF_STREAM + " or a long value. If this is a long value, " +
      EVENTHUBS_READ_POSITION_TYPE_NAME + " should be filled to define the meaning of the value. Default value is " +
      EVENTHUBS_READ_POSITION_END_OF_STREAM)
    .addValidator(EVENTHUBS_READ_POSITION_VALIDATOR)
    .required(false)
    .defaultValue(EVENTHUBS_READ_POSITION_END_OF_STREAM)
    .build

  val EVENTHUBS_READ_POSITION_TYPE_OFFSET = "offset"
  val EVENTHUBS_READ_POSITION_TYPE_SEQUENCE_NUMBER = "sequenceNumber"
  val EVENTHUBS_READ_POSITION_TYPE_EPOCH_MILLIS = "epochMillis"

  val EVENTHUBS_READ_POSITION_TYPE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name(EVENTHUBS_READ_POSITION_TYPE_NAME)
    .description("Specifies the type of the " + EVENTHUBS_READ_POSITION.getName + " value when it is a long value. " +
      "This can be " + EVENTHUBS_READ_POSITION_TYPE_OFFSET + ", " + EVENTHUBS_READ_POSITION_TYPE_SEQUENCE_NUMBER +
      " or " + EVENTHUBS_READ_POSITION_TYPE_EPOCH_MILLIS + ". Default value is " + EVENTHUBS_READ_POSITION_TYPE_OFFSET)
    .allowableValues(EVENTHUBS_READ_POSITION_TYPE_OFFSET, EVENTHUBS_READ_POSITION_TYPE_SEQUENCE_NUMBER,
      EVENTHUBS_READ_POSITION_TYPE_EPOCH_MILLIS)
    .required(false)
    .defaultValue(EVENTHUBS_READ_POSITION_TYPE_OFFSET)
    .build

  val EVENTHUBS_READ_RECEIVER_TIMEOUT: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.receiverTimeout")
    .description("The amount of time (in milliseconds) Event Hub receive calls will be retried before throwing an exception.")
    .addValidator(StandardValidators.LONG_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_READ_PREFETCH_COUNT: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.read.prefetchCount")
    .description("Sets the prefetch count for the underlying receiver and controls how many events are received in advance.")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_WRITE_EVENT_HUB: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.write.eventHub")
    .description("EventHub to write to.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_WRITE_SAS_KEY_NAME: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.write.sasKeyName")
    .description("SAS key name for write eventhub.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build

  val EVENTHUBS_WRITE_SAS_KEY: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("eventhubs.write.sasKey")
    .description("SAS key for write eventhub.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(false)
    .build
}
