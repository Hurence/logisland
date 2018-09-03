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
package com.hurence.logisland.stream.spark

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.validator.StandardValidators
import org.apache.kafka.connect.connector.Connector
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig
import org.apache.kafka.connect.storage.Converter

object StreamOptions {

    val MEMORY_BACKING_STORE = new AllowableValue("memory", "In memory backing store",
        "Standalone in memory offset backing store. Not suitable for clustered deployments unless source is unique or stateless")

    val FILE_BACKING_STORE = new AllowableValue("file", "File based backing store",
        "Standalone filesystem based offset backing store. " +
            "You have to specify the property " + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG + " for the file path." +
            "Not suitable for clustered deployments unless source is unique or standalone")

    val KAFKA_BACKING_STORE = new AllowableValue("kafka", "Kafka topic based backing store",
        "Distributed kafka topic based offset backing store. " +
            "See the javadoc of class org.apache.kafka.connect.storage.KafkaOffsetBackingStore for the configuration options." +
            "This backing store is well suited for distributed deployments.")


    //////////////////////////////////////
    // Kafka Connect options
    //////////////////////////////////////


    val KAFKA_CONNECT_CONNECTOR_CLASS = new PropertyDescriptor.Builder()
        .name("kc.connector.class")
        .description("The class canonical name of the kafka connector to use.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .addValidator(StandardValidators.TYPE_VALIDATOR(classOf[Connector]))
        .build

    val KAFKA_CONNECT_CONNECTOR_PROPERTIES = new PropertyDescriptor.Builder()
        .name("kc.connector.properties")
        .description("The properties (key=value) for the connector.")
        .required(false)
        .defaultValue("")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val KAFKA_CONNECT_MAX_TASKS = new PropertyDescriptor.Builder()
        .name("kc.worker.tasks.max")
        .description("Max number of threads for this connector")
        .required(true)
        .defaultValue("1")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build

    val KAFKA_CONNECT_MAX_PARTITIONS = new PropertyDescriptor.Builder()
        .name("kc.partitions.max")
        .description("Max number of partitions for this connector.")
        .required(false)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build

    val KAFKA_CONNECT_KEY_CONVERTER = new PropertyDescriptor.Builder()
        .name("kc.data.key.converter")
        .description("Key converter class")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .addValidator(StandardValidators.TYPE_VALIDATOR(classOf[Converter]))
        .build

    val KAFKA_CONNECT_VALUE_CONVERTER = new PropertyDescriptor.Builder()
        .name("kc.data.value.converter")
        .description("Value converter class")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .addValidator(StandardValidators.TYPE_VALIDATOR(classOf[Converter]))
        .build

    val KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES = new PropertyDescriptor.Builder()
        .name("kc.data.key.converter.properties")
        .description("Key converter properties")
        .required(false)
        .defaultValue("")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

    val KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES = new PropertyDescriptor.Builder()
        .name("kc.data.value.converter.properties")
        .description("Value converter properties")
        .required(false)
        .defaultValue("")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build


    val KAFKA_CONNECT_OFFSET_BACKING_STORE = new PropertyDescriptor.Builder()
        .name("kc.connector.offset.backing.store")
        .required(false)
        .description("The underlying backing store to be used.")
        .defaultValue(MEMORY_BACKING_STORE.getValue)
        .allowableValues(MEMORY_BACKING_STORE, FILE_BACKING_STORE, KAFKA_BACKING_STORE)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build()

    val KAFKA_CONNECT_OFFSET_BACKING_STORE_PROPERTIES = new PropertyDescriptor.Builder()
        .name("kc.connector.offset.backing.store.properties")
        .description("Properties to configure the offset backing store")
        .required(false)
        .defaultValue("")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build

}
