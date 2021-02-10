package com.hurence.logisland.stream.spark.structured.provider

import com.hurence.logisland.component.{AllowableValue, PropertyDescriptor}
import com.hurence.logisland.stream.StreamProperties._
import com.hurence.logisland.validator.StandardValidators

object MqttProperties {
  val MQTT_BROKER_URL: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.broker.url")
    .description("brokerUrl A url MqttClient connects to. Set this or path as the url of the Mqtt Server. e.g. tcp://localhost:1883")
    .addValidator(StandardValidators.URL_VALIDATOR)
    .defaultValue("tcp://localhost:1883")
    .required(false)
    .build

  val MQTT_PERSISTENCE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.persistence")
    .description("persistence By default it is used for storing incoming messages on disk. " +
      "If memory is provided as value for this option, then recovery on restart is not supported.")
    .defaultValue("memory")
    .required(false)
    .build

  val MQTT_TOPIC: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.topic")
    .description("Topic MqttClient subscribes to.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val MQTT_CLIENTID: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.client.id")
    .description("clientID this client is associated. Provide the same value to recover a stopped client.")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .required(true)
    .build

  val MQTT_QOS: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.qos")
    .description(" QoS The maximum quality of service to subscribe each topic at.Messages published at a lower " +
      "quality of service will be received at the published QoS.Messages published at a higher quality of " +
      "service will be received using the QoS specified on the subscribe")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("0")
    .required(false)
    .build

  val MQTT_USERNAME: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.username")
    .description(" username Sets the user name to use for the connection to Mqtt Server. " +
      "Do not set it, if server does not need this. Setting it empty will lead to errors.")
    .required(false)
    .build

  val MQTT_PASSWORD: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.password")
    .description("password Sets the password to use for the connection")
    .required(false)
    .build

  val MQTT_CLEAN_SESSION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.clean.session")
    .description("cleanSession Setting it true starts a clean session, removes all checkpointed messages by " +
      "a previous run of this source. This is set to false by default.")
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .defaultValue("true")
    .required(false)
    .build

  val MQTT_CONNECTION_TIMEOUT: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.connection.timeout")
    .description("connectionTimeout Sets the connection timeout, a value of 0 is interpreted as " +
      "wait until client connects. See MqttConnectOptions.setConnectionTimeout for more information")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5000")
    .required(false)
    .build

  val MQTT_KEEP_ALIVE: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.keep.alive")
    .description("keepAlive Same as MqttConnectOptions.setKeepAliveInterval.")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5000")
    .required(false)
    .build


  val MQTT_VERSION: PropertyDescriptor = new PropertyDescriptor.Builder()
    .name("mqtt.version")
    .description("mqttVersion Same as MqttConnectOptions.setMqttVersion")
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .defaultValue("5000")
    .required(false)
    .build
}
