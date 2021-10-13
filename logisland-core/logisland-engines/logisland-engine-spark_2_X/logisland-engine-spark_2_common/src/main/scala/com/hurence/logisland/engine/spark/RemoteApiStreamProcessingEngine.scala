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
package com.hurence.logisland.engine.spark

import java.time.Duration
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, TimeUnit}

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.engine.spark.remote.model.DataFlow
import com.hurence.logisland.engine.spark.remote.{RemoteApiClient, RemoteApiComponentFactory}

import com.hurence.logisland.validator.StandardValidators
import org.slf4j.LoggerFactory

class RemoteApiStreamProcessingEngine extends KafkaStreamProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[RemoteApiStreamProcessingEngine])
    private var initialized = false


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val ret = new util.ArrayList(super.getSupportedPropertyDescriptors)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_BASE_URL)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_POLLING_RATE)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_CONFIG_PUSH_RATE)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_CONNECT_TIMEOUT)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_USER)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_PASSWORD)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_SOCKET_TIMEOUT)
        return Collections.unmodifiableList(ret)
    }


    /**
      * start the engine
      *
      * @param engineContext
      */
    override def start(engineContext: EngineContext): Unit = {
       // engineContext.addStreamContext(new StandardStreamContext(new DummyRecordStream(), "busybox"))

        if (!initialized) {
            initialized = true
            val remoteApiClient = new RemoteApiClient(new RemoteApiClient.ConnectionSettings(
                engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_BASE_URL),
                Duration.ofMillis(engineContext.getPropertyValue(RemoteApiStreamProcessingEngine.REMOTE_API_SOCKET_TIMEOUT).asLong()),
                Duration.ofMillis(engineContext.getPropertyValue(RemoteApiStreamProcessingEngine.REMOTE_API_CONNECT_TIMEOUT).asLong()),
                engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_USER),
                engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_PASSWORD)))


            val appName = getCurrentSparkContext().appName
            var currentDataflow: DataFlow = null

            //schedule dataflow refresh
            @transient lazy val executor = Executors.newSingleThreadScheduledExecutor();
            @transient lazy val remoteApiComponentFactory = new RemoteApiComponentFactory


            executor.scheduleWithFixedDelay(new Runnable {
                val state = new RemoteApiClient.State

                override def run(): Unit = {
                    var changed = false
                    try {
                        val dataflow = remoteApiClient.fetchDataflow(appName, state)
                        if (dataflow.isPresent) {
                            changed = true
                            if (remoteApiComponentFactory.updateEngineContext(getCurrentSparkContext(), engineContext, dataflow.get, currentDataflow)) {
                                currentDataflow = dataflow.get()
                            }
                        }
                    } catch {
                        case default: Throwable => {
                            currentDataflow = null
                            logger.warn("Unexpected exception while trying to poll for new dataflow configuration", default)
                            softStop(engineContext)
                        }
                    } finally {
                        if (changed) {
                            try {
                                remoteApiClient.pushDataFlow(appName, currentDataflow);
                            } catch {
                                case default: Throwable => logger.warn("Unexpected exception while trying to push configuration to remote server", default)
                            }
                        }
                    }
                }
            }, 0, engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_POLLING_RATE).toInt, TimeUnit.MILLISECONDS
            )

            executor.scheduleWithFixedDelay(new Runnable {

                override def run(): Unit = {
                    try {
                        remoteApiClient.pushDataFlow(appName, currentDataflow)
                    } catch {
                        case default: Throwable => logger.warn("Unexpected exception while trying to push configuration to remote server", default)
                    }
                }
            }, 0, engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_CONFIG_PUSH_RATE).toInt, TimeUnit.MILLISECONDS
            )


        }


        super.start(engineContext)
    }


    override def stop(engineContext: EngineContext): Unit = {
        super.stop(engineContext)
    }

    override def softStop(engineContext: EngineContext): Unit = {
        logger.info(s"Resetting engine ${engineContext.getIdentifier}")
        super.softStop(engineContext)
        engineContext.getStreamContexts.clear()
        engineContext.getControllerServiceContexts.clear()
        engineContext.getControllerServiceConfigurations.clear()
    }



}
object RemoteApiStreamProcessingEngine {
    val REMOTE_API_BASE_URL = new PropertyDescriptor.Builder()
      .name("remote.api.baseUrl")
      .description("The base URL of the remote server providing logisland configuration")
      .required(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

    val REMOTE_API_POLLING_RATE = new PropertyDescriptor.Builder()
      .name("remote.api.polling.rate")
      .description("Remote api polling rate in milliseconds")
      .required(true)
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .build

    val REMOTE_API_CONFIG_PUSH_RATE = new PropertyDescriptor.Builder()
      .name("remote.api.push.rate")
      .description("Remote api configuration push rate in milliseconds")
      .required(true)
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .build

    val REMOTE_API_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
      .name("remote.api.timeouts.connect")
      .description("Remote api connection timeout in milliseconds")
      .required(false)
      .defaultValue("10000")
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .build

    val REMOTE_API_SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
      .name("remote.api.timeouts.socket")
      .description("Remote api default read/write socket timeout in milliseconds")
      .required(false)
      .defaultValue("10000")
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .build

    val REMOTE_API_USER = new PropertyDescriptor.Builder()
      .name("remote.api.auth.user")
      .description("The basic authentication user for the remote api endpoint.")
      .required(false)
      .build

    val REMOTE_API_PASSWORD = new PropertyDescriptor.Builder()
      .name("remote.api.auth.password")
      .description("The basic authentication password for the remote api endpoint.")
      .required(false)
      .build
}