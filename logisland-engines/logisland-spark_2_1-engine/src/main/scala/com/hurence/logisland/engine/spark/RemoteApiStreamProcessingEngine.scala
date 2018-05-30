/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.engine.spark

import java.time.Duration
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, TimeUnit}

import com.hurence.logisland.component.PropertyDescriptor
import com.hurence.logisland.engine.EngineContext
import com.hurence.logisland.engine.spark.remote.{RemoteApiClient, RemoteComponentRegistry}
import com.hurence.logisland.stream.StandardStreamContext
import com.hurence.logisland.stream.spark.DummyRecordStream
import com.hurence.logisland.validator.StandardValidators
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

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

class RemoteApiStreamProcessingEngine extends KafkaStreamProcessingEngine {

    private val logger = LoggerFactory.getLogger(classOf[RemoteApiStreamProcessingEngine])
    private val executor = Executors.newSingleThreadScheduledExecutor()
    private var remoteApiRegistry: RemoteComponentRegistry = _


    override def getSupportedPropertyDescriptors: util.List[PropertyDescriptor] = {
        val ret = new util.ArrayList(super.getSupportedPropertyDescriptors)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_BASE_URL)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_POLLING_RATE)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_CONNECT_TIMEOUT)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_USER)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_PASSWORD)
        ret.add(RemoteApiStreamProcessingEngine.REMOTE_API_SOCKET_TIMEOUT)
        return Collections.unmodifiableList(ret)
    }

    override protected def setupStreamingContexts(engineContext: EngineContext, scc: StreamingContext): Unit = {
        if (!engineContext.getControllerServiceConfigurations.isEmpty) {
            logger.warn("This engine will not load service controllers from the configuration file!")
            engineContext.getControllerServiceConfigurations.clear()
        }
        if (!engineContext.getStreamContexts.isEmpty()) {
            logger.warn("This engine will not handle streams from the configuration file!")
            engineContext.getStreamContexts.clear()
        }
        engineContext.addStreamContext(new StandardStreamContext(new DummyRecordStream(), "busybox"));
        super.setupStreamingContexts(engineContext, scc)
        remoteApiRegistry = new RemoteComponentRegistry(engineContext);

    }

    /**
      * Called after the engine has been started.
      *
      * @param engineContext
      */
    override protected def onStart(engineContext: EngineContext): Unit = {
        super.onStart(engineContext)
        val remoteApiClient = new RemoteApiClient(
            engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_BASE_URL),
            Duration.ofMillis(engineContext.getPropertyValue(RemoteApiStreamProcessingEngine.REMOTE_API_SOCKET_TIMEOUT).asLong()),
            Duration.ofMillis(engineContext.getPropertyValue(RemoteApiStreamProcessingEngine.REMOTE_API_CONNECT_TIMEOUT).asLong()),
            engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_USER),
            engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_PASSWORD))

        implicit def funToRunnable(fun: () => Unit) = new Runnable() {
            def run() = fun()
        }

        executor.scheduleWithFixedDelay(() => {
            val pipelines = remoteApiClient.fetchPipelines()
            if (pipelines.isPresent) {
                remoteApiRegistry.updateEngineContext(pipelines.get())
            }

        }, 0, engineContext.getProperty(RemoteApiStreamProcessingEngine.REMOTE_API_POLLING_RATE).toInt,
            TimeUnit.MILLISECONDS)
    }

    /**
      * Called before the engine is being stopped.
      *
      * @param engineContext
      */
    override protected def onStop(engineContext: EngineContext): Unit = {
        super.onStop(engineContext)
        executor.shutdown()
        //stop everything started from remote side.
        if (remoteApiRegistry != null) {
            remoteApiRegistry.updateEngineContext(Collections.emptyList())
        }
    }
}
