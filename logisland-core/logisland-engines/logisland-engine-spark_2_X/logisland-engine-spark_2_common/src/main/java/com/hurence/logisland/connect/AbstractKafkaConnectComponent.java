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
package com.hurence.logisland.connect;


import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.connect.source.KafkaConnectStreamSourceProvider;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka connect to spark sql streaming bridge.
 *
 * @author amarziali
 */
public abstract class AbstractKafkaConnectComponent<T extends Connector, U extends Task> {


    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaConnectComponent.class);
    protected final T connector;
    protected final List<U> tasks = new ArrayList<>();
    protected final OffsetBackingStore offsetBackingStore;
    protected final AtomicBoolean startWatch = new AtomicBoolean(false);
    protected final String connectorName;
    private final Map<String, String> connectorProperties;

    protected final SQLContext sqlContext;
    protected final Converter keyConverter;
    protected final Converter valueConverter;
    protected final int maxTasks;
    protected final String streamId;


    /**
     * Base constructor. Should be called by {@link KafkaConnectStreamSourceProvider}
     *
     * @param sqlContext          the spark sql context.
     * @param connectorProperties the connector related properties.
     * @param keyConverter        the converter for the data key
     * @param valueConverter      the converter for the data body
     * @param offsetBackingStore  the backing store implementation (can be in-memory, file based, kafka based, etc...)
     * @param maxTasks            the maximum theoretical number of tasks this source should spawn.
     * @param connectorClass      the class of kafka connect source connector to wrap.
     *                            @param streamId the Stream id.
     *
     */
    public AbstractKafkaConnectComponent(SQLContext sqlContext,
                                         Map<String, String> connectorProperties,
                                         Converter keyConverter,
                                         Converter valueConverter,
                                         OffsetBackingStore offsetBackingStore,
                                         int maxTasks,
                                         String connectorClass,
                                         String streamId) {
        try {
            this.sqlContext = sqlContext;
            this.maxTasks = maxTasks;
            //instantiate connector
            this.connectorName = connectorClass;
            connector = ComponentFactory.loadComponent(connectorClass);
            //create converters
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
            this.connectorProperties = connectorProperties;
            this.streamId = streamId;

            //Create the connector context
            final ConnectorContext connectorContext = new ConnectorContext() {
                @Override
                public void requestTaskReconfiguration() {
                    try {
                        stopAllTasks();
                        createAndStartAllTasks();
                    } catch (Throwable t) {
                        LOGGER.error("Unable to reconfigure tasks for connector " + connectorName(), t);
                    }
                }

                @Override
                public void raiseError(Exception e) {
                    LOGGER.error("Connector " + connectorName() + " raised error : " + e.getMessage(), e);
                }
            };

            LOGGER.info("Starting connector {}", connectorClass);
            connector.initialize(connectorContext);
            this.offsetBackingStore = offsetBackingStore;


        } catch (Exception e) {
            throw new DataException("Unable to create connector " + connectorName(), e);
        }

    }

    public void start() {
        try {
            offsetBackingStore.start();
            //create and start tasks
            createAndStartAllTasks();
        } catch (Exception e) {
            try {
                stop();
            } catch (Throwable t) {
                LOGGER.error("Unable to properly stop tasks of connector " + connectorName(), t);
            }
            throw new DataException("Unable to start connector " + connectorName(), e);
        }
    }

    protected abstract void initialize(U task);

    /**
     * Create all the {@link Runnable} workers needed to host the source tasks.
     *
     * @return
     * @throws IllegalAccessException if task instantiation fails.
     * @throws InstantiationException if task instantiation fails.
     */
    protected void createAndStartAllTasks() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        if (!startWatch.compareAndSet(false, true)) {
            throw new IllegalStateException("Connector is already started");
        }
        connector.start(connectorProperties);
        Class<U> taskClass = (Class<U>) connector.taskClass();
        List<Map<String, String>> configs = connector.taskConfigs(maxTasks);
        tasks.clear();
        LOGGER.info("Creating {} tasks for connector {}", configs.size(), connectorName());
        for (Map<String, String> conf : configs) {
            //create the task
            U task = PluginProxy.create(taskClass.newInstance());
            initialize(task);
            task.start(conf);
            tasks.add(task);

        }
    }


    /**
     * Create a converter to be used to translate internal data.
     * Child classes can override this method to provide alternative converters.
     *
     * @return an instance of {@link Converter}
     */
    protected Converter createInternalConverter(boolean isKey) {
        JsonConverter internalConverter = new JsonConverter();
        internalConverter.configure(Collections.singletonMap("schemas.enable", "false"), isKey);
        return internalConverter;
    }

    /**
     * Gets the connector name used by this stream source.
     *
     * @return
     */
    protected String connectorName() {
        return connectorName;
    }


    /**
     * Stops every tasks running and serving for this connector.
     */
    protected void stopAllTasks() {
        LOGGER.info("Stopping every tasks for connector {}", connectorName());
        while (!tasks.isEmpty()) {
            try {
                tasks.remove(0).stop();
            } catch (Throwable t) {
                LOGGER.warn("Error occurring while stopping a task of connector " + connectorName(), t);
            }
        }
    }

    protected void stop() {
        if (!startWatch.compareAndSet(true, false)) {
            throw new IllegalStateException("Connector is not started");
        }
        LOGGER.info("Stopping connector {}", connectorName());
        stopAllTasks();
        offsetBackingStore.stop();
        connector.stop();
    }


    /**
     * Check the stream source state.
     *
     * @return
     */
    public boolean isRunning() {
        return startWatch.get();
    }


}

