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
package com.hurence.logisland.engine.vanilla.stream.amqp;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.vanilla.PlainJavaEngine;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.BsonSerializer;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonServer;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AmqpClientPipelineStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(AmqpClientPipelineStreamTest.class);

    private ProtonServer server;
    private final TransferQueue<Message> inQueue = new LinkedTransferQueue<>();
    private final TransferQueue<Message> outQueue = new LinkedTransferQueue<>();


    public static class ChangeKeyProcessor extends AbstractProcessor {
        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(
                    new PropertyDescriptor.Builder()
                            .name("key")
                            .required(true)
                            .build()
            );
        }

        @Override
        public Collection<Record> process(ProcessContext context, Collection<Record> records) {
            final String key = context.getPropertyValue("key").asString();
            return records.stream()
                    .map(r -> {
                        r.setField(FieldDictionary.RECORD_KEY, FieldType.STRING, key);
                        return r;
                    }).collect(Collectors.toList());

        }
    }

    @After
    public void stopAmqpDummyServer() {
        server.close();
    }

    @Before
    public void startAmqpDummyServer() throws Exception {
        int port;
        try (ServerSocket tmp = new ServerSocket(0)) {
            port = tmp.getLocalPort();
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        startAmqpDummyServer(port, completableFuture);
        completableFuture.get();

    }

    private void startAmqpDummyServer(int port, CompletableFuture<Void> completableFuture) throws Exception {
        if (server == null) {
            server = ProtonServer.create(Vertx.vertx());

            server.connectHandler(connection -> {
                connection.openHandler(res -> {
                    logger.info("Client connection opened, container-id: {}", connection.getRemoteContainer());
                    connection.open();
                });

                connection.closeHandler(c -> {
                    logger.info("Client closing connection, container-id:  {}", connection.getRemoteContainer());
                    connection.close();
                    connection.disconnect();
                });

                connection.disconnectHandler(c -> {
                    logger.info("Client socket disconnected, container-id: {} ", connection.getRemoteContainer());
                    connection.disconnect();
                });

                connection.sessionOpenHandler(session -> {
                    session.closeHandler(x -> {
                        session.close();
                        session.free();
                    });
                    session.open();
                }).receiverOpenHandler(receiver -> {
                    receiver.setTarget(receiver.getRemoteTarget());
                    receiver.setSource(receiver.getRemoteSource());
                    receiver.setAutoAccept(true);
                    receiver.handler((delivery, message) -> {
                        try {
                            outQueue.put(message);
                            ProtonHelper.accepted(delivery, true);
                        } catch (Exception e) {
                            logger.error("Unexpected error during the delivery. Test should fail!", e);
                            ProtonHelper.rejected(delivery, true);
                        }
                    }).open();
                }).senderOpenHandler(sender -> {
                    sender.setSource(sender.getRemoteSource());
                    sender.setTarget(sender.getRemoteTarget());
                    Vertx.currentContext().owner().setPeriodic(1, id -> {
                        if (!sender.sendQueueFull() && !inQueue.isEmpty()) {
                            final Message m = inQueue.remove();
                            if (m != null) {
                                sender.send(m, event -> {
                                    if (!event.remotelySettled()) {
                                        inQueue.add(m);
                                    }
                                });
                            }
                        }
                    });

                    sender.open();


                });

            });
        }
        server.listen(port, event -> {
            if (event.failed()) {
                completableFuture.completeExceptionally(event.cause());
            } else {
                completableFuture.complete(null);
            }
        });

    }

    private EngineConfiguration engineConfiguration() {
        EngineConfiguration engineConfiguration = new EngineConfiguration();
        engineConfiguration.setType("engine");
        engineConfiguration.setDocumentation("Plain java engine");
        engineConfiguration.setComponent(PlainJavaEngine.class.getCanonicalName());
        return engineConfiguration;
    }

    private ProcessorConfiguration modifyKeyProcessor(String key) {
        ProcessorConfiguration ret = new ProcessorConfiguration();
        ret.setProcessor(UUID.randomUUID().toString());
        ret.setComponent(ChangeKeyProcessor.class.getName());
        ret.setType("processor");
        ret.setConfiguration(Collections.singletonMap("key", key));
        return ret;
    }

    private final Supplier<Map<String, String>> defaultPropertySupplier(Map<String, String> props) {
        return () -> {
            Map<String, String> conf = new HashMap<>();
            conf.put(StreamOptions.CONTAINER_ID.getName(), "test");
            conf.put(StreamOptions.CONNECTION_HOST.getName(), "localhost");
            conf.put(StreamOptions.CONNECTION_PORT.getName(), Integer.toString(server.actualPort()));
            conf.put(StreamOptions.READ_TOPIC.getName(), "in_address");
            conf.put(StreamOptions.WRITE_TOPIC.getName(), "out_address");
            conf.put(StreamOptions.WRITE_TOPIC_SERIALIZER.getName(), StreamOptions.BSON_SERIALIZER.getValue());
            conf.putAll(props);
            return conf;
        };
    }

    private final Supplier<Map<String, String>> defaultPropertySupplier() {
        return defaultPropertySupplier(Collections.emptyMap());
    }

    private StreamConfiguration emptyStream(Supplier<Map<String, String>> propertySupplier) {
        StreamConfiguration streamConfiguration = new StreamConfiguration();
        streamConfiguration.setStream("amqp_empty");
        streamConfiguration.setComponent(AmqpClientPipelineStream.class.getCanonicalName());
        streamConfiguration.setType("stream");
        streamConfiguration.setConfiguration(propertySupplier.get());
        return streamConfiguration;
    }

    @Test
    public void testEmptyEngine() {
        EngineConfiguration engineConfiguration = engineConfiguration();
        engineConfiguration.addPipelineConfigurations(emptyStream(defaultPropertySupplier()));
        EngineContext context = ComponentFactory.getEngineContext(engineConfiguration).get();
        Assert.assertTrue(context.isValid());
        context.getEngine().start(context);
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                context.getEngine().shutdown(context);
            }
        }, 2000);

        context.getEngine().awaitTermination(context);
    }

    private Message createMessage(String subject, String content) {
        Message ret = ProtonHelper.message();
        ret.setSubject(subject);
        ret.setBody(new Data(Binary.create(ByteBuffer.wrap(content.getBytes()))));
        return ret;
    }

    @Test
    public void testFlowControl() {
        final Vertx vertx = Vertx.vertx();


        EngineConfiguration engineConfiguration = engineConfiguration();
        StreamConfiguration streamConfiguration = emptyStream(defaultPropertySupplier(
                Collections.singletonMap(StreamOptions.WRITE_TOPIC_CONTENT_TYPE.getName(), "record/bson")
        ));
        streamConfiguration.addProcessorConfiguration(modifyKeyProcessor("i_m_the_new_key"));
        engineConfiguration.addPipelineConfigurations(streamConfiguration);
        EngineContext context = ComponentFactory.getEngineContext(engineConfiguration).get();
        Assert.assertTrue(context.isValid());
        context.getEngine().start(context);
        vertx.runOnContext(v -> {
            for (int i = 0; i < 10240; i++) {
                try {
                    Message m = createMessage("test", "I'm a test message " + i);
                    inQueue.put(m);
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while waiting");
                    break;
                }
            }
        });


        vertx.setPeriodic(1000, id -> {
            logger.info("Still {} messages waiting", inQueue.size());

            if (inQueue.size() == 0) {
                vertx.cancelTimer(id);
                context.getEngine().shutdown(context);
            }
        });

        context.getEngine().awaitTermination(context);
        final BsonSerializer bsonSerializer = new BsonSerializer();
        for (int i = 0; i < outQueue.size(); i++) {
            Message m = outQueue.remove();
            Assert.assertEquals("i_m_the_new_key", m.getSubject());
            Record r = bsonSerializer.deserialize(new ByteArrayInputStream(((Data) m.getBody()).getValue().getArray()));
            Assert.assertEquals("I'm a test message " + i,
                    r.getField(FieldDictionary.RECORD_VALUE).asString());
            Assert.assertEquals(r.getField(FieldDictionary.RECORD_KEY).asString(), m.getSubject());
            Assert.assertEquals("record/bson", m.getContentType());
            Assert.assertEquals(r.getId(), m.getMessageId().toString());
        }


    }



}


