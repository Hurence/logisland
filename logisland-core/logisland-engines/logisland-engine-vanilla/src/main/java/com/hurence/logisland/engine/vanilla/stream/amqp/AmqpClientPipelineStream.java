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

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.serializer.BsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.stream.AbstractRecordStream;
import com.hurence.logisland.stream.StreamContext;
import io.vertx.core.Vertx;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.proton.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.message.Message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class AmqpClientPipelineStream extends AbstractRecordStream {

    private ProtonConnection protonConnection;
    private ProtonSender sender;
    private ProtonReceiver receiver;
    private ProtonClientOptions options;
    private RecordSerializer serializer;
    private RecordSerializer deserializer;
    private StreamContext streamContext;
    private String contentType;
    private ConnectionControl connectionControl;
    private Vertx vertx;
    private ProtonClient protonClient;

    private byte[] extractBodyContent(Section body) {
        if (body instanceof AmqpValue) {
            return ((AmqpValue) body).getValue().toString().getBytes();
        } else if (body instanceof Data) {
            return ((Data) body).getValue().getArray();
        } else {
            throw new IllegalArgumentException("Unsupported section type " + body.getType().name());
        }
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
                StreamOptions.CONNECTION_HOST,
                StreamOptions.CONNECTION_PORT,
                StreamOptions.LINK_CREDITS,
                StreamOptions.CONNECTION_AUTH_USERNAME,
                StreamOptions.CONNECTION_AUTH_PASSWORD,
                StreamOptions.CONNECTION_AUTH_TLS_CERT,
                StreamOptions.CONNECTION_AUTH_TLS_KEY,
                StreamOptions.CONNECTION_AUTH_CA_CERT,
                StreamOptions.READ_TOPIC,
                StreamOptions.READ_TOPIC_SERIALIZER,
                StreamOptions.AVRO_INPUT_SCHEMA,
                StreamOptions.WRITE_TOPIC,
                StreamOptions.WRITE_TOPIC_SERIALIZER,
                StreamOptions.AVRO_OUTPUT_SCHEMA,
                StreamOptions.CONTAINER_ID,
                StreamOptions.WRITE_TOPIC_CONTENT_TYPE,
                StreamOptions.CONNECTION_RECONNECT_BACKOFF,
                StreamOptions.CONNECTION_RECONNECT_INITIAL_DELAY,
                StreamOptions.CONNECTION_RECONNECT_MAX_DELAY
        );
    }

    @Override
    public void init(ComponentContext context) {
        try {
            super.init(context);
            vertx = Vertx.vertx();
            options = new ProtonClientOptions();
            protonClient = ProtonClient.create(vertx);

            streamContext = (StreamContext) context;
            if (streamContext.getPropertyValue(StreamOptions.READ_TOPIC_SERIALIZER).asString().equals(StreamOptions.NO_SERIALIZER.getValue())) {
                deserializer = null;
            } else {
                deserializer = buildSerializer(streamContext.getPropertyValue(StreamOptions.READ_TOPIC_SERIALIZER).asString(),
                        streamContext.getPropertyValue(StreamOptions.AVRO_INPUT_SCHEMA).asString());
            }
            serializer = buildSerializer(streamContext.getPropertyValue(StreamOptions.WRITE_TOPIC_SERIALIZER).asString(),
                    streamContext.getPropertyValue(StreamOptions.AVRO_OUTPUT_SCHEMA).asString());

            contentType = streamContext.getPropertyValue(StreamOptions.WRITE_TOPIC_CONTENT_TYPE).asString();

            ControllerServiceLookup controllerServiceLookup = streamContext.getControllerServiceLookup();
            for (ProcessContext processContext : streamContext.getProcessContexts()) {
                if (processContext.getProcessor().hasControllerService()) {
                    processContext.setControllerServiceLookup(controllerServiceLookup);
                }
                processContext.getProcessor().init(processContext);
            }
        } catch (InitializationException ie) {
            throw new IllegalStateException("Unable to initialize processor pipeline", ie);
        }
    }

    @Override
    public void start() {
        connectionControl = new ConnectionControl(
                streamContext.getPropertyValue(StreamOptions.CONNECTION_RECONNECT_MAX_DELAY).asLong(),
                streamContext.getPropertyValue(StreamOptions.CONNECTION_RECONNECT_INITIAL_DELAY).asLong(),
                streamContext.getPropertyValue(StreamOptions.CONNECTION_RECONNECT_BACKOFF).asDouble());
        for (ProcessContext processContext : streamContext.getProcessContexts()) {
            processContext.getProcessor().start();
        }
        try {
            setupConnection();
        } catch (Throwable t) {
            throw new IllegalStateException("Unable to start stream", t);
        }

        super.start();
    }

    private CompletableFuture<ProtonConnection> setupConnection() {
        CompletableFuture<ProtonConnection> completableFuture = new CompletableFuture<>();
        String hostname = streamContext.getPropertyValue(StreamOptions.CONNECTION_HOST).asString();
        int port = streamContext.getPropertyValue(StreamOptions.CONNECTION_PORT).asInteger();
        int credits = streamContext.getPropertyValue(StreamOptions.LINK_CREDITS).asInteger();

        String user = streamContext.getPropertyValue(StreamOptions.CONNECTION_AUTH_USERNAME).asString();
        String password = streamContext.getPropertyValue(StreamOptions.CONNECTION_AUTH_PASSWORD).asString();
        if (user != null && password != null) {
            options.addEnabledSaslMechanism("PLAIN");
        } else if (streamContext.getPropertyValue(StreamOptions.CONNECTION_AUTH_TLS_CERT).isSet()) {
            String tlsCert = streamContext.getPropertyValue(StreamOptions.CONNECTION_AUTH_TLS_CERT).asString();
            String tlsKey = streamContext.getPropertyValue(StreamOptions.CONNECTION_AUTH_TLS_KEY).asString();
            String caCert = streamContext.getPropertyValue(StreamOptions.CONNECTION_AUTH_CA_CERT).asString();
            options.addEnabledSaslMechanism("EXTERNAL")
                    .setHostnameVerificationAlgorithm("")
                    .setPemKeyCertOptions(new PemKeyCertOptions()
                            .addCertPath(new File(tlsCert).getAbsolutePath())
                            .addKeyPath(new File(tlsKey).getAbsolutePath()));
            if (caCert != null) {
                options.setPemTrustOptions(new PemTrustOptions()
                        .addCertPath(new File(caCert).getAbsolutePath()));
            }

        }
        protonClient.connect(options, hostname, port, user, password, event -> {
            if (event.failed()) {
                handleConnectionFailure(false);
                completableFuture.completeExceptionally(event.cause());
                return;
            }
            connectionControl.connected();
            completableFuture.complete(event.result());
            protonConnection = event.result();
            String containerId = streamContext.getPropertyValue(StreamOptions.CONTAINER_ID).asString();
            if (containerId != null) {
                protonConnection.setContainer(containerId);
            }
            protonConnection
                    .closeHandler(x -> {
                        handleConnectionFailure(true);
                    })
                    .disconnectHandler(x -> {
                        handleConnectionFailure(false);
                    })
                    .openHandler(onOpen -> {


                        //setup the output path
                        sender = protonConnection.createSender(streamContext.getPropertyValue(StreamOptions.WRITE_TOPIC).asString());
                        sender.setAutoDrained(true);
                        sender.setAutoSettle(true);
                        sender.open();

                        //setup the input path
                        receiver = protonConnection.createReceiver(streamContext.getPropertyValue(StreamOptions.READ_TOPIC).asString());
                        receiver.setPrefetch(credits);
                        receiver.handler((delivery, message) -> {
                            try {
                                Record record;
                                if (deserializer == null) {
                                    record = RecordUtils.getKeyValueRecord(StringUtils.defaultIfEmpty(message.getSubject(), ""), new String(extractBodyContent(message.getBody())));
                                } else {
                                    record = deserializer.deserialize(new ByteArrayInputStream(extractBodyContent(message.getBody())));
                                    if (!record.hasField(FieldDictionary.RECORD_KEY)) {
                                        record.setField(FieldDictionary.RECORD_KEY, FieldType.STRING, message.getSubject());
                                    }
                                }


                                Collection<Record> r = Collections.singleton(record);
                                for (ProcessContext processContext : streamContext.getProcessContexts()) {
                                    r = processContext.getProcessor().process(processContext, r);
                                }
                                List<Message> toAdd = new ArrayList<>();
                                for (Record out : r) {
                                    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                                    serializer.serialize(byteOutputStream, out);
                                    Message mo = ProtonHelper.message();
                                    if (out.hasField(FieldDictionary.RECORD_KEY)) {
                                        mo.setSubject(out.getField(FieldDictionary.RECORD_KEY).asString());
                                    }
                                    if (StringUtils.isNotBlank(contentType)) {
                                        mo.setContentType(contentType);
                                    }
                                    mo.setMessageId(out.getId());
                                    mo.setBody(new Data(Binary.create(ByteBuffer.wrap(byteOutputStream.toByteArray()))));
                                    toAdd.add(mo);
                                }
                                toAdd.forEach(sender::send);
                                delivery.disposition(Accepted.getInstance(), true);
                            } catch (Exception e) {
                                Rejected rejected = new Rejected();
                                delivery.disposition(rejected, true);
                                getLogger().warn("Unable to process message : " + e.getMessage());
                            }
                        }).open();

                    }).open();

        });
        return completableFuture;
    }

    @Override
    public void stop() {
        if (streamContext!=null && streamContext.getProcessContexts() != null) {
            for (ProcessContext processContext : streamContext.getProcessContexts()) {
                processContext.getProcessor().stop();
            }
        }
        if (connectionControl != null) {
            connectionControl.setRunning(false);
        }
        try {
            if (receiver != null) {
                receiver.close();
            }
        } catch (Exception e) {
            getLogger().warn("Unable to complete receiver drain", e);
        }

        if (sender != null) {
            sender.close();
        }
        if (protonConnection != null) {
            try {
                protonConnection.close();
                protonConnection.disconnect();
            } catch (Exception e) {
                getLogger().warn("Unable to properly clear the connection");
            } finally {
                protonConnection = null;
            }
        }
        if (vertx != null) {
            vertx.close();
        }
        super.stop();
    }



    private void handleConnectionFailure(boolean remoteClose) {
        try {
            if (protonConnection != null) {
                protonConnection.closeHandler(null);
                protonConnection.disconnectHandler(null);

                if (remoteClose) {
                    protonConnection.close();
                    protonConnection.disconnect();
                }
            }
        } finally {
            if (connectionControl.shouldReconnect()) {
                connectionControl.scheduleReconnect((vertx) -> CompletableFuture.supplyAsync(this::setupConnection));


            }
        }
    }


    /**
     * build a serializer
     *
     * @param inSerializerClass the serializer type
     * @param schemaContent     an Avro schema
     * @return the serializer
     */
    private RecordSerializer buildSerializer(String inSerializerClass, String schemaContent) {
        if (BsonSerializer.class.getName().equals(inSerializerClass)) {
            return new BsonSerializer();
        }
        return SerializerProvider.getSerializer(inSerializerClass, schemaContent);
    }
}
