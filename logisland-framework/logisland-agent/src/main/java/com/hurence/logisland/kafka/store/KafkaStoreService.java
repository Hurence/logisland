
package com.hurence.logisland.kafka.store;


import com.hurence.logisland.kafka.registry.KafkaRegistry;
import com.hurence.logisland.kafka.registry.KafkaRegistryConfig;
import com.hurence.logisland.kafka.serialization.Serializer;
import com.hurence.logisland.kafka.registry.exceptions.*;
import com.hurence.logisland.kafka.store.exceptions.*;
import org.apache.commons.collections.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Wraps a Kafka store and handle all the versionning,
 * id allocation and master forwarding stuff
 */
public class KafkaStoreService {

    private static final Logger log = LoggerFactory.getLogger(KafkaStoreService.class);

    protected final Object masterLock = new Object();

    private final Map<Integer, RegistryKey> guidToSchemaKey;
    private final Map<MD5, RegistryValue> valueHashToGuid;

    protected final KafkaStore<RegistryKey, RegistryValue> kafkaStore;
    protected final Serializer<RegistryKey, RegistryValue> serializer;
    protected final KafkaRegistry kafkaRegistry;
    protected final int kafkaStoreTimeoutMs;

    public KafkaStore<RegistryKey, RegistryValue> getKafkaStore() {
        return kafkaStore;
    }

    public KafkaStoreService(KafkaRegistry kafkaRegistry,
                             String kafkaStoreTopicConfig,
                             KafkaRegistryConfig config,
                             Serializer<RegistryKey, RegistryValue> serializer) throws RegistryException {

        this.kafkaStoreTimeoutMs = config.getInt(KafkaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG);
        this.kafkaRegistry = kafkaRegistry;
        this.serializer = serializer;
        this.guidToSchemaKey = new HashMap<>();
        this.valueHashToGuid = new HashMap<>();

        kafkaStore =
                new KafkaStore<>(
                        kafkaStoreTopicConfig,
                        config,
                        (key, value) -> {

                        },
                        this.serializer,
                        new InMemoryStore<>(),
                        new NoopKey());


    }


    public void init() throws RegistryInitializationException {
        try {
            kafkaStore.init();
        } catch (StoreInitializationException e) {
            throw new RegistryInitializationException(
                    "Error initializing kafka store while initializing schema registry", e);
        }
    }

    public void close() {
        log.info("Shutting down kafka store wrapper");
        kafkaStore.close();
    }


    public RegistryValue create(RegistryKey key, RegistryValue value) throws RegistryException {
        try {
            // Ensure cache is up-to-date before any potential writes
            kafkaStore.waitUntilKafkaReaderReachesLastOffset(kafkaStoreTimeoutMs);

            // see if the schema to be registered already exists
            MD5 md5 = MD5.ofString(value.toString());
            if (this.valueHashToGuid.containsKey(md5)) {
                return this.valueHashToGuid.get(md5);
            }

            kafkaStore.put(key, value);

        } catch (StoreTimeoutException te) {
            throw new RegistryTimeoutException("Write to the Kafka store timed out while", te);
        } catch (StoreException e) {
            throw new RegistryStoreException("Error while registering the schema in the" +
                    " backend Kafka store", e);
        }
        return value;
    }

    public RegistryValue createOrForward(RegistryKey key, RegistryValue value, Map<String, String> headerProperties) throws RegistryException {

        synchronized (masterLock) {
            if (kafkaRegistry.isMaster()) {
                return create(key, value);
            } else {
                // forward registering request to the master
                if (kafkaRegistry.masterIdentity() != null) {
                    return forwardCreateRequestToMaster(key, value, headerProperties);
                } else {
                    throw new UnknownMasterException("Register schema request failed since master is "
                            + "unknown");
                }
            }
        }
    }


    private RegistryValue forwardCreateRequestToMaster(RegistryKey key,
                                                       RegistryValue value,
                                           Map<String, String> headerProperties)
            throws RegistryRequestForwardingException {

        throw new RegistryRequestForwardingException("not implemented yet => time to do it ?");
        /*
        UrlList baseUrl = masterRestService.getBaseUrls();

        RegisterSchemaRequest registerSchemaRequest = new RegisterSchemaRequest();
        registerSchemaRequest.setSchema(schemaString);
        log.debug(String.format("Forwarding registering schema request %s to %s",
                registerSchemaRequest, baseUrl));
        try {
            int id = masterRestService.registerSchema(headerProperties, registerSchemaRequest, subject);
            return id;
        } catch (IOException e) {
            throw new SchemaRegistryRequestForwardingException(
                    String.format("Unexpected error while forwarding the registering schema request %s to %s",
                            registerSchemaRequest, baseUrl),
                    e);
        } catch (RestClientException e) {
            throw new RestException(e.getMessage(), e.getStatus(), e.getErrorCode(), e);
        }*/
    }


    public RegistryValue get(RegistryKey key) throws RegistryException {
        try {

            return kafkaStore.get(key);
        } catch (StoreException e) {
            throw new RegistryStoreException(
                    "Error while retrieving schema from the backend Kafka" +
                            " store", e);
        }

    }

    public List<RegistryValue> getAll() throws RegistryException {
        try {

            return  IteratorUtils.toList(kafkaStore.getAll(null,null));
        } catch (StoreException e) {
            throw new RegistryStoreException(
                    "Error while retrieving schema from the backend Kafka" +
                            " store", e);
        }

    }

    public void delete(RegistryKey key) throws RegistryException {
        try {
            kafkaStore.delete(key);

        } catch (StoreTimeoutException te) {
            throw new RegistryTimeoutException("Write to the Kafka store timed out while", te);
        } catch (StoreException e) {
            throw new RegistryStoreException("Error while registering the schema in the" +
                    " backend Kafka store", e);
        }
    }


    public RegistryValue update(RegistryKey key, RegistryValue newValue) throws RegistryException {
        try {
            // Ensure cache is up-to-date before any potential writes
            kafkaStore.waitUntilKafkaReaderReachesLastOffset(kafkaStoreTimeoutMs);
            kafkaStore.put(key, newValue);
        } catch (StoreTimeoutException te) {
            throw new RegistryTimeoutException("Write to the Kafka store timed out while", te);
        } catch (StoreException e) {
            throw new RegistryStoreException("Error while registering the schema in the" +
                    " backend Kafka store", e);
        }
        return newValue;
    }


}
