package com.hurence.logisland.kakfa.registry;

import io.confluent.kafka.schemaregistry.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogislandKafkaStoreMessageHandler
        implements StoreUpdateHandler<SchemaRegistryKey, SchemaRegistryValue> {

    private static final Logger log = LoggerFactory.getLogger(LogislandKafkaStoreMessageHandler.class);
    private final LogislandKafkaRegistry schemaRegistry;

    public LogislandKafkaStoreMessageHandler(LogislandKafkaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    /**
     * Invoked on every new schema written to the Kafka store
     *
     * @param key    Key associated with the schema.
     * @param schema Schema written to the Kafka store
     */
    @Override
    public void handleUpdate(SchemaRegistryKey key, SchemaRegistryValue schema) {
        if (key.getKeyType() == SchemaRegistryKeyType.SCHEMA) {
            SchemaValue schemaObj = (SchemaValue) schema;
            SchemaKey schemaKey = (SchemaKey) key;
            schemaRegistry.guidToSchemaKey.put(schemaObj.getId(), schemaKey);

            // Update the maximum id seen so far
            if (schemaRegistry.getMaxIdInKafkaStore() < schemaObj.getId()) {
                schemaRegistry.setMaxIdInKafkaStore(schemaObj.getId());
            }

            MD5 md5 = MD5.ofString(schemaObj.getSchema());
            SchemaIdAndSubjects schemaIdAndSubjects = schemaRegistry.schemaHashToGuid.get(md5);
            if (schemaIdAndSubjects == null) {
                schemaIdAndSubjects = new SchemaIdAndSubjects(schemaObj.getId());
            }
            schemaIdAndSubjects.addSubjectAndVersion(schemaKey.getSubject(), schemaKey.getVersion());
            schemaRegistry.schemaHashToGuid.put(md5, schemaIdAndSubjects);
        }
    }
}
