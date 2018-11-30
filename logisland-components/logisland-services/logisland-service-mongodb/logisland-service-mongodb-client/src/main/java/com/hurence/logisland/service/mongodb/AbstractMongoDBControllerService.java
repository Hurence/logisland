/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.mongodb;

import com.hurence.logisland.annotation.lifecycle.OnStopped;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ConfigurationContext;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.Collections;

public abstract class AbstractMongoDBControllerService extends AbstractControllerService {

    static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
    static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
    static final String WRITE_CONCERN_FSYNCED = "FSYNCED";
    static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
    static final String WRITE_CONCERN_REPLICA_ACKNOWLEDGED = "REPLICA_ACKNOWLEDGED";
    static final String WRITE_CONCERN_MAJORITY = "MAJORITY";

    protected static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("mongo.uri")
            .displayName("Mongo URI")
            .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(Validation.DOCUMENT_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("mongo.db.name")
            .displayName("Mongo Database Name")
            .description("The name of the database to use")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("mongo.collection.name")
            .displayName("Mongo Collection Name")
            .description("The name of the collection to use")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /*
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("ssl-client-auth")
            .displayName("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();
*/
    public static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
            .name("mongo.write.concern")
            .displayName("Write Concern")
            .description("The write concern to use")
            .required(true)
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_FSYNCED, WRITE_CONCERN_JOURNALED,
                    WRITE_CONCERN_REPLICA_ACKNOWLEDGED, WRITE_CONCERN_MAJORITY)
            .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
            .build();

    public static final PropertyDescriptor UPSERT_CONDITION = new PropertyDescriptor.Builder()
            .name("mongo.bulk.upsert.condition")
            .displayName("Upsert condition")
            .description("A custom condition for the bulk upsert (Filter for the bulkwrite). " +
                    "If not specified the standard condition is to match same id ('_id': data._id)")
            .required(false)
            .defaultValue("${'{ \"_id\" :\"' + record_id + '\"}'}")
            .expressionLanguageSupported(true)
            .build();


    public static final AllowableValue BULK_MODE_INSERT = new AllowableValue("insert", "Insert", "Insert records whose key must be unique");
    public static final AllowableValue BULK_MODE_UPSERT = new AllowableValue("upsert", "Insert or Update",
            "Insert records if not already existing or update the record if already existing");


    public static final PropertyDescriptor BULK_MODE = new PropertyDescriptor.Builder()
            .name("mongo.bulk.mode")
            .description("Bulk mode (insert or upsert)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(BULK_MODE_INSERT, BULK_MODE_UPSERT)
            .defaultValue(BULK_MODE_INSERT.getValue())
            .build();




    protected MongoClient mongoClient;

    protected final void createClient(ControllerServiceInitializationContext context) throws IOException {
        if (mongoClient != null) {
            closeClient();
        }

        getLogger().info("Creating MongoClient");

        // Set up the client for secure (SSL/TLS communications) if configured to do so
        /*final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String rawClientAuth = context.getProperty(CLIENT_AUTH).getValue();*/
        final SSLContext sslContext = null;

       /* if (sslService != null) {
            final SSLContextService.ClientAuth clientAuth;
            if (StringUtils.isBlank(rawClientAuth)) {
                clientAuth = SSLContextService.ClientAuth.REQUIRED;
            } else {
                try {
                    clientAuth = SSLContextService.ClientAuth.valueOf(rawClientAuth);
                } catch (final IllegalArgumentException iae) {
                    throw new ProviderCreationException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                            rawClientAuth, StringUtils.join(SslContextFactory.ClientAuth.values(), ", ")));
                }
            }
            sslContext = sslService.createSSLContext(clientAuth);
        } else {
            sslContext = null;
        }*/

        try {
            if (sslContext == null) {
                mongoClient = new MongoClient(new MongoClientURI(getURI(context)));
            } else {
                mongoClient = new MongoClient(new MongoClientURI(getURI(context), getClientOptions(sslContext)));
            }
        } catch (Exception e) {
            getLogger().error("Failed to schedule {} due to {}", new Object[]{this.getClass().getName(), e}, e);
            throw e;
        }
    }

    protected Builder getClientOptions(final SSLContext sslContext) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.sslEnabled(true);
        builder.socketFactory(sslContext.getSocketFactory());
        return builder;
    }

    @OnStopped
    public final void closeClient() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    protected MongoDatabase getDatabase(final ConfigurationContext context) {
        return getDatabase(context, null);
    }

    protected MongoDatabase getDatabase(final ConfigurationContext context, final Record record) {
        final String databaseName = context.getPropertyValue(DATABASE_NAME).evaluate(record).asString();
        return mongoClient.getDatabase(databaseName);
    }

    protected MongoCollection<Document> getCollection(final ConfigurationContext context) {
        return getCollection(context, null);
    }

    protected MongoCollection<Document> getCollection(final ConfigurationContext context, final Record record) {
        final String collectionName = context.getPropertyValue(COLLECTION_NAME).evaluate(record).asString();
        return getDatabase(context, record).getCollection(collectionName);
    }

    protected String getURI(final ControllerServiceInitializationContext context) {
        return context.getPropertyValue(URI).evaluate(Collections.emptyMap()).asString();
    }

}
