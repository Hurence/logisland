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
package com.hurence.logisland.redis.service;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.redis.util.RedisAction;
import com.hurence.logisland.redis.util.RedisUtils;
import com.hurence.logisland.serializer.*;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.util.Tuple;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * Created by oalam on 23/05/2018.
 * <p>
 * <p>This an implementation of an high-performance "maybe-distributed" cache using Redis
 * It will cache every item automatically with put method. You just have to use get method
 * to retrieve cached object.</p>
 * <p>
 * <p>You specify default TTL </p>
 */
@Tags({"cache", "service", "key", "value", "pair", "redis"})
@CapabilityDescription("A controller service for caching records by key value pair with LRU (last recently used) strategy. using LinkedHashMap")
public class RedisKeyValueCacheService extends AbstractControllerService implements DatastoreClientService, CacheService<String, Record, Long> {

    private volatile RecordSerializer recordSerializer;
    private final Serializer<String> stringSerializer = new StringSerializer();
    private volatile RedisConnectionPool redisConnectionPool;


    public static final AllowableValue AVRO_SERIALIZER = new AllowableValue(AvroSerializer.class.getName(),
            "avro serialization", "serialize events as avro blocs");
    public static final AllowableValue JSON_SERIALIZER = new AllowableValue(JsonSerializer.class.getName(),
            "avro serialization", "serialize events as json blocs");
    public static final AllowableValue KRYO_SERIALIZER = new AllowableValue(KryoSerializer.class.getName(),
            "kryo serialization", "serialize events as json blocs");
    public static final AllowableValue BYTESARRAY_SERIALIZER = new AllowableValue(BytesArraySerializer.class.getName(),
            "byte array serialization", "serialize events as byte arrays");
    public static final AllowableValue KURA_PROTOCOL_BUFFER_SERIALIZER = new AllowableValue(KuraProtobufSerializer.class.getName(),
            "Kura Protobuf serialization", "serialize events as Kura protocol buffer");
    public static final AllowableValue NO_SERIALIZER = new AllowableValue("none", "no serialization", "send events as bytes");


    public static final PropertyDescriptor RECORD_SERIALIZER = new PropertyDescriptor.Builder()
            .name("record.recordSerializer")
            .description("the way to serialize/deserialize the record")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(KRYO_SERIALIZER, JSON_SERIALIZER, AVRO_SERIALIZER, BYTESARRAY_SERIALIZER, KURA_PROTOCOL_BUFFER_SERIALIZER, NO_SERIALIZER)
            .defaultValue(JSON_SERIALIZER.getValue())
            .build();


    public static final PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor.Builder()
            .name("record.avro.schema")
            .description("the avro schema definition")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            this.redisConnectionPool = new RedisConnectionPool();
            this.redisConnectionPool.init(context);
            this.recordSerializer = getSerializer(
                    context.getPropertyValue(RECORD_SERIALIZER).asString(),
                    context.getPropertyValue(AVRO_SCHEMA).asString());
        } catch (Exception e) {
            throw new InitializationException(e);
        }
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> properties = new ArrayList<>(RedisUtils.REDIS_CONNECTION_PROPERTY_DESCRIPTORS);
        properties.add(RECORD_SERIALIZER);

        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return RedisUtils.validate(validationContext);
    }

    @Override
    public Record get(String key) {
        try {
            return get(key, stringSerializer, (Deserializer<Record>) recordSerializer);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void set(String key, Record value) {
        try {
            put(key, value,stringSerializer, (Serializer<Record>) recordSerializer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void set(String key, Long score, Record value) {
        try {
            put(key, score, value,stringSerializer, (Serializer<Record>) recordSerializer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Record> get(String key, Long min, Long max, Long limit) {
        try {
            return get(key, min, max, limit, stringSerializer, (Deserializer<Record>) recordSerializer);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    public <String, Record> List<Record> get(final String key, final Long min, final Long max, final Long limit, final Serializer<String> keySerializer, final Deserializer<Record> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            Set<byte[]> res = redisConnection.zRangeByScore(k, min, max, 0, limit);
            List<Record> resList = new ArrayList<>();
            for(byte[] elem : res){
                InputStream input = new ByteArrayInputStream(elem);
                resList.add(valueDeserializer.deserialize(input));
            }
            if(resList.isEmpty()){
                return null;
            }else{
                return resList;
            }

        });
    }

    public <String, Record> void put(final String key, final Long score, final Record value, final Serializer<String> keySerializer, final Serializer<Record> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            redisConnection.zAdd(kv.getKey(), score, kv.getValue());
            return null;
        });
    }


    protected Cache<String, Record> createCache(final ControllerServiceInitializationContext context) throws IOException, InterruptedException {
        final int capacity = context.getPropertyValue(CACHE_SIZE).asInteger();
        return new LRUCache<String, Record>(capacity);
    }


    public <String, Record> boolean putIfAbsent(final String key, final Record value, final Serializer<String> keySerializer, final Serializer<Record> valueSerializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            return redisConnection.setNX(kv.getKey(), kv.getValue());
        });
    }

    public <String, Record> Record getAndPutIfAbsent(final String key, final Record value, final Serializer<String> keySerializer, final Serializer<Record> valueSerializer, final Deserializer<Record> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            do {
                // start a watch on the key and retrieve the current value
                redisConnection.watch(kv.getKey());
                final byte[] existingValue = redisConnection.get(kv.getKey());

                // start a transaction and perform the put-if-absent
                redisConnection.multi();
                redisConnection.setNX(kv.getKey(), kv.getValue());

                // execute the transaction
                final List<Object> results = redisConnection.exec();

                // if the results list was empty, then the transaction failed (i.e. key was modified after we started watching), so keep looping to retry
                // if the results list has results, then the transaction succeeded and it should have the result of the setNX operation
                if (results.size() > 0) {
                    final Object firstResult = results.get(0);
                    if (firstResult instanceof Boolean) {
                        final Boolean absent = (Boolean) firstResult;

                        if(absent){
                            return null;
                        }else {
                            InputStream input = new ByteArrayInputStream(existingValue);
                            return valueDeserializer.deserialize(input);
                        }
                    } else {
                        // this shouldn't really happen, but just in case there is a non-boolean result then bounce out of the loop
                        throw new IOException("Unexpected result from Redis transaction: Expected Boolean result, but got "
                                + firstResult.getClass().getName() + " with value " + firstResult.toString());
                    }
                }
            } while (isEnabled());

            return null;
        });
    }


    public <String> boolean containsKey(final String key, final Serializer<String> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            return redisConnection.exists(k);
        });
    }

    public <String, Record> void put(final String key, final Record value, final Serializer<String> keySerializer, final Serializer<Record> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            redisConnection.set(kv.getKey(), kv.getValue());
            return null;
        });
    }


    public <String, Record> Record get(final String key, final Serializer<String> keySerializer, final Deserializer<Record> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final byte[] v = redisConnection.get(k);
            if (v == null) {
                return null;
            }else {
                InputStream input = new ByteArrayInputStream(v);
                return valueDeserializer.deserialize(input);
            }
        });
    }

    public void close() throws IOException {
        try {
            if (this.redisConnectionPool != null)
                this.redisConnectionPool.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public <String> boolean remove(final String key, final Serializer<String> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final long numRemoved = redisConnection.del(k);
            return numRemoved > 0;
        });
    }

    public long removeByPattern(final java.lang.String regex) throws IOException {
        return withConnection(redisConnection -> {
            long deletedCount = 0;
            final List<byte[]> batchKeys = new ArrayList<>();

            // delete keys in batches of 1000 using the cursor
            final Cursor<byte[]> cursor = redisConnection.scan(ScanOptions.scanOptions().count(100).match(regex).build());
            while (cursor.hasNext()) {
                batchKeys.add(cursor.next());

                if (batchKeys.size() == 1000) {
                    deletedCount += redisConnection.del(getKeys(batchKeys));
                    batchKeys.clear();
                }
            }

            // delete any left-over keys if some were added to the batch but never reached 1000
            if (batchKeys.size() > 0) {
                deletedCount += redisConnection.del(getKeys(batchKeys));
                batchKeys.clear();
            }

            return deletedCount;
        });
    }

    /**
     * Convert the list of all keys to an array.
     */
    private byte[][] getKeys(final List<byte[]> keys) {
        final byte[][] allKeysArray = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            allKeysArray[i] = keys.get(i);
        }
        return allKeysArray;
    }


    private <K, Record> Tuple<byte[], byte[]> serialize(final K key, final Record value, final Serializer<K> keySerializer, final Serializer<Record> valueSerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] k = null;

        try {
            keySerializer.serialize(out, key);
            k= out.toByteArray();
        }catch (Throwable t){
            // do nothing
        }
        out.reset();

        byte[] v = null;
        try {
            valueSerializer.serialize(out, value);
            v = out.toByteArray();
        }catch (Throwable t){
            // do nothing
        }

        return new Tuple<>(k, v);
    }

    private <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(out, key);
        return out.toByteArray();
    }

    private <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = redisConnectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    getLogger().warn("Error closing connection: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * build a recordSerializer
     *
     * @param inSerializerClass the recordSerializer type
     * @param schemaContent     an Avro schema
     * @return the recordSerializer
     */
    private RecordSerializer getSerializer(String inSerializerClass, String schemaContent) {

        if (inSerializerClass.equals(AVRO_SERIALIZER.getValue())) {
            Schema.Parser parser = new Schema.Parser();
            Schema inSchema = parser.parse(schemaContent);
            new AvroSerializer(inSchema);
        } else if (inSerializerClass.equals(JSON_SERIALIZER.getValue())) {
            return new JsonSerializer();
        } else if (inSerializerClass.equals(BYTESARRAY_SERIALIZER.getValue())) {
            return new BytesArraySerializer();
        } else if (inSerializerClass.equals(KURA_PROTOCOL_BUFFER_SERIALIZER.getValue())) {
            return new KuraProtobufSerializer();
        }
            return new KryoSerializer(true);

    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {

    }

    @Override
    public void dropCollection(String name) throws DatastoreClientServiceException {

    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        return 0;
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        return false;
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {

    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {

    }

    @Override
    public void createAlias(String collection, String alias) throws DatastoreClientServiceException {

    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        return false;
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {

    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        set(record.getId(),record);
        String metricId = record.getField("metricId").asString();
        if(metricId != null && !metricId.isEmpty()){
            long currentTimestamp = (new Date()).getTime() / 1000;
            set(metricId,currentTimestamp, record);
        }
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        set(record.getId(),record);
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        try {
            remove(record.getId(),stringSerializer);
        } catch (IOException e) {
            getLogger().warn("Error removing record : " + e.getMessage(), e);
        }

    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
        return null;
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        return get(record.getId());
    }

    @Override
    public Collection<Record> query(String query) {
        return null;
    }

    @Override
    public long queryCount(String query) {
        return 0;
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(OutputStream output, String value) throws SerializationException, IOException {
            if (value != null) {
                output.write(value.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(InputStream input) throws DeserializationException, IOException {
            byte[] bytes = IOUtils.toByteArray(input);
            return input == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }
    }
}



