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
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.redis.util.RedisAction;
import com.hurence.logisland.redis.util.RedisUtils;
import com.hurence.logisland.serializer.Deserializer;
import com.hurence.logisland.serializer.Serializer;
import com.hurence.logisland.service.cache.CacheService;
import com.hurence.logisland.service.cache.model.Cache;
import com.hurence.logisland.service.cache.model.LRUCache;
import com.hurence.logisland.util.Tuple;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


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
public class RedisKeyValueCacheService extends AbstractControllerService implements CacheService<String, Record> {


    private volatile RedisConnectionPool redisConnectionPool;


    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            this.redisConnectionPool = new RedisConnectionPool();
            this.redisConnectionPool.init(context);
        } catch (Exception e) {
            throw new InitializationException(e);
        }
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return RedisUtils.REDIS_CONNECTION_PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return RedisUtils.validate(validationContext);
    }

    @Override
    public Record get(String string) {
        return null;
    }

    @Override
    public void set(String string, Record record) {

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
                        return absent ? null : valueDeserializer.deserialize(existingValue);
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
            return valueDeserializer.deserialize(v);
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

        keySerializer.serialize(key, out);
        final byte[] k = out.toByteArray();

        out.reset();

        valueSerializer.serialize(value, out);
        final byte[] v = out.toByteArray();

        return new Tuple<>(k, v);
    }

    private <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
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


}
