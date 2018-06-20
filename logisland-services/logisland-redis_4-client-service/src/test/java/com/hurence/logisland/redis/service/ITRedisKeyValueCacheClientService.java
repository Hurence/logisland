/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.redis.service;


import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.*;
import com.hurence.logisland.redis.util.RedisUtils;
import com.hurence.logisland.serializer.DeserializationException;
import com.hurence.logisland.serializer.Deserializer;
import com.hurence.logisland.serializer.SerializationException;
import com.hurence.logisland.serializer.Serializer;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public class ITRedisKeyValueCacheClientService {

    public static final String SERVICE_IDENTIFIER = "redis-map-cache-client";
    private TestRedisProcessor proc;
    private TestRunner testRunner;
    private RedisServer redisServer;
    private RedisKeyValueCacheService redisMapCacheClientService;
    private int redisPort;

    @Before
    public void setup() throws IOException {
        this.redisPort = getAvailablePort();

        this.redisServer = new RedisServer(redisPort);
        this.redisServer = new RedisServer(new File("C:\\temp\\soft\\redis\\redis-2.8.19\\redis-server.exe"), 6379);

        redisServer.start();

        proc = new TestRedisProcessor();
        testRunner = TestRunners.newTestRunner(proc);
    }

    private int getAvailablePort() throws IOException {
        try (SocketChannel socket = SocketChannel.open()) {
            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            socket.bind(new InetSocketAddress("localhost", 0));
            return socket.socket().getLocalPort();
        }
    }

    @After
    public void teardown() throws IOException {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    public void testStandaloneRedis() throws InitializationException, IOException, InterruptedException {
        try {
            // create, configure, and enable the RedisConnectionPool service
            redisMapCacheClientService = new RedisKeyValueCacheService();
            redisMapCacheClientService.setIdentifier(SERVICE_IDENTIFIER);


            testRunner.setProperty(RedisUtils.CONNECTION_STRING, "localhost:" + redisPort);
            testRunner.setProperty(RedisUtils.REDIS_MODE, RedisUtils.REDIS_MODE_STANDALONE);
            testRunner.setProperty(RedisUtils.DATABASE, "0");
            testRunner.setProperty(RedisUtils.COMMUNICATION_TIMEOUT, "10 seconds");

            testRunner.setProperty(RedisUtils.POOL_MAX_TOTAL, "8");
            testRunner.setProperty(RedisUtils.POOL_MAX_IDLE, "8");
            testRunner.setProperty(RedisUtils.POOL_MIN_IDLE, "0");
            testRunner.setProperty(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED, "true");
            testRunner.setProperty(RedisUtils.POOL_MAX_WAIT_TIME, "10 seconds");
            testRunner.setProperty(RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME, "60 seconds");
            testRunner.setProperty(RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS, "30 seconds");
            testRunner.setProperty(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN, "-1");
            testRunner.setProperty(RedisUtils.POOL_TEST_ON_CREATE, "false");
            testRunner.setProperty(RedisUtils.POOL_TEST_ON_BORROW, "false");
            testRunner.setProperty(RedisUtils.POOL_TEST_ON_RETURN, "false");
            testRunner.setProperty(RedisUtils.POOL_TEST_WHILE_IDLE, "true");

            testRunner.setProperty(RedisKeyValueCacheService.RECORD_SERIALIZER, "com.hurence.logisland.serializer.JsonSerializer");
            testRunner.addControllerService(SERVICE_IDENTIFIER, redisMapCacheClientService);

            // uncomment this to test using a different database index than the default 0
            //testRunner.setProperty(redisConnectionPool, RedisUtils.DATABASE, "1");

            // uncomment this to test using a password to authenticate to redis
            //testRunner.setProperty(redisConnectionPool, RedisUtils.PASSWORD, "foobared");

            testRunner.enableControllerService(redisMapCacheClientService);

            setupRedisMapCacheClientService();
            executeProcessor();
        } finally {
            if (redisMapCacheClientService != null) {
                redisMapCacheClientService.close();
            }
        }
    }

    private void setupRedisMapCacheClientService() throws InitializationException {
        // create, configure, and enable the RedisDistributedMapCacheClient service
        redisMapCacheClientService = new RedisKeyValueCacheService();
        redisMapCacheClientService.setIdentifier(SERVICE_IDENTIFIER);

        testRunner.addControllerService(SERVICE_IDENTIFIER, redisMapCacheClientService);
        //  testRunner.setProperty(redisMapCacheClientService, RedisKeyValueCacheService.REDIS_CONNECTION_POOL, "redis-connection-pool");
        testRunner.enableControllerService(redisMapCacheClientService);
        testRunner.setProperty(TestRedisProcessor.REDIS_MAP_CACHE, "redis-map-cache-client");
    }


    private Collection<Record> getRandomMetrics(int size) throws InterruptedException {

        List<Record> records = new ArrayList<>();
        Random rnd = new Random();
        long now = System.currentTimeMillis();

        String[] metricsType = {"disk.io", "cpu.wait", "io.wait"};
        String[] hosts = {"host1", "host2", "host3"};
        for (int i = 0; i < size; i++) {
            records.add(new StandardRecord(RecordDictionary.METRIC)
                    .setStringField(FieldDictionary.RECORD_NAME, metricsType[rnd.nextInt(3)])
                    .setStringField("host", hosts[rnd.nextInt(3)])
                    .setField(FieldDictionary.RECORD_TIME, FieldType.LONG, new Date().getTime())
                    .setField(FieldDictionary.RECORD_VALUE, FieldType.DOUBLE, 100.0 * Math.random())
                    .setTime(now)
            );
            now += rnd.nextInt(500);
        }

        return records;
    }


    private void executeProcessor() throws InterruptedException {
        // queue a flow file to trigger the processor and executeProcessor it
        testRunner.enqueue(getRandomMetrics(10));
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
    }

    /**
     * Test processor that exercises RedisDistributedMapCacheClient.
     */
    private static class TestRedisProcessor extends AbstractProcessor {

        public static final PropertyDescriptor REDIS_MAP_CACHE = new PropertyDescriptor.Builder()
                .name("redis-map-cache")
                .displayName("Redis Map Cache")
                .identifiesControllerService(RedisKeyValueCacheService.class)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(true)
                .build();


        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(REDIS_MAP_CACHE);
        }


        @Override
        public Collection<Record> process(ProcessContext context, Collection<Record> records) {
            if (records.isEmpty()) {
                return records;
            }

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final Serializer<String> stringSerializer = new StringSerializer();
            final Deserializer<String> stringDeserializer = new StringDeserializer();

            final RedisKeyValueCacheService cacheClient = context.getPropertyValue(REDIS_MAP_CACHE).asControllerService(RedisKeyValueCacheService.class);

            try {
                final long timestamp = System.currentTimeMillis();
                final String key = "test-redis-processor-" + timestamp;
                final String value = "the time is " + timestamp;

                // verify the key doesn't exists, put the key/value, then verify it exists
                Assert.assertFalse(cacheClient.containsKey(key, stringSerializer));
                cacheClient.put(key, value, stringSerializer, stringSerializer);
                Assert.assertTrue(cacheClient.containsKey(key, stringSerializer));

                // verify get returns the expected value we set above
                final String retrievedValue = cacheClient.get(key, stringSerializer, stringDeserializer);
                Assert.assertEquals(value, retrievedValue);

                // verify remove removes the entry and contains key returns false after
                Assert.assertTrue(cacheClient.remove(key, stringSerializer));
                Assert.assertFalse(cacheClient.containsKey(key, stringSerializer));

                // verify putIfAbsent works the first time and returns false the second time
                Assert.assertTrue(cacheClient.putIfAbsent(key, value, stringSerializer, stringSerializer));
                Assert.assertFalse(cacheClient.putIfAbsent(key, "some other value", stringSerializer, stringSerializer));
                Assert.assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify that getAndPutIfAbsent returns the existing value and doesn't modify it in the cache
                final String getAndPutIfAbsentResult = cacheClient.getAndPutIfAbsent(key, value, stringSerializer, stringSerializer, stringDeserializer);
                Assert.assertEquals(value, getAndPutIfAbsentResult);
                Assert.assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify that getAndPutIfAbsent on a key that doesn't exist returns null
                final String keyThatDoesntExist = key + "_DOES_NOT_EXIST";
                Assert.assertFalse(cacheClient.containsKey(keyThatDoesntExist, stringSerializer));
                final String getAndPutIfAbsentResultWhenDoesntExist = cacheClient.getAndPutIfAbsent(keyThatDoesntExist, value, stringSerializer, stringSerializer, stringDeserializer);
                Assert.assertEquals(null, getAndPutIfAbsentResultWhenDoesntExist);
                Assert.assertEquals(value, cacheClient.get(keyThatDoesntExist, stringSerializer, stringDeserializer));


                // get/set checks with serializer
                for (Record record : records) {
                    String recordKey = record.getId();
                    cacheClient.set(recordKey, record);
                    Assert.assertTrue(cacheClient.containsKey(recordKey, stringSerializer));
                    Record storedRecord = cacheClient.get(recordKey);
                    Assert.assertEquals(record,storedRecord);
                    cacheClient.remove(recordKey, stringSerializer);
                    Assert.assertFalse(cacheClient.containsKey(recordKey, stringSerializer));
                }

                /*
                // verify atomic fetch returns the correct entry
                final AtomicCacheEntry<String,String,byte[]> entry = cacheClient.fetch(key, stringSerializer, stringDeserializer);
                Assert.assertEquals(key, entry.getKey());
                Assert.assertEquals(value, entry.getValue());
                Assert.assertTrue(Arrays.equals(value.getBytes(StandardCharsets.UTF_8), entry.getRevision().orElse(null)));

                final AtomicCacheEntry<String,String,byte[]> notLatestEntry = new AtomicCacheEntry<>(entry.getKey(), entry.getValue(), "not previous".getBytes(StandardCharsets.UTF_8));

                // verify atomic replace does not replace when previous value is not equal
                Assert.assertFalse(cacheClient.replace(notLatestEntry, stringSerializer, stringSerializer));
                Assert.assertEquals(value, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify atomic replace does replace when previous value is equal
                final String replacementValue = "this value has been replaced";
                entry.setValue(replacementValue);
                Assert.assertTrue(cacheClient.replace(entry, stringSerializer, stringSerializer));
                Assert.assertEquals(replacementValue, cacheClient.get(key, stringSerializer, stringDeserializer));

                // verify atomic replace does replace no value previous existed
                final String replaceKeyDoesntExist = key + "_REPLACE_DOES_NOT_EXIST";
                final AtomicCacheEntry<String,String,byte[]> entryDoesNotExist = new AtomicCacheEntry<>(replaceKeyDoesntExist, replacementValue, null);
                Assert.assertTrue(cacheClient.replace(entryDoesNotExist, stringSerializer, stringSerializer));
                Assert.assertEquals(replacementValue, cacheClient.get(replaceKeyDoesntExist, stringSerializer, stringDeserializer));
*/
                final int numToDelete = 2000;
                for (int i = 0; i < numToDelete; i++) {
                    cacheClient.put(key + "-" + i, value, stringSerializer, stringSerializer);
                }

                Assert.assertTrue(cacheClient.removeByPattern("test-redis-processor-*") >= numToDelete);
                Assert.assertFalse(cacheClient.containsKey(key, stringSerializer));


            } catch (final Exception e) {
                getLogger().error("Routing to failure due to: " + e.getMessage(), e);

            }
            return Collections.emptyList();
        }

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
