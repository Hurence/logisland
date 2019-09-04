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
package com.hurence.logisland.redis.service;


import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.redis.util.RedisUtils;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestRedisConnectionPoolService {

    private TestRunner testRunner;
    private FakeRedisProcessor proc;
    private RedisKeyValueCacheService redisService;

    @Before
    public void setup() throws InitializationException {
        proc = new FakeRedisProcessor();
        testRunner = TestRunners.newTestRunner(proc);

        redisService = new RedisKeyValueCacheService();
        testRunner.addControllerService("redis-service", redisService);
        testRunner.setProperty(redisService, RedisUtils.REDIS_MODE, RedisUtils.REDIS_MODE_STANDALONE);
        testRunner.setProperty(redisService, RedisUtils.DATABASE, "0");
        testRunner.setProperty(redisService, RedisUtils.COMMUNICATION_TIMEOUT, "10 seconds");

        testRunner.setProperty(redisService, RedisUtils.POOL_MAX_TOTAL, "8");
        testRunner.setProperty(redisService, RedisUtils.POOL_MAX_IDLE, "8");
        testRunner.setProperty(redisService, RedisUtils.POOL_MIN_IDLE, "0");
        testRunner.setProperty(redisService, RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED, "true");
        testRunner.setProperty(redisService, RedisUtils.POOL_MAX_WAIT_TIME, "10 seconds");
        testRunner.setProperty(redisService, RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME, "60 seconds");
        testRunner.setProperty(redisService, RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS, "30 seconds");
        testRunner.setProperty(redisService, RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN, "-1");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_ON_CREATE, "false");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_ON_BORROW, "false");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_ON_RETURN, "false");
        testRunner.setProperty(redisService, RedisUtils.POOL_TEST_WHILE_IDLE, "true");
        testRunner.setProperty(redisService, RedisKeyValueCacheService.RECORD_SERIALIZER, "com.hurence.logisland.serializer.JsonSerializer");
    }

    @Test
    public void testValidateConnectionString() {
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, " ");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "${redis.connection}");
        testRunner.assertNotValid(redisService);

       /* testRunner.setProperty("redis.connection", "localhost:6379");
        testRunner.assertValid(redisService);*/

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:a");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379");
        testRunner.assertValid(redisService);

        // standalone can only have one host:port pair
        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379,localhost:6378");
        testRunner.assertNotValid(redisService);

        // cluster can have multiple host:port pairs
        testRunner.setProperty(redisService, RedisUtils.REDIS_MODE, RedisUtils.REDIS_MODE_CLUSTER.getValue());
        testRunner.assertValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379,localhost");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "local:host:6379,localhost:6378");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:a,localhost:b");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost  :6379,  localhost  :6378,    localhost:6377");
        testRunner.assertValid(redisService);
    }

    @Test
    public void testValidateSentinelMasterRequiredInSentinelMode() {
        testRunner.setProperty(redisService, RedisUtils.REDIS_MODE, RedisUtils.REDIS_MODE_SENTINEL.getValue());
        testRunner.setProperty(redisService, RedisUtils.CONNECTION_STRING, "localhost:6379,localhost:6378");
        testRunner.assertNotValid(redisService);

        testRunner.setProperty(redisService, RedisUtils.SENTINEL_MASTER, "mymaster");
        testRunner.assertValid(redisService);
    }

}
