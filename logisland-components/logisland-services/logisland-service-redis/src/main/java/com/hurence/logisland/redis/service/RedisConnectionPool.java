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


import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.redis.RedisType;
import com.hurence.logisland.redis.util.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;


public class RedisConnectionPool {

    private volatile ControllerServiceInitializationContext context;
    private volatile RedisType redisType;
    private volatile JedisConnectionFactory connectionFactory;

    private static Logger logger = LoggerFactory.getLogger(RedisConnectionPool.class);


    public void init(final ControllerServiceInitializationContext context) {
        this.context = context;

        final String redisMode = context.getPropertyValue(RedisUtils.REDIS_MODE).asString();
        this.redisType = RedisType.fromDisplayName(redisMode);
    }


    public void close() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
            connectionFactory = null;
            redisType = null;
            context = null;
        }
    }

    public RedisType getRedisType() {
        return redisType;
    }

    public RedisConnection getConnection() {
        if (connectionFactory == null) {
            synchronized (this) {
                if (connectionFactory == null) {
                    logger.info("creating Redis connection factory");
                    connectionFactory = RedisUtils.createConnectionFactory(context);
                }
            }
        }

        return connectionFactory.getConnection();
    }


}
