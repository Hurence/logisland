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
package com.hurence.logisland.redis.util;


import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.redis.RedisType;
import com.hurence.logisland.util.string.StringUtils;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisUtils {

    private static Logger logger = LoggerFactory.getLogger(RedisUtils.class);


    // These properties are shared between the connection pool controller service and the state provider, the name
    // is purposely set to be more human-readable since that will be referenced in state-management.xml

    public static final AllowableValue REDIS_MODE_STANDALONE = new AllowableValue(RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDescription());
    public static final AllowableValue REDIS_MODE_SENTINEL = new AllowableValue(RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDescription());
    public static final AllowableValue REDIS_MODE_CLUSTER = new AllowableValue(RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDescription());

    public static final PropertyDescriptor REDIS_MODE = new PropertyDescriptor.Builder()
            .name("redis.mode")
            .displayName("Redis Mode")
            .description("The type of Redis being communicated with - standalone, sentinel, or clustered.")
            .allowableValues(REDIS_MODE_STANDALONE, REDIS_MODE_SENTINEL, REDIS_MODE_CLUSTER)
            .defaultValue(REDIS_MODE_STANDALONE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("connection.string")
            .displayName("Connection String")
            .description("The connection string for Redis. In a standalone instance this value will be of the form hostname:port. " +
                    "In a sentinel instance this value will be the comma-separated list of sentinels, such as host1:port1,host2:port2,host3:port3. " +
                    "In a clustered instance this value will be the comma-separated list of cluster masters, such as host1:port,host2:port,host3:port.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        //    .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("database.index")
            .displayName("Database Index")
            .description("The database index to be used by connections created from this connection pool. " +
                    "See the databases property in redis.conf, by default databases 0-15 will be available.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
         //   .expressionLanguageSupported(true)
            .required(true)
            .build();

    public static final PropertyDescriptor COMMUNICATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("communication.timeout")
            .displayName("Communication Timeout")
            .description("The timeout to use when attempting to communicate with Redis.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor CLUSTER_MAX_REDIRECTS = new PropertyDescriptor.Builder()
            .name("cluster.max.redirects")
            .displayName("Cluster Max Redirects")
            .description("The maximum number of redirects that can be performed when clustered.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .required(true)
            .build();

    public static final PropertyDescriptor SENTINEL_MASTER = new PropertyDescriptor.Builder()
            .name("sentinel.master")
            .displayName("Sentinel Master")
            .description("The name of the sentinel master, require when Mode is set to Sentinel")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
          //  .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("The password used to authenticate to the Redis server. See the requirepass property in redis.conf.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        //    .expressionLanguageSupported(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_TOTAL = new PropertyDescriptor.Builder()
            .name("pool.max.total")
            .displayName("Pool - Max Total")
            .description("The maximum number of connections that can be allocated by the pool (checked out to clients, or idle awaiting checkout). " +
                    "A negative value indicates that there is no limit.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_IDLE = new PropertyDescriptor.Builder()
            .name("pool.max.idle")
            .displayName("Pool - Max Idle")
            .description("The maximum number of idle connections that can be held in the pool, or a negative value if there is no limit.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MIN_IDLE = new PropertyDescriptor.Builder()
            .name("pool.min.idle")
            .displayName("Pool - Min Idle")
            .description("The target for the minimum number of idle connections to maintain in the pool. If the configured value of Min Idle is " +
                    "greater than the configured value for Max Idle, then the value of Max Idle will be used instead.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_BLOCK_WHEN_EXHAUSTED = new PropertyDescriptor.Builder()
            .name("pool.block.when.exhausted")
            .displayName("Pool - Block When Exhausted")
            .description("Whether or not clients should block and wait when trying to obtain a connection from the pool when the pool has no available connections. " +
                    "Setting this to false means an error will occur immediately when a client requests a connection and none are available.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("pool.max.wait.time")
            .displayName("Pool - Max Wait Time")
            .description("The amount of time to wait for an available connection when Block When Exhausted is set to true.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("pool.min.evictable.idle.time")
            .displayName("Pool - Min Evictable Idle Time")
            .description("The minimum amount of time an object may sit idle in the pool before it is eligible for eviction.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("60 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TIME_BETWEEN_EVICTION_RUNS = new PropertyDescriptor.Builder()
            .name("pool.time.between.eviction.runs")
            .displayName("Pool - Time Between Eviction Runs")
            .description("The amount of time between attempting to evict idle connections from the pool.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_NUM_TESTS_PER_EVICTION_RUN = new PropertyDescriptor.Builder()
            .name("pool.num.tests.per.eviction.run")
            .displayName("Pool - Num Tests Per Eviction Run")
            .description("The number of connections to tests per eviction attempt. A negative value indicates to test all connections.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("-1")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_CREATE = new PropertyDescriptor.Builder()
            .name("pool.test.on.create")
            .displayName("Pool - Test On Create")
            .description("Whether or not connections should be tested upon creation.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_BORROW = new PropertyDescriptor.Builder()
            .name("pool.test.on.borrow")
            .displayName("Pool - Test On Borrow")
            .description("Whether or not connections should be tested upon borrowing from the pool.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_RETURN = new PropertyDescriptor.Builder()
            .name("pool.test.on.return")
            .displayName("Pool - Test On Return")
            .description("Whether or not connections should be tested upon returning to the pool.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_WHILE_IDLE = new PropertyDescriptor.Builder()
            .name("pool.test.while.idle")
            .displayName("Pool - Test While Idle")
            .description("Whether or not connections should be tested while idle.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();














    public static final List<PropertyDescriptor> REDIS_CONNECTION_PROPERTY_DESCRIPTORS;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RedisUtils.REDIS_MODE);
        props.add(RedisUtils.CONNECTION_STRING);
        props.add(RedisUtils.DATABASE);
        props.add(RedisUtils.COMMUNICATION_TIMEOUT);
        props.add(RedisUtils.CLUSTER_MAX_REDIRECTS);
        props.add(RedisUtils.SENTINEL_MASTER);
        props.add(RedisUtils.PASSWORD);
        props.add(RedisUtils.POOL_MAX_TOTAL);
        props.add(RedisUtils.POOL_MAX_IDLE);
        props.add(RedisUtils.POOL_MIN_IDLE);
        props.add(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED);
        props.add(RedisUtils.POOL_MAX_WAIT_TIME);
        props.add(RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME);
        props.add(RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS);
        props.add(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN);
        props.add(RedisUtils.POOL_TEST_ON_CREATE);
        props.add(RedisUtils.POOL_TEST_ON_BORROW);
        props.add(RedisUtils.POOL_TEST_ON_RETURN);
        props.add(RedisUtils.POOL_TEST_WHILE_IDLE);
        REDIS_CONNECTION_PROPERTY_DESCRIPTORS = Collections.unmodifiableList(props);
    }


    public static JedisConnectionFactory createConnectionFactory(final ControllerServiceInitializationContext context) {
        final String redisMode = context.getPropertyValue(RedisUtils.REDIS_MODE).asString();
        final String connectionString = context.getPropertyValue(RedisUtils.CONNECTION_STRING).asString();
        final Integer dbIndex = context.getPropertyValue(RedisUtils.DATABASE).asInteger();
        final String password = context.getPropertyValue(RedisUtils.PASSWORD).asString();
        final Integer timeout = context.getPropertyValue(RedisUtils.COMMUNICATION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final JedisPoolConfig poolConfig = createJedisPoolConfig(context);

        JedisConnectionFactory connectionFactory;

        if (RedisUtils.REDIS_MODE_STANDALONE.getValue().equals(redisMode)) {
            final JedisShardInfo jedisShardInfo = createJedisShardInfo(connectionString, timeout, password);

            logger.info("Connecting to Redis in standalone mode at " + connectionString);
            connectionFactory = new JedisConnectionFactory(jedisShardInfo);

        } else if (RedisUtils.REDIS_MODE_SENTINEL.getValue().equals(redisMode)) {
            final String[] sentinels = connectionString.split("[,]");
            final String sentinelMaster = context.getPropertyValue(RedisUtils.SENTINEL_MASTER).asString();
            final RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration(sentinelMaster, new HashSet<>(getTrimmedValues(sentinels)));
            final JedisShardInfo jedisShardInfo = createJedisShardInfo(sentinels[0], timeout, password);

            logger.info("Connecting to Redis in sentinel mode...");
            logger.info("Redis master = " + sentinelMaster);

            for (final String sentinel : sentinels) {
                logger.info("Redis sentinel at " + sentinel);
            }

            connectionFactory = new JedisConnectionFactory(sentinelConfiguration, poolConfig);
            connectionFactory.setShardInfo(jedisShardInfo);

        } else {
            final String[] clusterNodes = connectionString.split("[,]");
            final Integer maxRedirects = context.getPropertyValue(RedisUtils.CLUSTER_MAX_REDIRECTS).asInteger();

            final RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration(getTrimmedValues(clusterNodes));
            clusterConfiguration.setMaxRedirects(maxRedirects);

            logger.info("Connecting to Redis in clustered mode...");
            for (final String clusterNode : clusterNodes) {
                logger.info("Redis cluster node at " + clusterNode);
            }

            connectionFactory = new JedisConnectionFactory(clusterConfiguration, poolConfig);
        }

        connectionFactory.setUsePool(true);
        connectionFactory.setPoolConfig(poolConfig);
        connectionFactory.setDatabase(dbIndex);
        connectionFactory.setTimeout(timeout);

        if (!StringUtils.isBlank(password)) {
            connectionFactory.setPassword(password);
        }

        // need to call this to initialize the pool/connections
        connectionFactory.afterPropertiesSet();
        logger.info("done creating Connection factory");
        return connectionFactory;
    }

    private static List<String> getTrimmedValues(final String[] values) {
        final List<String> trimmedValues = new ArrayList<>();
        for (final String value : values) {
            trimmedValues.add(value.trim());
        }
        return trimmedValues;
    }

    private static JedisShardInfo createJedisShardInfo(final String hostAndPort, final Integer timeout, final String password) {
        final String[] hostAndPortSplit = hostAndPort.split("[:]");
        final String host = hostAndPortSplit[0].trim();
        final Integer port = Integer.parseInt(hostAndPortSplit[1].trim());

        final JedisShardInfo jedisShardInfo = new JedisShardInfo(host, port);
        jedisShardInfo.setConnectionTimeout(timeout);
        jedisShardInfo.setSoTimeout(timeout);

        if (!StringUtils.isEmpty(password)) {
            jedisShardInfo.setPassword(password);
        }

        return jedisShardInfo;
    }

    private static JedisPoolConfig createJedisPoolConfig(final ControllerServiceInitializationContext context) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(context.getPropertyValue(RedisUtils.POOL_MAX_TOTAL).asInteger());
        poolConfig.setMaxIdle(context.getPropertyValue(RedisUtils.POOL_MAX_IDLE).asInteger());
        poolConfig.setMinIdle(context.getPropertyValue(RedisUtils.POOL_MIN_IDLE).asInteger());
        poolConfig.setBlockWhenExhausted(context.getPropertyValue(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED).asBoolean());
        poolConfig.setMaxWaitMillis(context.getPropertyValue(RedisUtils.POOL_MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
        poolConfig.setMinEvictableIdleTimeMillis(context.getPropertyValue(RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
        poolConfig.setTimeBetweenEvictionRunsMillis(context.getPropertyValue(RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS).asTimePeriod(TimeUnit.MILLISECONDS));
        poolConfig.setNumTestsPerEvictionRun(context.getPropertyValue(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN).asInteger());
        poolConfig.setTestOnCreate(context.getPropertyValue(RedisUtils.POOL_TEST_ON_CREATE).asBoolean());
        poolConfig.setTestOnBorrow(context.getPropertyValue(RedisUtils.POOL_TEST_ON_BORROW).asBoolean());
        poolConfig.setTestOnReturn(context.getPropertyValue(RedisUtils.POOL_TEST_ON_RETURN).asBoolean());
        poolConfig.setTestWhileIdle(context.getPropertyValue(RedisUtils.POOL_TEST_WHILE_IDLE).asBoolean());
        return poolConfig;
    }

    public static List<ValidationResult> validate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String redisMode = validationContext.getPropertyValue(RedisUtils.REDIS_MODE).asString();
        final String connectionString = validationContext.getPropertyValue(RedisUtils.CONNECTION_STRING).asString();
        final Integer dbIndex = validationContext.getPropertyValue(RedisUtils.DATABASE).asInteger();

        if (StringUtils.isBlank(connectionString)) {
            results.add(new ValidationResult.Builder()
                    .subject(RedisUtils.CONNECTION_STRING.getDisplayName())
                    .valid(false)
                    .explanation("Connection String cannot be blank")
                    .build());
        } else if (RedisUtils.REDIS_MODE_STANDALONE.getValue().equals(redisMode)) {
            final String[] hostAndPort = connectionString.split("[:]");
            if (hostAndPort == null || hostAndPort.length != 2 || StringUtils.isBlank(hostAndPort[0]) || StringUtils.isBlank(hostAndPort[1]) || !isInteger(hostAndPort[1])) {
                results.add(new ValidationResult.Builder()
                        .subject(RedisUtils.CONNECTION_STRING.getDisplayName())
                        .input(connectionString)
                        .valid(false)
                        .explanation("Standalone Connection String must be in the form host:port")
                        .build());
            }
        } else {
            for (final String connection : connectionString.split("[,]")) {
                final String[] hostAndPort = connection.split("[:]");
                if (hostAndPort == null || hostAndPort.length != 2 || StringUtils.isBlank(hostAndPort[0]) || StringUtils.isBlank(hostAndPort[1]) || !isInteger(hostAndPort[1])) {
                    results.add(new ValidationResult.Builder()
                            .subject(RedisUtils.CONNECTION_STRING.getDisplayName())
                            .input(connection)
                            .valid(false)
                            .explanation("Connection String must be in the form host:port,host:port,host:port,etc.")
                            .build());
                }
            }
        }

        if (RedisUtils.REDIS_MODE_CLUSTER.getValue().equals(redisMode) && dbIndex > 0) {
            results.add(new ValidationResult.Builder()
                    .subject(RedisUtils.DATABASE.getDisplayName())
                    .valid(false)
                    .explanation("Database Index must be 0 when using clustered Redis")
                    .build());
        }

        if (RedisUtils.REDIS_MODE_SENTINEL.getValue().equals(redisMode)) {
            final String sentinelMaster = validationContext.getPropertyValue(RedisUtils.SENTINEL_MASTER).asString();
            if (StringUtils.isEmpty(sentinelMaster)) {
                results.add(new ValidationResult.Builder()
                        .subject(RedisUtils.SENTINEL_MASTER.getDisplayName())
                        .valid(false)
                        .explanation("Sentinel Master must be provided when Mode is Sentinel")
                        .build());
            }
        }

        return results;
    }

    private static boolean isInteger(final String number) {
        try {
            Integer.parseInt(number);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
