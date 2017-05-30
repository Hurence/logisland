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
package com.hurence.logisland.engine;

import com.google.common.base.Splitter;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.security.SecureRandom;
import java.util.*;

import static org.junit.Assert.assertTrue;


public class TopicPartitionerTest {


    private static Logger logger = LoggerFactory.getLogger(TopicPartitionerTest.class);


    /**
     * id are generated like following
     *
     * 0:j20ize7q:pAsoBf029w4Ah4J0rP60X0gXQkFqSHgq
     * 0:j20ize84:nuSamuvnm01ZhfS2dLI4QTtX5hoRVno_
     * 0:j20ize8c:NpIsnSnbcuqnoT-C9tV8AEXFSCQLr-AX
     * 0:j20ize8d:3FB4b4zreyrwuic54eOKI4S3Ua9VFTD_
     */
    @Test
    public void validatePartitioningIdempotency() {

        int numPartitions = 50;

        for (int i = 0; i < 1000; i++) {
            String recordId = WebAnalyticIdentifier.generate().value;
            logger.info(recordId);

            Set<Integer> partitions = new HashSet<>();
            for (int j = 0; j < 10; j++) {
                partitions.add(getKafkaDefaultPartition(numPartitions, recordId));
            }
            assertTrue(partitions.size() == 1);
        }
    }

    private int getKafkaDefaultPartition(int numPartitions, String recordId) {
        return  Utils.abs(Utils.murmur2(recordId.getBytes())) % numPartitions;
    }
}

/**
 * Unique time-based identifiers for web analytics.
 * <p>
 * Divolte uses unique identifiers for several purposes, some of which require
 * an embedded timestamp indicating when the identifier was generated. (Although
 * we could use Version 1 UUIDs, not all clients can trivially generate these.)
 */
final class WebAnalyticIdentifier {
    private final static char VERSION = '0';
    private final static String VERSION_STRING = "" + VERSION;
    private static final char SEPARATOR_CHAR = ':';

    private final static Splitter splitter = Splitter.on(SEPARATOR_CHAR).limit(4);

    /**
     * The unique identifier
     */
    @Nonnull
    public final String value;
    /**
     * The difference, measured in milliseconds by the system that generated the
     * identifier, between when the identifier was generated and midnight, January 1, 1970 UTC.
     */
    public final long timestamp;
    /**
     * The version of the identifier.
     */
    public final char version;

    private WebAnalyticIdentifier(final long timestamp, final String id) {
        this.version = VERSION;
        this.timestamp = timestamp;
        this.value = VERSION_STRING + SEPARATOR_CHAR
                + Long.toString(timestamp, 36) + SEPARATOR_CHAR
                + Objects.requireNonNull(id);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return this == other ||
                null != other && getClass() == other.getClass() && value.equals(((WebAnalyticIdentifier) other).value);
    }

    public static Optional<WebAnalyticIdentifier> tryParse(final String input) {
        Objects.requireNonNull(input);
        try {
            final List<String> parts = (List<String>) splitter.split(input);
            return parts.size() == 3 && VERSION_STRING.equals(parts.get(0))
                    ? Optional.of(new WebAnalyticIdentifier(Long.parseLong(parts.get(1), 36), parts.get(2)))
                    : Optional.empty();
        } catch (final NumberFormatException e) {
            return Optional.empty();
        }
    }

    // Some sources mention it's a good idea to avoid contention on SecureRandom instances...
    private final static ThreadLocal<SecureRandom> localRandom = ThreadLocal.withInitial(SecureRandom::new);

    public static WebAnalyticIdentifier generate(final long ts) {
        final SecureRandom random = localRandom.get();

        final byte[] randomBytes = new byte[24];
        random.nextBytes(randomBytes);
        final String id = Base64.getUrlEncoder().encodeToString(randomBytes);

        return new WebAnalyticIdentifier(ts, id);
    }

    public static WebAnalyticIdentifier generate() {
        return generate(System.currentTimeMillis());
    }
}

