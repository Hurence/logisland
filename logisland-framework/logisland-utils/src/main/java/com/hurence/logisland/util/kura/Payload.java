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
package com.hurence.logisland.util.kura;

import com.hurence.logisland.record.Position;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Payload {

    private final ByteBuffer body;
    private final Position position;
    private final Map<String, Object> metrics;

    private static ByteBuffer copy(final ByteBuffer source) {
        if (source == null || !source.hasRemaining()) {
            return null;
        }

        final ByteBuffer result = ByteBuffer.allocate(source.remaining());
        result.put(source);
        result.flip();
        return result.asReadOnlyBuffer();
    }

    public Payload(final ByteBuffer body, final Position position, final Map<String, Object> metrics) {
        this.body = copy(body);
        this.position = position;
        this.metrics = metrics != null ? Collections.unmodifiableMap(new HashMap<>(metrics)) : Collections.emptyMap();
    }

    public Payload(final Map<String, Object> metrics) {
        this(null, null, metrics);
    }

    public ByteBuffer getBody() {
        return this.body;
    }

    public Position getPosition() {
        return this.position;
    }

    public Map<String, Object> getMetrics() {
        return this.metrics;
    }
}