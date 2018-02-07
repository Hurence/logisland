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
package com.hurence.logisland.util.kura;

import com.google.protobuf.InvalidProtocolBufferException;
import org.eclipse.kura.core.message.protobuf.KuraPayloadProto;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertTrue;


public class KuraPayloadDecoderTest {


    /**
     * Extraction of metrics form Kapua message payload.
     *
     * @param payload   payload received from Kapua
     * @param metricKey string representing key of metric
     * @return string representation of metric value
     */
    private String getMetric(byte[] payload, String metricKey) {

        String value = null;
        KuraPayloadProto.KuraPayload kuraPayload = null;
        try {
            kuraPayload = KuraPayloadProto.KuraPayload.parseFrom(payload);
        } catch (InvalidProtocolBufferException e) {
            value = null;
        }
        if (kuraPayload == null) {
            return value;
        }

        List<KuraPayloadProto.KuraPayload.KuraMetric> metrics = kuraPayload.getMetricList();
        for (KuraPayloadProto.KuraPayload.KuraMetric metric : metrics) {
            String name = metric.getName();
            if (name.equals(metricKey)) {
                value = metric.getStringValue();
            }
        }

        return value;
    }

    private static Logger logger = LoggerFactory.getLogger(KuraPayloadDecoderTest.class);


    protected static Map<String, Object> generateMetrics(final Instant timestamp, final Map<String, Function<Instant, ?>> generators) {
        final Map<String, Object> result = new HashMap<>(generators.size());

        for (final Map.Entry<String, Function<Instant, ?>> entry : generators.entrySet()) {
            result.put(entry.getKey(), entry.getValue().apply(timestamp));
        }

        return result;
    }


    public static ToDoubleFunction<Instant> sineDouble(final Duration period, final double amplitude, final double offset, final Short shift) {
        final double freq = 1.0 / period.toMillis() * Math.PI * 2.0;
        if (shift == null) {
            return (timestamp) -> Math.sin(freq * timestamp.toEpochMilli()) * amplitude + offset;
        } else {
            final double radShift = Math.toRadians(shift);
            return (timestamp) -> Math.sin(freq * timestamp.toEpochMilli() + radShift) * amplitude + offset;
        }
    }


    public static Function<Instant, Double> sine(final Duration period, final double amplitude, final double offset, final Short shift) {
        final ToDoubleFunction<Instant> func = sineDouble(period, amplitude, offset, shift);
        return timestamp -> func.applyAsDouble(timestamp);
    }

    @Test
    public void validateIntegration() throws IOException {

        Instant timestamp = Instant.now();

        final Map<String, Function<Instant, ?>> apps = new HashMap<>();
        apps.put("sine", sine(ofSeconds(120), 100, 0, null));

        final Payload payload = new Payload(generateMetrics(timestamp, apps));


        final KuraPayloadProto.KuraPayload.Builder builder = KuraPayloadProto.KuraPayload.newBuilder();
        Metrics.buildPayload(builder, payload);
        builder.setTimestamp(timestamp.toEpochMilli());

        KuraPayloadProto.KuraPayload protPayload = builder.build();


        byte[] bytesK1 = new byte[]{8, -19, -95, -120, -44, -106, 44, -62, -72, 2, 18, 10, 5,
                99, 111, 110, 115, 116, 16, 0, 25, -10, 40, 92, -113, -62, 53, 69, 64};


        assertTrue(new KuraPayloadDecoder(bytesK1)
                .buildFromByteArray()
                .getMetric("const")
                .toString()
                .equals("42.42"));

    }
}
