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
package com.hurence.logisland.util.kura;

/*******************************************************************************
 * Copyright (C) 2015 - Amit Kumar Mondal <admin@amitinside.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/


import java.util.*;

/**
 * EdcPayload defines the recommended payload structure for the messages sent to
 * the Everyware Cloud platform. Eurotech designed the format as an open format
 * that is flexible from the aspect of data modeling yet is efficient when it
 * comes to bandwidth conservation. The same payload model is used by the REST
 * API - in which case it is serialized into XML or JSON as requested by the
 * client - or uses the efficient Google ProtoBuf when sent over an MQTT
 * connection when the bandwidth is very important. The EdcPayload contains the
 * following fields: sentOn timestamp, an optional set of metrics represented as
 * name-value pairs, an optional position field to capture a GPS position, and
 * an optional binary body.
 * <ul>
 * <li>sentOn: it is the timestamp when the data was captured and sent to the
 * Everyware Cloud platform.
 * <li>metrics: a metric is a data structure composed of the name, a value, and
 * the type of the value. When used with the REST API valid metric types are:
 * string, double, int, float, long, boolean, base64Binary. Data exposed into
 * the payload metrics can be processed through the real-time rules offered by
 * the Everyware Cloud platform or used as query criteria when searching for
 * messages through the messages/searchByMetric API. Each payload can have zero
 * or more metrics.
 * <li>position: it is an optional field used to capture a geo position
 * associated to this payload.
 * <li>body: it is an optional part of the payload that allows additional
 * information to be transmitted in any format determined by the user. This
 * field will be stored into the platform database, but the Everyware Cloud
 * cannot apply any statistical analysis on it.
 * </ul>
 */
public class KuraPayload {
    /**
     * Timestamp when the data was captured and sent to the Everyware Cloud
     * platform.
     */
    private Date timestamp;

    /**
     * It is an optional field used to capture a geo position associated to this
     * payload.
     */
    private KuraPosition position;

    /**
     * A metric is a data structure composed of the name, a value, and the type
     * of the value. When used with the REST API valid metric types are: string,
     * double, int, float, long, boolean, base64Binary. Data exposed into the
     * payload metrics can be processed through the real-time rules offered by
     * the Everyware Cloud platform or used a query criteria when searching for
     * messages through the messages/searchByMetric API. Each payload can have
     * zero or more metrics.
     */
    private final Map<String, Object> metrics;

    /**
     * It is an optional part of the payload that allows additional information
     * to be transmitted in any format determined by the user. This field will
     * be stored into the platform database but the Everyware Cloud cannot apply
     * any statistical analysis on it.
     */
    private byte[] body;

    public KuraPayload() {
        this.metrics = new HashMap<String, Object>();
        this.body = null;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public KuraPosition getPosition() {
        return position;
    }

    public void setPosition(KuraPosition position) {
        this.position = position;
    }

    public Object getMetric(String name) {
        return metrics.get(name);
    }

    public void addMetric(String name, Object value) {
        metrics.put(name, value);
    }

    public void removeMetric(String name) {
        metrics.remove(name);
    }

    public void removeAllMetrics() {
        metrics.clear();
    }

    public Set<String> metricNames() {
        return Collections.unmodifiableSet(metrics.keySet());
    }

    public Iterator<String> metricsIterator() {
        return metrics.keySet().iterator();
    }

    public Map<String, Object> metrics() {
        return Collections.unmodifiableMap(metrics);
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String
    toString() {
        return "KuraPayload{" +
                "timestamp=" + timestamp +
                ", position=" + position +
                ", metrics=" + metrics +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}