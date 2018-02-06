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

import java.time.Instant;

public class Position {

    private static final Position EMPTY = new Position(null, null, null, null, null, null, null, null);

    private final Double altitude;
    private final Double heading;
    private final Double latitude;
    private final Double longitude;
    private final Double precision;
    private final Integer satellites;
    private final Double speed;
    private final Instant timestamp;

    private Position(
            final Double altitude,
            final Double heading,
            final Double latitude,
            final Double longitude,
            final Double precision,
            final Integer satellites,
            final Double speed,
            final Instant timestamp) {

        this.altitude = altitude;
        this.heading = heading;
        this.latitude = latitude;
        this.longitude = longitude;
        this.precision = precision;
        this.satellites = satellites;
        this.speed = speed;
        this.timestamp = timestamp;

    }

    public Double getAltitude() {
        return this.altitude;
    }

    public Double getHeading() {
        return this.heading;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public Double getLongitude() {
        return this.longitude;
    }

    public Double getPrecision() {
        return this.precision;
    }

    public Integer getSatellites() {
        return this.satellites;
    }

    public Double getSpeed() {
        return this.speed;
    }

    public Instant getTimestamp() {
        return this.timestamp;
    }

    public static Position from(final Double altitude, final Double heading, final Double latitude, final Double longitude, final Double precision, final Integer satellites, final Double speed,
                                final Instant timestamp) {
        return new Position(altitude, heading, latitude, longitude, precision, satellites, speed, timestamp);
    }

    public static Position empty() {
        return EMPTY;
    }
}