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
package com.hurence.logisland.record;

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


import java.util.Date;

/**
 * EdcPosition is a data structure to capture a geo location. It can be
 * associated to an EdcPayload to geotag an EdcMessage before sending to the
 * Everyware Cloud. Refer to the description of each of the fields for more
 * information on the model of EdcPosition.
 */
public class Position  extends StandardRecord{
    /**
     * Longitude of this position in degrees. This is a mandatory field.
     */
    private final Double longitude;

    /**
     * Latitude of this position in degrees. This is a mandatory field.
     */
    private final Double latitude;

    /**
     * Altitude of the position in meters.
     */
    private final Double altitude;

    /**
     * Dilution of the precision (DOP) of the current GPS fix.
     */
    private final Double precision;

    /**
     * Heading (direction) of the position in degrees
     */
    private final Double heading;

    /**
     * Speed for this position in meter/sec.
     */
    private final Double speed;

    /**
     * Timestamp extracted from the GPS system
     */
    private final Date timestamp;

    /**
     * Number of satellites seen by the systems
     */
    private final Integer satellites;

    /**
     * Status of GPS system: 1 = no GPS response, 2 = error in response, 4 =
     * valid.
     */
    private final Integer status;

    private Position(
            final Double altitude,
            final Double heading,
            final Double latitude,
            final Double longitude,
            final Double precision,
            final Integer satellites,
            final Integer status,
            final Double speed,
            final Date timestamp) {

        this.altitude = altitude;
        this.heading = heading;
        this.latitude = latitude;
        this.longitude = longitude;
        this.precision = precision;
        this.satellites = satellites;
        this.speed = speed;
        this.timestamp = timestamp;
        this.status = status;

    }

    private static final Position EMPTY = new Position(null, null, null, null, null, null, null, null, null);


    public Double getLongitude() {
        return longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public Double getPrecision() {
        return precision;
    }

    public Double getHeading() {
        return heading;
    }

    public Double getSpeed() {
        return speed;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Integer getSatellites() {
        return satellites;
    }

    public Integer getStatus() {
        return status;
    }


    public static Position from(final Double altitude, final Double heading, final Double latitude, final Double longitude, final Double precision, final Integer satellites, final Integer status, final Double speed,
                                final Date timestamp) {
        return new Position(altitude, heading, latitude, longitude, precision, satellites, status, speed, timestamp);
    }

    public static Position empty() {
        return EMPTY;
    }

    @Override
    public String toString() {
        return "Position{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", altitude=" + altitude +
                ", precision=" + precision +
                ", heading=" + heading +
                ", speed=" + speed +
                ", timestamp=" + timestamp +
                ", satellites=" + satellites +
                ", status=" + status +
                '}';
    }
}
