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


import java.util.Date;

/**
 * EdcPosition is a data structure to capture a geo location. It can be
 * associated to an EdcPayload to geotag an EdcMessage before sending to the
 * Everyware Cloud. Refer to the description of each of the fields for more
 * information on the model of EdcPosition.
 */
public class KuraPosition {
    /**
     * Longitude of this position in degrees. This is a mandatory field.
     */
    private Double longitude;

    /**
     * Latitude of this position in degrees. This is a mandatory field.
     */
    private Double latitude;

    /**
     * Altitude of the position in meters.
     */
    private Double altitude;

    /**
     * Dilution of the precision (DOP) of the current GPS fix.
     */
    private Double precision;

    /**
     * Heading (direction) of the position in degrees
     */
    private Double heading;

    /**
     * Speed for this position in meter/sec.
     */
    private Double speed;

    /**
     * Timestamp extracted from the GPS system
     */
    private Date timestamp;

    /**
     * Number of satellites seen by the systems
     */
    private Integer satellites;

    /**
     * Status of GPS system: 1 = no GPS response, 2 = error in response, 4 =
     * valid.
     */
    private Integer status;

    public KuraPosition() {
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    public Double getPrecision() {
        return precision;
    }

    public void setPrecision(double precision) {
        this.precision = precision;
    }

    public Double getHeading() {
        return heading;
    }

    public void setHeading(double heading) {
        this.heading = heading;
    }

    public Double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getSatellites() {
        return satellites;
    }

    public void setSatellites(int satellites) {
        this.satellites = satellites;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "KuraPosition{" +
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
