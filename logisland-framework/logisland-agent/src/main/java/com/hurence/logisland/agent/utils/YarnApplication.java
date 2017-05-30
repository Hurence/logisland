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
package com.hurence.logisland.agent.utils;


public class YarnApplication {


    private String id;
    private String name;
    private String type;
    private String user;
    private String yarnQueue;
    private String state;
    private String finalState;
    private String progress;
    private String trackingUrl;

    public YarnApplication(String yarnCmdLine) {
        // remove multiple withspace from string
        String[] tokens = yarnCmdLine.trim().replaceAll(" ", "").split("\t");
        if(tokens.length != 9)
            throw new IllegalArgumentException("wrong yarn line : " + yarnCmdLine);

        this.id = tokens[0];
        this.name = tokens[1];
        this.type = tokens[2];
        this.user = tokens[3];
        this.yarnQueue = tokens[4];
        this.state = tokens[5];
        this.finalState = tokens[6];
        this.progress = tokens[7];
        this.trackingUrl = tokens[8];
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getFinalState() {
        return finalState;
    }

    public void setFinalState(String finalState) {
        this.finalState = finalState;
    }

    public String getProgress() {
        return progress;
    }

    public void setProgress(String progress) {
        this.progress = progress;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }
}
