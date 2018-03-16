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
package com.hurence.logisland.config.v2;


import java.util.ArrayList;
import java.util.List;

/**
 * Yaml definition of the Logisland config
 *
 *
 * version: 0.9.5
 * documentation: LogIsland analytics main config file. Put here every engine or component config
 * engine: spark_engine
 *   component: com.hurence.logisland.engine.SparkStreamProcessingEngine
 *   ....
 *
 *
 */
public class JobConfig {

    private String documentation;
    private String version;
    private EngineConfig engine;
    private List<StreamConfig> streams = new ArrayList<>();
    private List<ServiceConfig> services = new ArrayList<>();


    public List<StreamConfig> getStreams() {
        return streams;
    }

    public void setStreams(List<StreamConfig> streams) {
        this.streams = streams;
    }

    public List<ServiceConfig> getServices() {
        return services;
    }

    public void setServices(List<ServiceConfig> services) {
        this.services = services;
    }

    public String getDocumentation() {
        return documentation;
    }

    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public EngineConfig getEngine() {
        return engine;
    }

    public void setEngine(EngineConfig engine) {
        this.engine = engine;
    }

    @Override
    public String toString() {
        return "LogislandSessionConfiguration{" +
                "documentation='" + documentation + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
