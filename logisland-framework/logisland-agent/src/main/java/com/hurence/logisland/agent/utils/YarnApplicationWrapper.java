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
package com.hurence.logisland.agent.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;



public class YarnApplicationWrapper {

    private static Logger logger = LoggerFactory.getLogger(YarnApplication.class);
    private Map<String, YarnApplication> apps = new HashMap<>();

    public YarnApplicationWrapper(String yarnLogs) {
        String[] lines = yarnLogs.split("\n");
        for (String line : lines) {
            if (line.contains("application_")) {
                try {
                    YarnApplication app = new YarnApplication(line);
                    apps.put(app.getName(), app);
                } catch (Exception e) {
                    logger.error("unable to parse Yarn log line {} : {}", line, e.toString());
                }
            }
        }
    }

    public YarnApplication getApplication(String name) {
        return apps.get(name);
    }
}
