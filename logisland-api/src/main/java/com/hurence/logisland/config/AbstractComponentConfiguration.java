/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * Example of yaml configuration
 * <p>
 * component: com.hurence.logisland.processor.SplitText
 * type: parser
 * documentation: a parser that produce events from a REGEX
 * configuration:
 * key.regex: (\S*):(\S*)
 * key.fields: c,d
 * value.regex: (\S*):(\S*)
 * value.fields: a,b
 */
public abstract class AbstractComponentConfiguration implements Serializable{

    private String component = "";
    private String documentation = "";
    private String type = "";

    private Map<String, String> configuration = new HashMap<>();

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public String getDocumentation() {
        return documentation;
    }

    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    @Override
    public String toString() {
        return "ComponentConfiguration{" +
                "component='" + component + '\'' +
                ", documentation='" + documentation + '\'' +
                ", type='" + type + '\'' +
                ", configuration=" + configuration +
                '}';
    }
}
