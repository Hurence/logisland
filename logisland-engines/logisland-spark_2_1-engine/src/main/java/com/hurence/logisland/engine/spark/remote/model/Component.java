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
package com.hurence.logisland.engine.spark.remote.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Component
 */
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-06-03T13:00:49.942Z")

public class Component {
    @JsonProperty("name")
    private String name = null;

    @JsonProperty("component")
    private String component = null;

    @JsonProperty("documentation")
    private String documentation = null;

    @JsonProperty("config")
    @Valid
    private List<Property> config = new ArrayList<>();

    public Component name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Get name
     *
     * @return name
     **/
    @ApiModelProperty(required = true, value = "")
    @NotNull


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Component component(String component) {
        this.component = component;
        return this;
    }

    /**
     * Get component
     *
     * @return component
     **/
    @ApiModelProperty(required = true, value = "")
    @NotNull


    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public Component documentation(String documentation) {
        this.documentation = documentation;
        return this;
    }

    /**
     * Get documentation
     *
     * @return documentation
     **/
    @ApiModelProperty(value = "")


    public String getDocumentation() {
        return documentation;
    }

    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    public Component config(List<Property> config) {
        this.config = config;
        return this;
    }

    public Component addConfigItem(Property configItem) {
        if (this.config == null) {
            this.config = new ArrayList<Property>();
        }
        this.config.add(configItem);
        return this;
    }

    /**
     * Get config
     *
     * @return config
     **/
    @ApiModelProperty(value = "")

    @Valid

    public List<Property> getConfig() {
        return config;
    }

    public void setConfig(List<Property> config) {
        this.config = config;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Component component = (Component) o;
        return Objects.equals(this.name, component.name) &&
                Objects.equals(this.component, component.component) &&
                Objects.equals(this.documentation, component.documentation) &&
                Objects.equals(this.config, component.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, component, documentation, config);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class Component {\n");

        sb.append("    name: ").append(toIndentedString(name)).append("\n");
        sb.append("    component: ").append(toIndentedString(component)).append("\n");
        sb.append("    documentation: ").append(toIndentedString(documentation)).append("\n");
        sb.append("    config: ").append(toIndentedString(config)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}

