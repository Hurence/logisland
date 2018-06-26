/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.engine.spark.remote.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tracks stream processing pipeline configuration
 */
@ApiModel(description = "Tracks stream processing pipeline configuration")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-06-03T13:00:49.942Z")

public class Pipeline extends Versioned {
    @JsonProperty("processors")
    @Valid
    private List<Processor> processors = new ArrayList<>();

    public Pipeline processors(List<Processor> processors) {
        this.processors = processors;
        return this;
    }

    public Pipeline addProcessorsItem(Processor processorsItem) {
        if (this.processors == null) {
            this.processors = new ArrayList<Processor>();
        }
        this.processors.add(processorsItem);
        return this;
    }

    /**
     * Get processors
     *
     * @return processors
     **/
    @ApiModelProperty(value = "")

    @Valid

    public List<Processor> getProcessors() {
        return processors;
    }

    public void setProcessors(List<Processor> processors) {
        this.processors = processors;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pipeline pipeline = (Pipeline) o;
        return Objects.equals(this.processors, pipeline.processors) &&
                super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors, super.hashCode());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class Pipeline {\n");
        sb.append("    ").append(toIndentedString(super.toString())).append("\n");
        sb.append("    processors: ").append(toIndentedString(processors)).append("\n");
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

