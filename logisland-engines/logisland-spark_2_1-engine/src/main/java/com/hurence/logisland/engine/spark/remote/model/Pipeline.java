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
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A streaming pipeline.
 */
@ApiModel(description = "A streaming pipeline.")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-05-24T14:20:39.061Z")
public class Pipeline {
    @JsonProperty("name")
    @NotNull
    private String name = null;

    @JsonProperty("lastModified")
    @NotNull
    private OffsetDateTime lastModified = null;

    @JsonProperty("services")
    @Valid
    private List<Service> services = null;

    @JsonProperty("streams")
    @Valid
    @Size(min = 1)
    @NotNull
    private List<Stream> streams = null;

    public Pipeline name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The pipeline name
     *
     * @return name
     **/
    @ApiModelProperty(required = true, value = "The pipeline name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Pipeline lastModified(OffsetDateTime lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    /**
     * the last modified timestamp of this pipeline (used to trigger changes).
     *
     * @return lastModified
     **/
    @ApiModelProperty(required = true, value = "the last modified timestamp of this pipeline (used to trigger changes).")
    public OffsetDateTime getLastModified() {
        return lastModified;
    }

    public void setLastModified(OffsetDateTime lastModified) {
        this.lastModified = lastModified;
    }

    public Pipeline services(List<Service> services) {
        this.services = services;
        return this;
    }

    public Pipeline addServicesItem(Service servicesItem) {
        if (this.services == null) {
            this.services = new ArrayList<Service>();
        }
        this.services.add(servicesItem);
        return this;
    }

    /**
     * The service controllers.
     *
     * @return services
     **/
    @ApiModelProperty(value = "The service controllers.")
    public List<Service> getServices() {
        return services;
    }

    public void setServices(List<Service> services) {
        this.services = services;
    }

    public Pipeline streams(List<Stream> streams) {
        this.streams = streams;
        return this;
    }

    public Pipeline addStreamsItem(Stream streamsItem) {
        if (this.streams == null) {
            this.streams = new ArrayList<Stream>();
        }
        this.streams.add(streamsItem);
        return this;
    }

    /**
     * The engine properties.
     *
     * @return streams
     **/
    @ApiModelProperty(value = "The engine properties.")
    public List<Stream> getStreams() {
        return streams;
    }

    public void setStreams(List<Stream> streams) {
        this.streams = streams;
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pipeline pipeline = (Pipeline) o;
        return Objects.equals(this.name, pipeline.name) &&
                Objects.equals(this.lastModified, pipeline.lastModified) &&
                Objects.equals(this.services, pipeline.services) &&
                Objects.equals(this.streams, pipeline.streams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, lastModified, services, streams);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class Pipeline {\n");

        sb.append("    name: ").append(toIndentedString(name)).append("\n");
        sb.append("    lastModified: ").append(toIndentedString(lastModified)).append("\n");
        sb.append("    services: ").append(toIndentedString(services)).append("\n");
        sb.append("    streams: ").append(toIndentedString(streams)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}



