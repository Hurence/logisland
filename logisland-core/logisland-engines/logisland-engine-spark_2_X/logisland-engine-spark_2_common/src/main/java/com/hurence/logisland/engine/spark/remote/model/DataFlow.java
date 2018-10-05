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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A streaming pipeline.
 */
@ApiModel(description = "A streaming pipeline.")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-06-03T13:00:49.942Z")

public class DataFlow extends Versioned {
    @JsonProperty("services")
    @Valid
    private List<Service> services = new ArrayList<>();

    @JsonProperty("streams")
    @Valid
    private List<Stream> streams = new ArrayList<>();

    public DataFlow services(List<Service> services) {
        this.services = services;
        return this;
    }

    public DataFlow addServicesItem(Service servicesItem) {
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

    @Valid

    public List<Service> getServices() {
        return services;
    }

    public void setServices(List<Service> services) {
        this.services = services;
    }

    public DataFlow streams(List<Stream> streams) {
        this.streams = streams;
        return this;
    }

    public DataFlow addStreamsItem(Stream streamsItem) {
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

    @Valid

    public List<Stream> getStreams() {
        return streams;
    }

    public void setStreams(List<Stream> streams) {
        this.streams = streams;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataFlow dataFlow = (DataFlow) o;
        return Objects.equals(this.services, dataFlow.services) &&
                Objects.equals(this.streams, dataFlow.streams) &&
                super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(services, streams, super.hashCode());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class DataFlow {\n");
        sb.append("    ").append(toIndentedString(super.toString())).append("\n");
        sb.append("    services: ").append(toIndentedString(services)).append("\n");
        sb.append("    streams: ").append(toIndentedString(streams)).append("\n");
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

