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
import java.util.Objects;

/**
 * Stream
 */
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-06-03T13:00:49.942Z")
public class Stream extends Component {
    @JsonProperty("pipeline")
    private Pipeline pipeline = null;

    public Stream pipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * Get pipeline
     *
     * @return pipeline
     **/
    @ApiModelProperty(value = "")

    @Valid

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Stream stream = (Stream) o;
        return Objects.equals(this.pipeline, stream.pipeline) &&
                super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipeline, super.hashCode());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class Stream {\n");
        sb.append("    ").append(toIndentedString(super.toString())).append("\n");
        sb.append("    pipeline: ").append(toIndentedString(pipeline)).append("\n");
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

