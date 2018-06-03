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
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * a versioned component
 */
@ApiModel(description = "a versioned component")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2018-06-03T13:00:49.942Z")

public class Versioned {
    @JsonProperty("lastModified")
    private OffsetDateTime lastModified = null;

    @JsonProperty("modificationReason")
    private String modificationReason = null;

    public Versioned lastModified(OffsetDateTime lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    /**
     * the last modified timestamp of this pipeline (used to trigger changes).
     *
     * @return lastModified
     **/
    @ApiModelProperty(required = true, value = "the last modified timestamp of this pipeline (used to trigger changes).")
    @NotNull

    @Valid

    public OffsetDateTime getLastModified() {
        return lastModified;
    }

    public void setLastModified(OffsetDateTime lastModified) {
        this.lastModified = lastModified;
    }

    public Versioned modificationReason(String modificationReason) {
        this.modificationReason = modificationReason;
        return this;
    }

    /**
     * Can be used to document latest changeset.
     *
     * @return modificationReason
     **/
    @ApiModelProperty(value = "Can be used to document latest changeset.")


    public String getModificationReason() {
        return modificationReason;
    }

    public void setModificationReason(String modificationReason) {
        this.modificationReason = modificationReason;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Versioned versioned = (Versioned) o;
        return Objects.equals(this.lastModified, versioned.lastModified) &&
                Objects.equals(this.modificationReason, versioned.modificationReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastModified, modificationReason);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class Versioned {\n");

        sb.append("    lastModified: ").append(toIndentedString(lastModified)).append("\n");
        sb.append("    modificationReason: ").append(toIndentedString(modificationReason)).append("\n");
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

