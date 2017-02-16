/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.kakfa.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hurence.logisland.agent.rest.model.Job;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class JobValue implements Comparable<JobValue>, RegistryValue {

  @NotEmpty
  private String name;
  @Min(1)
  private Integer version;
  @Min(0)
  private Long id;
  @NotEmpty
  private Job job;

  public JobValue(@JsonProperty("name") String name,
                  @JsonProperty("version") Integer version,
                  @JsonProperty("id") Long id,
                  @JsonProperty("job") Job job) {
    this.name = name;
    this.version = version;
    this.id = id;
    this.job = job;
  }

  public JobValue(Job job) {
    this.name = job.getName();
    this.version = job.getVersion();
    this.id = job.getId();
    this.job = job;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @JsonProperty("id")
  public Long getId() {
    return this.id;
  }

  @JsonProperty("id")
  public void setId(Long id) {
    this.id = id;
  }

  @JsonProperty("job")
  public Job getJob() {
    return this.job;
  }

  @JsonProperty("schema")
  public void setJob(Job job) {
    this.job = job;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JobValue that = (JobValue) o;

    if (!this.name.equals(that.name)) {
      return false;
    }
    if (!this.version.equals(that.version)) {
      return false;
    }
    if (!this.id.equals(that.getId())) {
      return false;
    }
    if (!this.job.equals(that.job)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + version;
    result = 31 * result + id.intValue();
    result = 31 * result + job.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{name=" + this.name + ",");
    sb.append("version=" + this.version + ",");
    sb.append("id=" + this.id + ",");
    sb.append("job=" + this.job + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(JobValue that) {
    int result = this.name.compareTo(that.name);
    if (result != 0) {
      return result;
    }
    result = this.version - that.version;
    return result;
  }
}
