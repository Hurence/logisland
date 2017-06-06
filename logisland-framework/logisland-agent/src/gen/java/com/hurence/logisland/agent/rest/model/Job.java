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
package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Engine;
import com.hurence.logisland.agent.rest.model.JobSummary;
import com.hurence.logisland.agent.rest.model.Stream;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * Job
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public class Job   {
  private Long id = null;

  private Integer version = null;

  private String name = null;

  private JobSummary summary = null;

  private Engine engine = null;

  private List<Stream> streams = new ArrayList<Stream>();

  public Job id(Long id) {
    this.id = id;
    return this;
  }

   /**
   * a unique identifier for the job
   * @return id
  **/
  @ApiModelProperty(value = "a unique identifier for the job")
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Job version(Integer version) {
    this.version = version;
    return this;
  }

   /**
   * the version of the job configuration
   * @return version
  **/
  @ApiModelProperty(required = true, value = "the version of the job configuration")
  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public Job name(String name) {
    this.name = name;
    return this;
  }

   /**
   * the job name
   * @return name
  **/
  @ApiModelProperty(required = true, value = "the job name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Job summary(JobSummary summary) {
    this.summary = summary;
    return this;
  }

   /**
   * Get summary
   * @return summary
  **/
  @ApiModelProperty(value = "")
  public JobSummary getSummary() {
    return summary;
  }

  public void setSummary(JobSummary summary) {
    this.summary = summary;
  }

  public Job engine(Engine engine) {
    this.engine = engine;
    return this;
  }

   /**
   * Get engine
   * @return engine
  **/
  @ApiModelProperty(required = true, value = "")
  public Engine getEngine() {
    return engine;
  }

  public void setEngine(Engine engine) {
    this.engine = engine;
  }

  public Job streams(List<Stream> streams) {
    this.streams = streams;
    return this;
  }

  public Job addStreamsItem(Stream streamsItem) {
    this.streams.add(streamsItem);
    return this;
  }

   /**
   * Get streams
   * @return streams
  **/
  @ApiModelProperty(required = true, value = "")
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
    Job job = (Job) o;
    return Objects.equals(this.id, job.id) &&
        Objects.equals(this.version, job.version) &&
        Objects.equals(this.name, job.name) &&
        Objects.equals(this.summary, job.summary) &&
        Objects.equals(this.engine, job.engine) &&
        Objects.equals(this.streams, job.streams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, version, name, summary, engine, streams);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Job {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    summary: ").append(toIndentedString(summary)).append("\n");
    sb.append("    engine: ").append(toIndentedString(engine)).append("\n");
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

