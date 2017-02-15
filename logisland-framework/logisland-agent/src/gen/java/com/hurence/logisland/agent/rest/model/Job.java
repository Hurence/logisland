package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Engine;
import com.hurence.logisland.agent.rest.model.Stream;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * Job
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T12:36:09.930+01:00")
public class Job   {
  private String version = "v1";

  private String name = null;

  private String dateModified = null;

  private String documentation = null;

  private Engine engine = null;

  private List<Stream> streams = new ArrayList<Stream>();

  public Job version(String version) {
    this.version = version;
    return this;
  }

   /**
   * Get version
   * @return version
  **/
  @ApiModelProperty(required = true, value = "")
  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Job name(String name) {
    this.name = name;
    return this;
  }

   /**
   * Get name
   * @return name
  **/
  @ApiModelProperty(required = true, value = "")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Job dateModified(String dateModified) {
    this.dateModified = dateModified;
    return this;
  }

   /**
   * Get dateModified
   * @return dateModified
  **/
  @ApiModelProperty(value = "")
  public String getDateModified() {
    return dateModified;
  }

  public void setDateModified(String dateModified) {
    this.dateModified = dateModified;
  }

  public Job documentation(String documentation) {
    this.documentation = documentation;
    return this;
  }

   /**
   * Get documentation
   * @return documentation
  **/
  @ApiModelProperty(value = "")
  public String getDocumentation() {
    return documentation;
  }

  public void setDocumentation(String documentation) {
    this.documentation = documentation;
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
    return Objects.equals(this.version, job.version) &&
        Objects.equals(this.name, job.name) &&
        Objects.equals(this.dateModified, job.dateModified) &&
        Objects.equals(this.documentation, job.documentation) &&
        Objects.equals(this.engine, job.engine) &&
        Objects.equals(this.streams, job.streams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, name, dateModified, documentation, engine, streams);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Job {\n");
    
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    dateModified: ").append(toIndentedString(dateModified)).append("\n");
    sb.append("    documentation: ").append(toIndentedString(documentation)).append("\n");
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

