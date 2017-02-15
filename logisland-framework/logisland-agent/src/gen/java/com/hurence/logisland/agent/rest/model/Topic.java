package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * Topic
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T12:36:09.930+01:00")
public class Topic   {
  private String name = null;

  private String serializer = null;

  private String schema = null;

  private Long partitions = null;

  private Long replicationFactor = null;

  public Topic name(String name) {
    this.name = name;
    return this;
  }

   /**
   * the name of the topic
   * @return name
  **/
  @ApiModelProperty(required = true, value = "the name of the topic")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Topic serializer(String serializer) {
    this.serializer = serializer;
    return this;
  }

   /**
   * the class of the Serializer
   * @return serializer
  **/
  @ApiModelProperty(required = true, value = "the class of the Serializer")
  public String getSerializer() {
    return serializer;
  }

  public void setSerializer(String serializer) {
    this.serializer = serializer;
  }

  public Topic schema(String schema) {
    this.schema = schema;
    return this;
  }

   /**
   * Avro schema as a json string
   * @return schema
  **/
  @ApiModelProperty(required = true, value = "Avro schema as a json string")
  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public Topic partitions(Long partitions) {
    this.partitions = partitions;
    return this;
  }

   /**
   * default number of partitions
   * @return partitions
  **/
  @ApiModelProperty(required = true, value = "default number of partitions")
  public Long getPartitions() {
    return partitions;
  }

  public void setPartitions(Long partitions) {
    this.partitions = partitions;
  }

  public Topic replicationFactor(Long replicationFactor) {
    this.replicationFactor = replicationFactor;
    return this;
  }

   /**
   * default replication factor
   * @return replicationFactor
  **/
  @ApiModelProperty(required = true, value = "default replication factor")
  public Long getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(Long replicationFactor) {
    this.replicationFactor = replicationFactor;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Topic topic = (Topic) o;
    return Objects.equals(this.name, topic.name) &&
        Objects.equals(this.serializer, topic.serializer) &&
        Objects.equals(this.schema, topic.schema) &&
        Objects.equals(this.partitions, topic.partitions) &&
        Objects.equals(this.replicationFactor, topic.replicationFactor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, serializer, schema, partitions, replicationFactor);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Topic {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    serializer: ").append(toIndentedString(serializer)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("    partitions: ").append(toIndentedString(partitions)).append("\n");
    sb.append("    replicationFactor: ").append(toIndentedString(replicationFactor)).append("\n");
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

