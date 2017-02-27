package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Record;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * Topic
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-27T15:35:58.847+01:00")
public class Topic   {
  private Long id = null;

  private Integer version = null;

  private String name = null;

  private String serializer = null;

  private Record valueSchema = null;

  private Record keySchema = null;

  private Long partitions = null;

  private Long replicationFactor = null;

  public Topic id(Long id) {
    this.id = id;
    return this;
  }

   /**
   * a unique identifier for the topic
   * @return id
  **/
  @ApiModelProperty(value = "a unique identifier for the topic")
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Topic version(Integer version) {
    this.version = version;
    return this;
  }

   /**
   * the version of the topic configuration
   * @return version
  **/
  @ApiModelProperty(value = "the version of the topic configuration")
  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

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

  public Topic valueSchema(Record valueSchema) {
    this.valueSchema = valueSchema;
    return this;
  }

   /**
   * Get valueSchema
   * @return valueSchema
  **/
  @ApiModelProperty(required = true, value = "")
  public Record getValueSchema() {
    return valueSchema;
  }

  public void setValueSchema(Record valueSchema) {
    this.valueSchema = valueSchema;
  }

  public Topic keySchema(Record keySchema) {
    this.keySchema = keySchema;
    return this;
  }

   /**
   * Get keySchema
   * @return keySchema
  **/
  @ApiModelProperty(value = "")
  public Record getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(Record keySchema) {
    this.keySchema = keySchema;
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
    return Objects.equals(this.id, topic.id) &&
        Objects.equals(this.version, topic.version) &&
        Objects.equals(this.name, topic.name) &&
        Objects.equals(this.serializer, topic.serializer) &&
        Objects.equals(this.valueSchema, topic.valueSchema) &&
        Objects.equals(this.keySchema, topic.keySchema) &&
        Objects.equals(this.partitions, topic.partitions) &&
        Objects.equals(this.replicationFactor, topic.replicationFactor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, version, name, serializer, valueSchema, keySchema, partitions, replicationFactor);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Topic {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    serializer: ").append(toIndentedString(serializer)).append("\n");
    sb.append("    valueSchema: ").append(toIndentedString(valueSchema)).append("\n");
    sb.append("    keySchema: ").append(toIndentedString(keySchema)).append("\n");
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

