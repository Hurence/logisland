package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.FieldType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;




/**
 * Topic
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-02T15:08:39.722+01:00")
public class Topic   {
  private Long id = null;

  private Integer version = null;

  private String name = null;

  private Integer partitions = null;

  private Integer replicationFactor = null;

  private Date dateModified = null;

  private String documentation = null;

  private String serializer = "com.hurence.logisland.serializer.KryoSerializer";

  private String businessTimeField = "record_time";

  private String rowkeyField = "record_id";

  private String recordTypeField = "record_type";

  private List<FieldType> keySchema = new ArrayList<FieldType>();

  private List<FieldType> valueSchema = new ArrayList<FieldType>();

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

  public Topic partitions(Integer partitions) {
    this.partitions = partitions;
    return this;
  }

   /**
   * default number of partitions
   * @return partitions
  **/
  @ApiModelProperty(required = true, value = "default number of partitions")
  public Integer getPartitions() {
    return partitions;
  }

  public void setPartitions(Integer partitions) {
    this.partitions = partitions;
  }

  public Topic replicationFactor(Integer replicationFactor) {
    this.replicationFactor = replicationFactor;
    return this;
  }

   /**
   * default replication factor
   * @return replicationFactor
  **/
  @ApiModelProperty(required = true, value = "default replication factor")
  public Integer getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(Integer replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public Topic dateModified(Date dateModified) {
    this.dateModified = dateModified;
    return this;
  }

   /**
   * latest date of modification
   * @return dateModified
  **/
  @ApiModelProperty(value = "latest date of modification")
  public Date getDateModified() {
    return dateModified;
  }

  public void setDateModified(Date dateModified) {
    this.dateModified = dateModified;
  }

  public Topic documentation(String documentation) {
    this.documentation = documentation;
    return this;
  }

   /**
   * the description of the topic
   * @return documentation
  **/
  @ApiModelProperty(value = "the description of the topic")
  public String getDocumentation() {
    return documentation;
  }

  public void setDocumentation(String documentation) {
    this.documentation = documentation;
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

  public Topic businessTimeField(String businessTimeField) {
    this.businessTimeField = businessTimeField;
    return this;
  }

   /**
   * the record_time field
   * @return businessTimeField
  **/
  @ApiModelProperty(value = "the record_time field")
  public String getBusinessTimeField() {
    return businessTimeField;
  }

  public void setBusinessTimeField(String businessTimeField) {
    this.businessTimeField = businessTimeField;
  }

  public Topic rowkeyField(String rowkeyField) {
    this.rowkeyField = rowkeyField;
    return this;
  }

   /**
   * the record_id field
   * @return rowkeyField
  **/
  @ApiModelProperty(value = "the record_id field")
  public String getRowkeyField() {
    return rowkeyField;
  }

  public void setRowkeyField(String rowkeyField) {
    this.rowkeyField = rowkeyField;
  }

  public Topic recordTypeField(String recordTypeField) {
    this.recordTypeField = recordTypeField;
    return this;
  }

   /**
   * the record type field
   * @return recordTypeField
  **/
  @ApiModelProperty(value = "the record type field")
  public String getRecordTypeField() {
    return recordTypeField;
  }

  public void setRecordTypeField(String recordTypeField) {
    this.recordTypeField = recordTypeField;
  }

  public Topic keySchema(List<FieldType> keySchema) {
    this.keySchema = keySchema;
    return this;
  }

  public Topic addKeySchemaItem(FieldType keySchemaItem) {
    this.keySchema.add(keySchemaItem);
    return this;
  }

   /**
   * Get keySchema
   * @return keySchema
  **/
  @ApiModelProperty(value = "")
  public List<FieldType> getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(List<FieldType> keySchema) {
    this.keySchema = keySchema;
  }

  public Topic valueSchema(List<FieldType> valueSchema) {
    this.valueSchema = valueSchema;
    return this;
  }

  public Topic addValueSchemaItem(FieldType valueSchemaItem) {
    this.valueSchema.add(valueSchemaItem);
    return this;
  }

   /**
   * Get valueSchema
   * @return valueSchema
  **/
  @ApiModelProperty(required = true, value = "")
  public List<FieldType> getValueSchema() {
    return valueSchema;
  }

  public void setValueSchema(List<FieldType> valueSchema) {
    this.valueSchema = valueSchema;
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
        Objects.equals(this.partitions, topic.partitions) &&
        Objects.equals(this.replicationFactor, topic.replicationFactor) &&
        Objects.equals(this.dateModified, topic.dateModified) &&
        Objects.equals(this.documentation, topic.documentation) &&
        Objects.equals(this.serializer, topic.serializer) &&
        Objects.equals(this.businessTimeField, topic.businessTimeField) &&
        Objects.equals(this.rowkeyField, topic.rowkeyField) &&
        Objects.equals(this.recordTypeField, topic.recordTypeField) &&
        Objects.equals(this.keySchema, topic.keySchema) &&
        Objects.equals(this.valueSchema, topic.valueSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, version, name, partitions, replicationFactor, dateModified, documentation, serializer, businessTimeField, rowkeyField, recordTypeField, keySchema, valueSchema);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Topic {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    partitions: ").append(toIndentedString(partitions)).append("\n");
    sb.append("    replicationFactor: ").append(toIndentedString(replicationFactor)).append("\n");
    sb.append("    dateModified: ").append(toIndentedString(dateModified)).append("\n");
    sb.append("    documentation: ").append(toIndentedString(documentation)).append("\n");
    sb.append("    serializer: ").append(toIndentedString(serializer)).append("\n");
    sb.append("    businessTimeField: ").append(toIndentedString(businessTimeField)).append("\n");
    sb.append("    rowkeyField: ").append(toIndentedString(rowkeyField)).append("\n");
    sb.append("    recordTypeField: ").append(toIndentedString(recordTypeField)).append("\n");
    sb.append("    keySchema: ").append(toIndentedString(keySchema)).append("\n");
    sb.append("    valueSchema: ").append(toIndentedString(valueSchema)).append("\n");
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

