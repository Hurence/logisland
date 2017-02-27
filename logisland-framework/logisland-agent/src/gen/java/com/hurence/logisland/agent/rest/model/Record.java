package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Field;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * Record
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-27T15:35:58.847+01:00")
public class Record   {
  private Long id = null;

  private String name = null;

  private Boolean multiline = null;

  private String timestampField = "record_time";

  private String rowkeyField = "record_id";

  private List<Field> fields = new ArrayList<Field>();

  public Record id(Long id) {
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

  public Record name(String name) {
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

  public Record multiline(Boolean multiline) {
    this.multiline = multiline;
    return this;
  }

   /**
   * is the record multiline
   * @return multiline
  **/
  @ApiModelProperty(value = "is the record multiline")
  public Boolean getMultiline() {
    return multiline;
  }

  public void setMultiline(Boolean multiline) {
    this.multiline = multiline;
  }

  public Record timestampField(String timestampField) {
    this.timestampField = timestampField;
    return this;
  }

   /**
   * the record_time field
   * @return timestampField
  **/
  @ApiModelProperty(value = "the record_time field")
  public String getTimestampField() {
    return timestampField;
  }

  public void setTimestampField(String timestampField) {
    this.timestampField = timestampField;
  }

  public Record rowkeyField(String rowkeyField) {
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

  public Record fields(List<Field> fields) {
    this.fields = fields;
    return this;
  }

  public Record addFieldsItem(Field fieldsItem) {
    this.fields.add(fieldsItem);
    return this;
  }

   /**
   * Get fields
   * @return fields
  **/
  @ApiModelProperty(value = "")
  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record record = (Record) o;
    return Objects.equals(this.id, record.id) &&
        Objects.equals(this.name, record.name) &&
        Objects.equals(this.multiline, record.multiline) &&
        Objects.equals(this.timestampField, record.timestampField) &&
        Objects.equals(this.rowkeyField, record.rowkeyField) &&
        Objects.equals(this.fields, record.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, multiline, timestampField, rowkeyField, fields);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Record {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    multiline: ").append(toIndentedString(multiline)).append("\n");
    sb.append("    timestampField: ").append(toIndentedString(timestampField)).append("\n");
    sb.append("    rowkeyField: ").append(toIndentedString(rowkeyField)).append("\n");
    sb.append("    fields: ").append(toIndentedString(fields)).append("\n");
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

