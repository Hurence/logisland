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
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-28T16:28:07.083+01:00")
public class Record   {
  private String id = null;

  private String type = null;

  private String timestampField = "record_time";

  private String rowkeyField = "record_id";

  private List<Field> fields = new ArrayList<Field>();

  public Record id(String id) {
    this.id = id;
    return this;
  }

   /**
   * a unique identifier
   * @return id
  **/
  @ApiModelProperty(value = "a unique identifier")
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Record type(String type) {
    this.type = type;
    return this;
  }

   /**
   * the type of the record
   * @return type
  **/
  @ApiModelProperty(required = true, value = "the type of the record")
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
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
        Objects.equals(this.type, record.type) &&
        Objects.equals(this.timestampField, record.timestampField) &&
        Objects.equals(this.rowkeyField, record.rowkeyField) &&
        Objects.equals(this.fields, record.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, type, timestampField, rowkeyField, fields);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Record {\n");
    
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
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

