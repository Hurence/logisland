package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * Field
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-27T15:35:58.847+01:00")
public class Field   {
  private String name = null;

  private Boolean encrypted = null;

  private Boolean indexed = null;

  private Boolean persistent = null;

  /**
   * the type of the field
   */
  public enum TypeEnum {
    STRING("string"),
    
    LONG("long"),
    
    ARRAY("array"),
    
    FLOAT("float"),
    
    DOUBLE("double"),
    
    BYTES("bytes"),
    
    RECORD("record"),
    
    MAP("map"),
    
    ENUM("enum"),
    
    BOOLEAN("boolean");

    private String value;

    TypeEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private TypeEnum type = TypeEnum.STRING;

  private Object value = null;

  public Field name(String name) {
    this.name = name;
    return this;
  }

   /**
   * a unique identifier for the topic
   * @return name
  **/
  @ApiModelProperty(required = true, value = "a unique identifier for the topic")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Field encrypted(Boolean encrypted) {
    this.encrypted = encrypted;
    return this;
  }

   /**
   * is the field need to be encrypted
   * @return encrypted
  **/
  @ApiModelProperty(value = "is the field need to be encrypted")
  public Boolean getEncrypted() {
    return encrypted;
  }

  public void setEncrypted(Boolean encrypted) {
    this.encrypted = encrypted;
  }

  public Field indexed(Boolean indexed) {
    this.indexed = indexed;
    return this;
  }

   /**
   * is the field need to be indexed to search store
   * @return indexed
  **/
  @ApiModelProperty(value = "is the field need to be indexed to search store")
  public Boolean getIndexed() {
    return indexed;
  }

  public void setIndexed(Boolean indexed) {
    this.indexed = indexed;
  }

  public Field persistent(Boolean persistent) {
    this.persistent = persistent;
    return this;
  }

   /**
   * is the field need to be persisted to data store
   * @return persistent
  **/
  @ApiModelProperty(value = "is the field need to be persisted to data store")
  public Boolean getPersistent() {
    return persistent;
  }

  public void setPersistent(Boolean persistent) {
    this.persistent = persistent;
  }

  public Field type(TypeEnum type) {
    this.type = type;
    return this;
  }

   /**
   * the type of the field
   * @return type
  **/
  @ApiModelProperty(required = true, value = "the type of the field")
  public TypeEnum getType() {
    return type;
  }

  public void setType(TypeEnum type) {
    this.type = type;
  }

  public Field value(Object value) {
    this.value = value;
    return this;
  }

   /**
   * a unique identifier for the topic
   * @return value
  **/
  @ApiModelProperty(value = "a unique identifier for the topic")
  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Field field = (Field) o;
    return Objects.equals(this.name, field.name) &&
        Objects.equals(this.encrypted, field.encrypted) &&
        Objects.equals(this.indexed, field.indexed) &&
        Objects.equals(this.persistent, field.persistent) &&
        Objects.equals(this.type, field.type) &&
        Objects.equals(this.value, field.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, encrypted, indexed, persistent, type, value);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Field {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    encrypted: ").append(toIndentedString(encrypted)).append("\n");
    sb.append("    indexed: ").append(toIndentedString(indexed)).append("\n");
    sb.append("    persistent: ").append(toIndentedString(persistent)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
    sb.append("    value: ").append(toIndentedString(value)).append("\n");
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

