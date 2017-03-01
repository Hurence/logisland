package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * FieldType
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-01T22:13:54.411+01:00")
public class FieldType   {
  private String name = null;

  private Boolean encrypted = false;

  private Boolean indexed = true;

  private Boolean persistent = true;

  private Boolean optional = true;

  /**
   * the type of the field
   */
  public enum TypeEnum {
    STRING("string"),
    
    INT("int"),
    
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

  public FieldType name(String name) {
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

  public FieldType encrypted(Boolean encrypted) {
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

  public FieldType indexed(Boolean indexed) {
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

  public FieldType persistent(Boolean persistent) {
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

  public FieldType optional(Boolean optional) {
    this.optional = optional;
    return this;
  }

   /**
   * is the field mandatory
   * @return optional
  **/
  @ApiModelProperty(value = "is the field mandatory")
  public Boolean getOptional() {
    return optional;
  }

  public void setOptional(Boolean optional) {
    this.optional = optional;
  }

  public FieldType type(TypeEnum type) {
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


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldType fieldType = (FieldType) o;
    return Objects.equals(this.name, fieldType.name) &&
        Objects.equals(this.encrypted, fieldType.encrypted) &&
        Objects.equals(this.indexed, fieldType.indexed) &&
        Objects.equals(this.persistent, fieldType.persistent) &&
        Objects.equals(this.optional, fieldType.optional) &&
        Objects.equals(this.type, fieldType.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, encrypted, indexed, persistent, optional, type);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FieldType {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    encrypted: ").append(toIndentedString(encrypted)).append("\n");
    sb.append("    indexed: ").append(toIndentedString(indexed)).append("\n");
    sb.append("    persistent: ").append(toIndentedString(persistent)).append("\n");
    sb.append("    optional: ").append(toIndentedString(optional)).append("\n");
    sb.append("    type: ").append(toIndentedString(type)).append("\n");
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

