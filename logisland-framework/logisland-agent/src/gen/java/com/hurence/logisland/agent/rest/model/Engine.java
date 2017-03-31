package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Property;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * Engine
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public class Engine   {
  private String name = null;

  private String component = null;

  private List<Property> config = new ArrayList<Property>();

  public Engine name(String name) {
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

  public Engine component(String component) {
    this.component = component;
    return this;
  }

   /**
   * Get component
   * @return component
  **/
  @ApiModelProperty(required = true, value = "")
  public String getComponent() {
    return component;
  }

  public void setComponent(String component) {
    this.component = component;
  }

  public Engine config(List<Property> config) {
    this.config = config;
    return this;
  }

  public Engine addConfigItem(Property configItem) {
    this.config.add(configItem);
    return this;
  }

   /**
   * Get config
   * @return config
  **/
  @ApiModelProperty(required = true, value = "")
  public List<Property> getConfig() {
    return config;
  }

  public void setConfig(List<Property> config) {
    this.config = config;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Engine engine = (Engine) o;
    return Objects.equals(this.name, engine.name) &&
        Objects.equals(this.component, engine.component) &&
        Objects.equals(this.config, engine.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, component, config);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Engine {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    component: ").append(toIndentedString(component)).append("\n");
    sb.append("    config: ").append(toIndentedString(config)).append("\n");
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

