package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Processor;
import com.hurence.logisland.agent.rest.model.Property;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * Stream
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-27T15:35:58.847+01:00")
public class Stream   {
  private String name = null;

  private String component = null;

  private String documentation = null;

  private List<Property> config = new ArrayList<Property>();

  private List<Processor> processors = new ArrayList<Processor>();

  public Stream name(String name) {
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

  public Stream component(String component) {
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

  public Stream documentation(String documentation) {
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

  public Stream config(List<Property> config) {
    this.config = config;
    return this;
  }

  public Stream addConfigItem(Property configItem) {
    this.config.add(configItem);
    return this;
  }

   /**
   * Get config
   * @return config
  **/
  @ApiModelProperty(value = "")
  public List<Property> getConfig() {
    return config;
  }

  public void setConfig(List<Property> config) {
    this.config = config;
  }

  public Stream processors(List<Processor> processors) {
    this.processors = processors;
    return this;
  }

  public Stream addProcessorsItem(Processor processorsItem) {
    this.processors.add(processorsItem);
    return this;
  }

   /**
   * Get processors
   * @return processors
  **/
  @ApiModelProperty(value = "")
  public List<Processor> getProcessors() {
    return processors;
  }

  public void setProcessors(List<Processor> processors) {
    this.processors = processors;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Stream stream = (Stream) o;
    return Objects.equals(this.name, stream.name) &&
        Objects.equals(this.component, stream.component) &&
        Objects.equals(this.documentation, stream.documentation) &&
        Objects.equals(this.config, stream.config) &&
        Objects.equals(this.processors, stream.processors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, component, documentation, config, processors);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Stream {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    component: ").append(toIndentedString(component)).append("\n");
    sb.append("    documentation: ").append(toIndentedString(documentation)).append("\n");
    sb.append("    config: ").append(toIndentedString(config)).append("\n");
    sb.append("    processors: ").append(toIndentedString(processors)).append("\n");
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

