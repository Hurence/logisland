package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.hurence.logisland.agent.rest.model.Processor;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;




/**
 * Stream
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T12:36:09.930+01:00")
public class Stream   {
  private String name = null;

  private String component = null;

  private String documentation = null;

  private List<String> kafkaInputTopics = new ArrayList<String>();

  private List<String> kafkaOutputTopics = new ArrayList<String>();

  private List<String> kafkaErrorTopics = new ArrayList<String>();

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
  @ApiModelProperty(required = true, value = "")
  public String getDocumentation() {
    return documentation;
  }

  public void setDocumentation(String documentation) {
    this.documentation = documentation;
  }

  public Stream kafkaInputTopics(List<String> kafkaInputTopics) {
    this.kafkaInputTopics = kafkaInputTopics;
    return this;
  }

  public Stream addKafkaInputTopicsItem(String kafkaInputTopicsItem) {
    this.kafkaInputTopics.add(kafkaInputTopicsItem);
    return this;
  }

   /**
   * Get kafkaInputTopics
   * @return kafkaInputTopics
  **/
  @ApiModelProperty(required = true, value = "")
  public List<String> getKafkaInputTopics() {
    return kafkaInputTopics;
  }

  public void setKafkaInputTopics(List<String> kafkaInputTopics) {
    this.kafkaInputTopics = kafkaInputTopics;
  }

  public Stream kafkaOutputTopics(List<String> kafkaOutputTopics) {
    this.kafkaOutputTopics = kafkaOutputTopics;
    return this;
  }

  public Stream addKafkaOutputTopicsItem(String kafkaOutputTopicsItem) {
    this.kafkaOutputTopics.add(kafkaOutputTopicsItem);
    return this;
  }

   /**
   * Get kafkaOutputTopics
   * @return kafkaOutputTopics
  **/
  @ApiModelProperty(required = true, value = "")
  public List<String> getKafkaOutputTopics() {
    return kafkaOutputTopics;
  }

  public void setKafkaOutputTopics(List<String> kafkaOutputTopics) {
    this.kafkaOutputTopics = kafkaOutputTopics;
  }

  public Stream kafkaErrorTopics(List<String> kafkaErrorTopics) {
    this.kafkaErrorTopics = kafkaErrorTopics;
    return this;
  }

  public Stream addKafkaErrorTopicsItem(String kafkaErrorTopicsItem) {
    this.kafkaErrorTopics.add(kafkaErrorTopicsItem);
    return this;
  }

   /**
   * Get kafkaErrorTopics
   * @return kafkaErrorTopics
  **/
  @ApiModelProperty(value = "")
  public List<String> getKafkaErrorTopics() {
    return kafkaErrorTopics;
  }

  public void setKafkaErrorTopics(List<String> kafkaErrorTopics) {
    this.kafkaErrorTopics = kafkaErrorTopics;
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
  @ApiModelProperty(required = true, value = "")
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
        Objects.equals(this.kafkaInputTopics, stream.kafkaInputTopics) &&
        Objects.equals(this.kafkaOutputTopics, stream.kafkaOutputTopics) &&
        Objects.equals(this.kafkaErrorTopics, stream.kafkaErrorTopics) &&
        Objects.equals(this.processors, stream.processors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, component, documentation, kafkaInputTopics, kafkaOutputTopics, kafkaErrorTopics, processors);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Stream {\n");
    
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    component: ").append(toIndentedString(component)).append("\n");
    sb.append("    documentation: ").append(toIndentedString(documentation)).append("\n");
    sb.append("    kafkaInputTopics: ").append(toIndentedString(kafkaInputTopics)).append("\n");
    sb.append("    kafkaOutputTopics: ").append(toIndentedString(kafkaOutputTopics)).append("\n");
    sb.append("    kafkaErrorTopics: ").append(toIndentedString(kafkaErrorTopics)).append("\n");
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

