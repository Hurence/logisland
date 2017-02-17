package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;




/**
 * Metrics
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T18:31:14.122+01:00")
public class Metrics   {
  private String sparkAppName = null;

  private Integer sparkPartitionId = null;

  private String componentName = null;

  private String inputTopics = null;

  private String outputTopics = null;

  private Long topicOffsetFrom = null;

  private Long topicOffsetUntil = null;

  private Integer numIncomingMessages = null;

  private Integer numIncomingRecords = null;

  private Integer numOutgoingRecords = null;

  private Long numErrorsRecords = null;

  private Float errorPercentage = null;

  private Integer averageBytesPerField = null;

  private Integer averageBytesPerSecond = null;

  private Integer averageNumRecordsPerSecond = null;

  private Integer averageFieldsPerRecord = null;

  private Integer averageBytesPerRecord = null;

  private Integer totalBytes = null;

  private Integer totalFields = null;

  private Long totalProcessingTimeInMs = null;

  public Metrics sparkAppName(String sparkAppName) {
    this.sparkAppName = sparkAppName;
    return this;
  }

   /**
   * Get sparkAppName
   * @return sparkAppName
  **/
  @ApiModelProperty(value = "")
  public String getSparkAppName() {
    return sparkAppName;
  }

  public void setSparkAppName(String sparkAppName) {
    this.sparkAppName = sparkAppName;
  }

  public Metrics sparkPartitionId(Integer sparkPartitionId) {
    this.sparkPartitionId = sparkPartitionId;
    return this;
  }

   /**
   * Get sparkPartitionId
   * @return sparkPartitionId
  **/
  @ApiModelProperty(value = "")
  public Integer getSparkPartitionId() {
    return sparkPartitionId;
  }

  public void setSparkPartitionId(Integer sparkPartitionId) {
    this.sparkPartitionId = sparkPartitionId;
  }

  public Metrics componentName(String componentName) {
    this.componentName = componentName;
    return this;
  }

   /**
   * Get componentName
   * @return componentName
  **/
  @ApiModelProperty(value = "")
  public String getComponentName() {
    return componentName;
  }

  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  public Metrics inputTopics(String inputTopics) {
    this.inputTopics = inputTopics;
    return this;
  }

   /**
   * Get inputTopics
   * @return inputTopics
  **/
  @ApiModelProperty(value = "")
  public String getInputTopics() {
    return inputTopics;
  }

  public void setInputTopics(String inputTopics) {
    this.inputTopics = inputTopics;
  }

  public Metrics outputTopics(String outputTopics) {
    this.outputTopics = outputTopics;
    return this;
  }

   /**
   * Get outputTopics
   * @return outputTopics
  **/
  @ApiModelProperty(value = "")
  public String getOutputTopics() {
    return outputTopics;
  }

  public void setOutputTopics(String outputTopics) {
    this.outputTopics = outputTopics;
  }

  public Metrics topicOffsetFrom(Long topicOffsetFrom) {
    this.topicOffsetFrom = topicOffsetFrom;
    return this;
  }

   /**
   * Get topicOffsetFrom
   * @return topicOffsetFrom
  **/
  @ApiModelProperty(value = "")
  public Long getTopicOffsetFrom() {
    return topicOffsetFrom;
  }

  public void setTopicOffsetFrom(Long topicOffsetFrom) {
    this.topicOffsetFrom = topicOffsetFrom;
  }

  public Metrics topicOffsetUntil(Long topicOffsetUntil) {
    this.topicOffsetUntil = topicOffsetUntil;
    return this;
  }

   /**
   * Get topicOffsetUntil
   * @return topicOffsetUntil
  **/
  @ApiModelProperty(value = "")
  public Long getTopicOffsetUntil() {
    return topicOffsetUntil;
  }

  public void setTopicOffsetUntil(Long topicOffsetUntil) {
    this.topicOffsetUntil = topicOffsetUntil;
  }

  public Metrics numIncomingMessages(Integer numIncomingMessages) {
    this.numIncomingMessages = numIncomingMessages;
    return this;
  }

   /**
   * Get numIncomingMessages
   * @return numIncomingMessages
  **/
  @ApiModelProperty(value = "")
  public Integer getNumIncomingMessages() {
    return numIncomingMessages;
  }

  public void setNumIncomingMessages(Integer numIncomingMessages) {
    this.numIncomingMessages = numIncomingMessages;
  }

  public Metrics numIncomingRecords(Integer numIncomingRecords) {
    this.numIncomingRecords = numIncomingRecords;
    return this;
  }

   /**
   * Get numIncomingRecords
   * @return numIncomingRecords
  **/
  @ApiModelProperty(value = "")
  public Integer getNumIncomingRecords() {
    return numIncomingRecords;
  }

  public void setNumIncomingRecords(Integer numIncomingRecords) {
    this.numIncomingRecords = numIncomingRecords;
  }

  public Metrics numOutgoingRecords(Integer numOutgoingRecords) {
    this.numOutgoingRecords = numOutgoingRecords;
    return this;
  }

   /**
   * Get numOutgoingRecords
   * @return numOutgoingRecords
  **/
  @ApiModelProperty(value = "")
  public Integer getNumOutgoingRecords() {
    return numOutgoingRecords;
  }

  public void setNumOutgoingRecords(Integer numOutgoingRecords) {
    this.numOutgoingRecords = numOutgoingRecords;
  }

  public Metrics numErrorsRecords(Long numErrorsRecords) {
    this.numErrorsRecords = numErrorsRecords;
    return this;
  }

   /**
   * Get numErrorsRecords
   * @return numErrorsRecords
  **/
  @ApiModelProperty(value = "")
  public Long getNumErrorsRecords() {
    return numErrorsRecords;
  }

  public void setNumErrorsRecords(Long numErrorsRecords) {
    this.numErrorsRecords = numErrorsRecords;
  }

  public Metrics errorPercentage(Float errorPercentage) {
    this.errorPercentage = errorPercentage;
    return this;
  }

   /**
   * Get errorPercentage
   * @return errorPercentage
  **/
  @ApiModelProperty(value = "")
  public Float getErrorPercentage() {
    return errorPercentage;
  }

  public void setErrorPercentage(Float errorPercentage) {
    this.errorPercentage = errorPercentage;
  }

  public Metrics averageBytesPerField(Integer averageBytesPerField) {
    this.averageBytesPerField = averageBytesPerField;
    return this;
  }

   /**
   * Get averageBytesPerField
   * @return averageBytesPerField
  **/
  @ApiModelProperty(value = "")
  public Integer getAverageBytesPerField() {
    return averageBytesPerField;
  }

  public void setAverageBytesPerField(Integer averageBytesPerField) {
    this.averageBytesPerField = averageBytesPerField;
  }

  public Metrics averageBytesPerSecond(Integer averageBytesPerSecond) {
    this.averageBytesPerSecond = averageBytesPerSecond;
    return this;
  }

   /**
   * Get averageBytesPerSecond
   * @return averageBytesPerSecond
  **/
  @ApiModelProperty(value = "")
  public Integer getAverageBytesPerSecond() {
    return averageBytesPerSecond;
  }

  public void setAverageBytesPerSecond(Integer averageBytesPerSecond) {
    this.averageBytesPerSecond = averageBytesPerSecond;
  }

  public Metrics averageNumRecordsPerSecond(Integer averageNumRecordsPerSecond) {
    this.averageNumRecordsPerSecond = averageNumRecordsPerSecond;
    return this;
  }

   /**
   * Get averageNumRecordsPerSecond
   * @return averageNumRecordsPerSecond
  **/
  @ApiModelProperty(value = "")
  public Integer getAverageNumRecordsPerSecond() {
    return averageNumRecordsPerSecond;
  }

  public void setAverageNumRecordsPerSecond(Integer averageNumRecordsPerSecond) {
    this.averageNumRecordsPerSecond = averageNumRecordsPerSecond;
  }

  public Metrics averageFieldsPerRecord(Integer averageFieldsPerRecord) {
    this.averageFieldsPerRecord = averageFieldsPerRecord;
    return this;
  }

   /**
   * Get averageFieldsPerRecord
   * @return averageFieldsPerRecord
  **/
  @ApiModelProperty(value = "")
  public Integer getAverageFieldsPerRecord() {
    return averageFieldsPerRecord;
  }

  public void setAverageFieldsPerRecord(Integer averageFieldsPerRecord) {
    this.averageFieldsPerRecord = averageFieldsPerRecord;
  }

  public Metrics averageBytesPerRecord(Integer averageBytesPerRecord) {
    this.averageBytesPerRecord = averageBytesPerRecord;
    return this;
  }

   /**
   * Get averageBytesPerRecord
   * @return averageBytesPerRecord
  **/
  @ApiModelProperty(value = "")
  public Integer getAverageBytesPerRecord() {
    return averageBytesPerRecord;
  }

  public void setAverageBytesPerRecord(Integer averageBytesPerRecord) {
    this.averageBytesPerRecord = averageBytesPerRecord;
  }

  public Metrics totalBytes(Integer totalBytes) {
    this.totalBytes = totalBytes;
    return this;
  }

   /**
   * Get totalBytes
   * @return totalBytes
  **/
  @ApiModelProperty(value = "")
  public Integer getTotalBytes() {
    return totalBytes;
  }

  public void setTotalBytes(Integer totalBytes) {
    this.totalBytes = totalBytes;
  }

  public Metrics totalFields(Integer totalFields) {
    this.totalFields = totalFields;
    return this;
  }

   /**
   * Get totalFields
   * @return totalFields
  **/
  @ApiModelProperty(value = "")
  public Integer getTotalFields() {
    return totalFields;
  }

  public void setTotalFields(Integer totalFields) {
    this.totalFields = totalFields;
  }

  public Metrics totalProcessingTimeInMs(Long totalProcessingTimeInMs) {
    this.totalProcessingTimeInMs = totalProcessingTimeInMs;
    return this;
  }

   /**
   * Get totalProcessingTimeInMs
   * @return totalProcessingTimeInMs
  **/
  @ApiModelProperty(value = "")
  public Long getTotalProcessingTimeInMs() {
    return totalProcessingTimeInMs;
  }

  public void setTotalProcessingTimeInMs(Long totalProcessingTimeInMs) {
    this.totalProcessingTimeInMs = totalProcessingTimeInMs;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Metrics metrics = (Metrics) o;
    return Objects.equals(this.sparkAppName, metrics.sparkAppName) &&
        Objects.equals(this.sparkPartitionId, metrics.sparkPartitionId) &&
        Objects.equals(this.componentName, metrics.componentName) &&
        Objects.equals(this.inputTopics, metrics.inputTopics) &&
        Objects.equals(this.outputTopics, metrics.outputTopics) &&
        Objects.equals(this.topicOffsetFrom, metrics.topicOffsetFrom) &&
        Objects.equals(this.topicOffsetUntil, metrics.topicOffsetUntil) &&
        Objects.equals(this.numIncomingMessages, metrics.numIncomingMessages) &&
        Objects.equals(this.numIncomingRecords, metrics.numIncomingRecords) &&
        Objects.equals(this.numOutgoingRecords, metrics.numOutgoingRecords) &&
        Objects.equals(this.numErrorsRecords, metrics.numErrorsRecords) &&
        Objects.equals(this.errorPercentage, metrics.errorPercentage) &&
        Objects.equals(this.averageBytesPerField, metrics.averageBytesPerField) &&
        Objects.equals(this.averageBytesPerSecond, metrics.averageBytesPerSecond) &&
        Objects.equals(this.averageNumRecordsPerSecond, metrics.averageNumRecordsPerSecond) &&
        Objects.equals(this.averageFieldsPerRecord, metrics.averageFieldsPerRecord) &&
        Objects.equals(this.averageBytesPerRecord, metrics.averageBytesPerRecord) &&
        Objects.equals(this.totalBytes, metrics.totalBytes) &&
        Objects.equals(this.totalFields, metrics.totalFields) &&
        Objects.equals(this.totalProcessingTimeInMs, metrics.totalProcessingTimeInMs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sparkAppName, sparkPartitionId, componentName, inputTopics, outputTopics, topicOffsetFrom, topicOffsetUntil, numIncomingMessages, numIncomingRecords, numOutgoingRecords, numErrorsRecords, errorPercentage, averageBytesPerField, averageBytesPerSecond, averageNumRecordsPerSecond, averageFieldsPerRecord, averageBytesPerRecord, totalBytes, totalFields, totalProcessingTimeInMs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Metrics {\n");
    
    sb.append("    sparkAppName: ").append(toIndentedString(sparkAppName)).append("\n");
    sb.append("    sparkPartitionId: ").append(toIndentedString(sparkPartitionId)).append("\n");
    sb.append("    componentName: ").append(toIndentedString(componentName)).append("\n");
    sb.append("    inputTopics: ").append(toIndentedString(inputTopics)).append("\n");
    sb.append("    outputTopics: ").append(toIndentedString(outputTopics)).append("\n");
    sb.append("    topicOffsetFrom: ").append(toIndentedString(topicOffsetFrom)).append("\n");
    sb.append("    topicOffsetUntil: ").append(toIndentedString(topicOffsetUntil)).append("\n");
    sb.append("    numIncomingMessages: ").append(toIndentedString(numIncomingMessages)).append("\n");
    sb.append("    numIncomingRecords: ").append(toIndentedString(numIncomingRecords)).append("\n");
    sb.append("    numOutgoingRecords: ").append(toIndentedString(numOutgoingRecords)).append("\n");
    sb.append("    numErrorsRecords: ").append(toIndentedString(numErrorsRecords)).append("\n");
    sb.append("    errorPercentage: ").append(toIndentedString(errorPercentage)).append("\n");
    sb.append("    averageBytesPerField: ").append(toIndentedString(averageBytesPerField)).append("\n");
    sb.append("    averageBytesPerSecond: ").append(toIndentedString(averageBytesPerSecond)).append("\n");
    sb.append("    averageNumRecordsPerSecond: ").append(toIndentedString(averageNumRecordsPerSecond)).append("\n");
    sb.append("    averageFieldsPerRecord: ").append(toIndentedString(averageFieldsPerRecord)).append("\n");
    sb.append("    averageBytesPerRecord: ").append(toIndentedString(averageBytesPerRecord)).append("\n");
    sb.append("    totalBytes: ").append(toIndentedString(totalBytes)).append("\n");
    sb.append("    totalFields: ").append(toIndentedString(totalFields)).append("\n");
    sb.append("    totalProcessingTimeInMs: ").append(toIndentedString(totalProcessingTimeInMs)).append("\n");
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

