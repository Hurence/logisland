package com.hurence.logisland.agent.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;




/**
 * JobSummary
 */
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-01T10:52:58.937+01:00")
public class JobSummary   {
  private Integer usedCores = null;

  private Integer usedMemory = null;

  /**
   * the job status
   */
  public enum StatusEnum {
    STOPPED("stopped"),
    
    RUNNING("running"),
    
    FAILED("failed"),
    
    PAUSED("paused");

    private String value;

    StatusEnum(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private StatusEnum status = StatusEnum.STOPPED;

  private Date dateModified = null;

  private String documentation = null;

  public JobSummary usedCores(Integer usedCores) {
    this.usedCores = usedCores;
    return this;
  }

   /**
   * the number of used cores
   * @return usedCores
  **/
  @ApiModelProperty(value = "the number of used cores")
  public Integer getUsedCores() {
    return usedCores;
  }

  public void setUsedCores(Integer usedCores) {
    this.usedCores = usedCores;
  }

  public JobSummary usedMemory(Integer usedMemory) {
    this.usedMemory = usedMemory;
    return this;
  }

   /**
   * the total memory allocated for this job
   * @return usedMemory
  **/
  @ApiModelProperty(value = "the total memory allocated for this job")
  public Integer getUsedMemory() {
    return usedMemory;
  }

  public void setUsedMemory(Integer usedMemory) {
    this.usedMemory = usedMemory;
  }

  public JobSummary status(StatusEnum status) {
    this.status = status;
    return this;
  }

   /**
   * the job status
   * @return status
  **/
  @ApiModelProperty(value = "the job status")
  public StatusEnum getStatus() {
    return status;
  }

  public void setStatus(StatusEnum status) {
    this.status = status;
  }

  public JobSummary dateModified(Date dateModified) {
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

  public JobSummary documentation(String documentation) {
    this.documentation = documentation;
    return this;
  }

   /**
   * write here what the job is doing
   * @return documentation
  **/
  @ApiModelProperty(value = "write here what the job is doing")
  public String getDocumentation() {
    return documentation;
  }

  public void setDocumentation(String documentation) {
    this.documentation = documentation;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobSummary jobSummary = (JobSummary) o;
    return Objects.equals(this.usedCores, jobSummary.usedCores) &&
        Objects.equals(this.usedMemory, jobSummary.usedMemory) &&
        Objects.equals(this.status, jobSummary.status) &&
        Objects.equals(this.dateModified, jobSummary.dateModified) &&
        Objects.equals(this.documentation, jobSummary.documentation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(usedCores, usedMemory, status, dateModified, documentation);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class JobSummary {\n");
    
    sb.append("    usedCores: ").append(toIndentedString(usedCores)).append("\n");
    sb.append("    usedMemory: ").append(toIndentedString(usedMemory)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    dateModified: ").append(toIndentedString(dateModified)).append("\n");
    sb.append("    documentation: ").append(toIndentedString(documentation)).append("\n");
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

