/*
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.logisland.kafka.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

@JsonPropertyOrder(value = {"keytype", "name", "version", "magic"})
public class TopicKey extends RegistryKey {

  private static final int MAGIC_BYTE = 0;
  @NotEmpty
  private String name;
  @Min(1)
  @NotEmpty
  private Integer version;

  public TopicKey(@JsonProperty("name") String name,
                  @JsonProperty("version") int version) {
    super(RegistryKeyType.TOPIC);
    this.magicByte = MAGIC_BYTE;
    this.name = name;
    this.version = version;
  }

  @JsonProperty("name")
  public String getName() {
    return this.name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("version")
  public int getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(int version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    TopicKey that = (TopicKey) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (version != that.version) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + name.hashCode();
    result = 31 * result + version;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{magic=" + this.magicByte + ",");
    sb.append("keytype=" + this.keyType.keyType + ",");
    sb.append("subject=" + this.name + ",");
    sb.append("version=" + this.version + "}");
    return sb.toString();
  }

  @Override
  public int compareTo(RegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      TopicKey otherKey = (TopicKey) o;
      int subjectComp = this.name.compareTo(otherKey.name);
      return subjectComp == 0 ? this.version - otherKey.version : subjectComp;
    } else {
      return compare;
    }
  }
}
