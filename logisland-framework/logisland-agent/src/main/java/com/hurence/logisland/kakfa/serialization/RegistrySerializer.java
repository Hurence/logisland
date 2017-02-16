/**
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
package com.hurence.logisland.kakfa.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hurence.logisland.kakfa.store.*;
import com.hurence.logisland.kakfa.store.exceptions.SerializationException;

import java.io.IOException;
import java.util.Map;

public class RegistrySerializer   implements Serializer<RegistryKey, RegistryValue> {

  public RegistrySerializer() {

  }

  /**
   * @param key Typed key
   * @return bytes of the serialized key
   */
  @Override
  public byte[] serializeKey(RegistryKey key) throws SerializationException {
    try {
      return new ObjectMapper().writeValueAsBytes(key);
    } catch (IOException e) {
      throw new SerializationException("Error while serializing key" + key.toString(),
                                       e);
    }
  }

  /**
   * @param value Typed value
   * @return bytes of the serialized value
   */
  @Override
  public byte[] serializeValue(RegistryValue value) throws SerializationException {
    try {
      return new ObjectMapper().writeValueAsBytes(value);
    } catch (IOException e) {
      throw new SerializationException(
          "Error while serializing value value " + value.toString(),
          e);
    }
  }

  @Override
  public RegistryKey deserializeKey(byte[] key) throws SerializationException {
    RegistryKey registryKey = null;
    RegistryKeyType keyType = null;
    try {
      try {
        Map<Object, Object> keyObj = null;
        keyObj = new ObjectMapper().readValue(key,
                                              new TypeReference<Map<Object, Object>>() {
                                              });
        keyType = RegistryKeyType.forName((String) keyObj.get("keytype"));
        if (keyType == RegistryKeyType.JOB) {
          registryKey = new ObjectMapper().readValue(key, JobKey.class);
        } else if (keyType == RegistryKeyType.NOOP) {
          registryKey = new ObjectMapper().readValue(key, NoopKey.class);
        } else {
          registryKey = new ObjectMapper().readValue(key, TopicKey.class);
        }
      } catch (JsonProcessingException e) {

        String type = "unknown";
        if (keyType == RegistryKeyType.JOB) {
          type = RegistryKeyType.JOB.name();
        } else if (keyType == RegistryKeyType.TOPIC) {
          type = RegistryKeyType.TOPIC.name();
        } else if (keyType == RegistryKeyType.NOOP) {
          type = RegistryKeyType.NOOP.name();
        } 
        
        throw new SerializationException("Failed to deserialize " + type + " key", e);
      }
    } catch (IOException e) {
      throw new SerializationException("Error while deserializing schema key", e);
    }
    return registryKey;
  }

  /**
   * @param key   Typed key corresponding to this value
   * @param value Bytes of the serialized value
   * @return Typed deserialized value. Must be one of {@link JobValue}
   * or {@link TopicValue}
   */
  @Override
  public RegistryValue deserializeValue(RegistryKey key, byte[] value)
      throws SerializationException {
    RegistryValue schemaRegistryValue = null;
    if (key.getKeyType().equals(RegistryKeyType.JOB)) {
      try {
        schemaRegistryValue = new ObjectMapper().readValue(value, JobValue.class);
      } catch (IOException e) {
        throw new SerializationException("Error while deserializing job", e);
      }
    } else if (key.getKeyType().equals(RegistryKeyType.TOPIC)) {
      try {
        schemaRegistryValue = new ObjectMapper().readValue(value, TopicValue.class);
      } catch (IOException e) {
        throw new SerializationException("Error while deserializing topic", e);
      }
    } else {
      throw new SerializationException("Unrecognized key type. Must be one of schema or config");
    }
    return schemaRegistryValue;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> stringMap) {

  }
}
