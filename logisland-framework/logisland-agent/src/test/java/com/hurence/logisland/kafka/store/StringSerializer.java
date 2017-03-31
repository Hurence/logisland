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
package com.hurence.logisland.kafka.store;



import com.hurence.logisland.kafka.serialization.Serializer;
import com.hurence.logisland.kafka.store.exceptions.SerializationException;

import java.util.Map;

public class StringSerializer implements Serializer<String, String> {

  public static StringSerializer INSTANCE = new StringSerializer();

  // only a singleton is needed
  private StringSerializer() {
  }

  /**
   * @param key Typed key
   * @return bytes of the serialized key
   */
  @Override
  public byte[] serializeKey(String key) throws SerializationException {
    return key != null ? key.getBytes() : null;
  }

  /**
   * @param value Typed value
   * @return bytes of the serialized value
   */
  @Override
  public byte[] serializeValue(String value) throws SerializationException {
    return value != null ? value.getBytes() : null;
  }

  @Override
  public String deserializeKey(byte[] key) {
    return new String(key);
  }

  /**
   * @param key   Typed key corresponding to this value
   * @param value Bytes of the serialized value
   * @return Typed deserialized value
   */
  @Override
  public String deserializeValue(String key, byte[] value) throws SerializationException {
    return new String(value);
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void configure(Map<String, ?> stringMap) {
    // do nothing
  }
}
