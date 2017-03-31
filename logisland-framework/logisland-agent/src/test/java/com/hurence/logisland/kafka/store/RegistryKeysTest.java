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


import com.hurence.logisland.kafka.registry.KafkaRegistry;
import com.hurence.logisland.kafka.serialization.RegistrySerializer;
import com.hurence.logisland.kafka.serialization.Serializer;
import com.hurence.logisland.kafka.store.exceptions.SerializationException;
import com.hurence.logisland.kafka.store.exceptions.StoreException;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class RegistryKeysTest {

  @Test
  public void testTopicKeySerde() {
    String subject = "foo";
    int version = 1;
    TopicKey key = new TopicKey(subject, version);
    Serializer<RegistryKey, RegistryValue> serializer = new RegistrySerializer();
    byte[] serializedKey = null;
    try {
      serializedKey = serializer.serializeKey(key);
    } catch (SerializationException e) {
      fail();
    }
    assertNotNull(serializedKey);
    try {
      RegistryKey deserializedKey = serializer.deserializeKey(serializedKey);
      assertEquals("Deserialized key should be equal to original key", key, deserializedKey);
    } catch (SerializationException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTopicKeyComparator() {
    String subject = "foo";
    RegistryKey key1 = new TopicKey(subject, 0);
    RegistryKey key2 = new TopicKey(subject, Integer.MAX_VALUE);
    assertTrue("key 1 should be less than key2", key1.compareTo(key2) < 0);
    RegistryKey key1Dup = new TopicKey(subject, 0);
    assertEquals("key 1 should be equal to key1Dup", key1, key1Dup);
    String subject4 = "bar";
    RegistryKey key4 = new TopicKey(subject4, Integer.MIN_VALUE);
    assertTrue("key1 should be greater than key4", key1.compareTo(key4) > 0);
    String subject5 = "fo";
    RegistryKey key5 = new TopicKey(subject5, Integer.MIN_VALUE);
    // compare key1 and key5
    assertTrue("key5 should be less than key1", key1.compareTo(key5) > 0);
    RegistryKey[] expectedOrder = {key4, key5, key1, key2};
    testStoreKeyOrder(expectedOrder);
  }

  @Test
  public void testJobKeySerde() {
    String subject = "foo";
    JobKey key1 = new JobKey(null,0);
    JobKey key2 = new JobKey(subject,0);
    Serializer<RegistryKey, RegistryValue> serializer = new RegistrySerializer();
    byte[] serializedKey1 = null;
    byte[] serializedKey2 = null;
    try {
      serializedKey1 = serializer.serializeKey(key1);
      serializedKey2 = serializer.serializeKey(key2);
    } catch (SerializationException e) {
      fail();
    }
    try {
      RegistryKey deserializedKey1 = serializer.deserializeKey(serializedKey1);
      RegistryKey deserializedKey2 = serializer.deserializeKey(serializedKey2);
      assertEquals("Deserialized key should be equal to original key", key1, deserializedKey1);
      assertEquals("Deserialized key should be equal to original key", key2, deserializedKey2);
    } catch (SerializationException e) {
      fail();
    }
  }

  @Test
  public void testJobKeyComparator() {
    JobKey key1 = new JobKey(null,0);
    JobKey key2 = new JobKey(null,0);
    assertEquals("Top level config keys should be equal", key1, key2);
    String subject = "foo";
    JobKey key3 = new JobKey(subject,0);
    assertTrue("Top level config should be less than subject level config",
               key1.compareTo(key3) < 0);
    String subject4 = "bar";
    JobKey key4 = new JobKey(subject4,0);
    assertTrue("key3 should be greater than key4", key3.compareTo(key4) > 0);
    RegistryKey[] expectedOrder = {key1, key4, key3};
    testStoreKeyOrder(expectedOrder);
  }

  @Test
  public void testKeyComparator() {
    String subject = "foo";
    JobKey topLevelJobKey = new JobKey(null,0);
    JobKey subjectLevelJobKey = new JobKey(subject,0);
    TopicKey schemaKey = new TopicKey(subject, 1);
    TopicKey schemaKeyWithHigherVersion = new TopicKey(subject, 2);
    RegistryKey[]
        expectedOrder =
        {topLevelJobKey, subjectLevelJobKey, schemaKey, schemaKeyWithHigherVersion};
    testStoreKeyOrder(expectedOrder);
  }

  private void testStoreKeyOrder(RegistryKey[] orderedKeys) {
    int numKeys = orderedKeys.length;
    InMemoryStore<RegistryKey, String> store = new InMemoryStore<RegistryKey, String>();
    while (--numKeys >= 0) {
      try {
        store.put(orderedKeys[numKeys], orderedKeys[numKeys].toString());
      } catch (StoreException e) {
        fail("Error writing key " + orderedKeys[numKeys].toString() + " to the in memory store");
      }
    }
    // test key order
    try {
      Iterator<RegistryKey> keys = store.getAllKeys();
      RegistryKey[] retrievedKeyOrder = new RegistryKey[orderedKeys.length];
      int keyIndex = 0;
      while (keys.hasNext()) {
        retrievedKeyOrder[keyIndex++] = keys.next();
      }
      assertArrayEquals(orderedKeys, retrievedKeyOrder);
    } catch (StoreException e) {
      fail();
    }
  }
}
