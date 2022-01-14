package com.hurence.logisland.processor.webanalytics.modele;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FirstUserVisitCompositeKeyTest {

    @Test
    public void testEquals() {
        Map<String, String> additionalAttributes = Collections.singletonMap("someFieldA", "someFieldValueA");
        FirstUserVisitCompositeKey refKey = new FirstUserVisitCompositeKey("userA", additionalAttributes);

        assertEquals(refKey, new FirstUserVisitCompositeKey("userA", mapOf("someFieldA", "someFieldValueA")));
        assertNotEquals(refKey, new FirstUserVisitCompositeKey("userA", mapOf()));
        assertNotEquals(refKey, new FirstUserVisitCompositeKey("userB", mapOf("someFieldA", "someFieldValueA")));
        assertNotEquals(refKey, new FirstUserVisitCompositeKey("userA", mapOf("someFieldA", "someFieldValueB")));
        assertNotEquals(refKey, new FirstUserVisitCompositeKey("userA", mapOf("someFieldA", "someFieldValueA", "someFieldB", "someFieldValueB")));
        assertNotEquals(refKey, new FirstUserVisitCompositeKey("userA", null));
    }

    static Map<String, String> mapOf(String... keyValueArray) {
        if (keyValueArray.length % 2 != 0) {
            throw new IllegalArgumentException("keyValueArray array length should be even (key value pairs).");
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keyValueArray.length ; i = i + 2) {
            map.put(keyValueArray[i], keyValueArray[i+1]);
        }
        return map;
    }
}
