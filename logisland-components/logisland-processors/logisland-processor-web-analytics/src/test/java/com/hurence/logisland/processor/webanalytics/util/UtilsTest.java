package com.hurence.logisland.processor.webanalytics.util;


import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

public class UtilsTest {

    public static void assertMapsAreEqualsIgnoringSomeKeys(Map<String, Object> expectedMap, Map<String, Object> actualMap, String... keysToIgnore) {
        final Map<String, String> wrongValuesMessages = getDiffInMaps(expectedMap, actualMap);
        for (String key : keysToIgnore) {
            wrongValuesMessages.remove(key);
        }
        assertMapEquals(expectedMap, actualMap, wrongValuesMessages);
    }

    private static void assertMapEquals(Map<String, Object> expectedMap, Map<String, Object> actualMap, Map<String, String> wrongValuesMessages) {
        String error = "";
        if (expectedMap.size() != actualMap.size()) {
            error = "expected map size to be " + expectedMap.size() + " but was " + actualMap.size();
            if (expectedMap.size() > actualMap.size()) {
                for (String key : expectedMap.keySet()) {
                    if (!actualMap.containsKey(key)) {
                        error += "\n key " + key + " was expected";
                    }
                }
            }
        }
        String mapAsString = wrongValuesMessages.keySet().stream()
                .map(key -> key + " : " + wrongValuesMessages.get(key))
                .collect(Collectors.joining(",\n", "{", "}"));
        if (!error.isEmpty() || !wrongValuesMessages.isEmpty()) {
            String msg = error + "\n" + mapAsString;
            fail(msg);
        }
        return;
    }

    public static void assertMapsAreEquals(Map<String, Object> expectedMap, Map<String, Object> actualMap) {
        final Map<String, String> wrongValuesMessages = getDiffInMaps(expectedMap, actualMap);
        assertMapEquals(expectedMap, actualMap, wrongValuesMessages);
    }


    private static Map<String, String> getDiffInMaps(Map<String, Object> expectedMap, Map<String, Object> actualMap) {
        final Map<String, String> wrongValuesMessages = new HashMap<>();
        actualMap.forEach(handleEntry(expectedMap, wrongValuesMessages));
        return wrongValuesMessages;
    }

    @NotNull
    private static BiConsumer<String, Object> handleEntry(Map<String, Object> expectedMap, Map<String, String> wrongValuesMessages) {
        return (key, value) -> handleEntry(key, value, expectedMap, wrongValuesMessages);
    }

    private static void handleEntry(String key, Object value, Map<String, Object> expectedMap, Map<String, String> wrongValuesMessages) {
        if (!expectedMap.containsKey(key)) {
            wrongValuesMessages.put(key, "Did not expect this key to exist");
            return;
        }
        Object expectedValue = expectedMap.get(key);
        if (expectedValue == null && value != null) {
            wrongValuesMessages.put(key, "contain '" + value + "' but null was expected");
            return;
        }
        if (expectedValue == null) {//both null
            return;
        }
        if (expectedValue.getClass().isArray()) {
            if (!value.getClass().isArray()) {
                wrongValuesMessages.put(key, "contain '" + value + "' which is not an array but was expected an array");
                return;
            }
            if (Arrays.equals((Object[]) expectedValue, (Object[]) value)) return;
            wrongValuesMessages.put(key, "contain '" + value + "' but " + expectedValue + " was expected");
        }
        if (expectedValue.equals(value)) return;
        wrongValuesMessages.put(key, "contain '" + value + "' but " + expectedValue + " was expected");
    }

}

