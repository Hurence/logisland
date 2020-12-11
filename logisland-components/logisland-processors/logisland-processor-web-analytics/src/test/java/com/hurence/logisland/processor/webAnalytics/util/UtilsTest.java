package com.hurence.logisland.processor.webAnalytics.util;

import com.hurence.logisland.processor.webAnalytics.modele.Event;
import com.hurence.logisland.processor.webAnalytics.modele.TestMappings;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

public class UtilsTest {

    @Test
    public void testClone() {
        Event event0 = new Event(
                new WebEvent("0", "session","user", 0L, "url"),
                TestMappings.eventsInternalFields
        );
        Record record = event0.cloneRecord();
        Utils.toMap(record, false);
    }

    @Test
    public void test() {
        Record record = new StandardRecord();
        record.setStringField("string", "value");
        record.setBooleanField("bool", true);
        record.setBooleanField("bool2", false);
        record.setDoubleField("double", 0.2d);
        record.setLongField("long", 45L);
        record.setIntField("int", 5);
        record.setFloatField("float", 5.6f);
        record.setBytesField("bytes", new Byte[]{1, 2});
        record.setArrayField("array", Arrays.asList("1", "2"));
        record.setDateTimeField("date", new Date(0));
//        record.setRecordField("record", "value");
//        record.setObjectField("object", "value");
        Map<String, Object> mapFromRecord = Utils.toMap(record, false);
        Map<String, Object> mapExpected = new HashMap<>();
        mapExpected.put("string" ,"value");
        mapExpected.put("bool" ,true);
        mapExpected.put("bool2" ,false);
        mapExpected.put("double" ,0.2d);
        mapExpected.put("long" ,45L);
        mapExpected.put("int" ,5);
        mapExpected.put("float" ,5.6f);
        mapExpected.put("bytes" ,new Byte[]{1, 2});
        mapExpected.put("array" ,Arrays.asList("1", "2"));
        mapExpected.put("date" ,new Date(0));
        //{date=Thu Jan 01 01:00:00 CET 1970, bool=true, string=value, double=0.2, float=5.6, int=5, long=45, record_type=generic, bool2=false, record_id=47fbd588-f70c-4a85-a2bc-1c690cb88e60, array=[1, 2], bytes=[Ljava.lang.Byte;@2d6e8792, record_time=1607619730231}
        assertMapsAreEquals(mapExpected, mapFromRecord);
    }

    @Test
    public void test2() {
        Record record = new StandardRecord();
        record.setStringField("string", "value");
        record.setBooleanField("bool", true);
        record.setBooleanField("bool2", false);
        record.setDoubleField("double", 0.2d);
        record.setLongField("long", 45L);
        record.setIntField("int", 5);
        record.setFloatField("float", 5.6f);
        record.setBytesField("bytes", new Byte[]{1, 2});
        record.setArrayField("array", Arrays.asList("1", "2"));
        record.setDateTimeField("date", new Date(0));
//        record.setRecordField("record", "value");
//        record.setObjectField("object", "value");
        Map<String, Object> mapFromRecord = Utils.toMap(record, true);
        Map<String, Object> mapExpected = new HashMap<>();
        mapExpected.put("string" ,"value");
        mapExpected.put("bool" ,true);
        mapExpected.put("bool2" ,false);
        mapExpected.put("double" ,0.2d);
        mapExpected.put("long" ,45L);
        mapExpected.put("int" ,5);
        mapExpected.put("float" ,5.6f);
        mapExpected.put("bytes" ,new Byte[]{1, 2});
        mapExpected.put("array" ,Arrays.asList("1", "2"));
        mapExpected.put("date" ,new Date(0));
//        {date=Thu Jan 01 01:00:00 CET 1970, bool2=false, bool=true, string=value, array=[1, 2], bytes=[Ljava.lang.Byte;@22f71333, double=0.2, float=5.6, int=5, long=45}
        assertMapsAreEquals(mapExpected, mapFromRecord);

    }

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

