package com.hurence.logisland.processor.webanalytics.util;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FirstUserVisitExtraFieldsParserTest {

    @Test
    public void nominalCases() {

        assertEquals(Collections.emptyMap(), new FirstUserVisitExtraFieldsParser().parseFields(""));
        assertEquals(Collections.singletonMap("field", "field"), new FirstUserVisitExtraFieldsParser().parseFields("field"));
        assertEquals(Collections.singletonMap("field", "field.raw"), new FirstUserVisitExtraFieldsParser().parseFields("field:field.raw"));
        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put("field1", "field1.raw");
        expectedMap.put("field2", "field2");
        expectedMap.put("field3", "field3.raw");
        assertEquals(expectedMap, new FirstUserVisitExtraFieldsParser().parseFields("field1:field1.raw,field2,field3:field3.raw"));
    }

}
