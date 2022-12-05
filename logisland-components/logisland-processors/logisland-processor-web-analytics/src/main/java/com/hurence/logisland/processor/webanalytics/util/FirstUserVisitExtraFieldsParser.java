package com.hurence.logisland.processor.webanalytics.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FirstUserVisitExtraFieldsParser {

    /**
     * Parses inputString to determine the fields, optionally along with their name for ES queries
     * fieldNameA[:fieldNameAForESQuery],fieldNameB[:fieldNameBForESQuery],...
     *
     * The extracted fields cannot be validated here, because they might be inherited from upstream processing and unknown by this processor.
     * Instead an exception is raised in SessionsCalculator when a field cannot be found in the Record object of the websession.
     *
     * @param inputString
     * @return
     */
    public Map<String, String> parseFields(String inputString) {
        Map<String, String> fieldMap = new HashMap<>();
        if (inputString.isEmpty()) {
            return fieldMap;
        }
        if (!inputString.contains(",")) {
            addFieldsToMap(fieldMap, inputString);
        } else {
            Arrays.stream(inputString.split(",")).forEach(split -> addFieldsToMap(fieldMap, split));
        }
        return fieldMap;
    }

    /**
     * Parses the inputString to determine the field name (as in the websession) and optionally the name for the Elasticsearch query
     * The name used in the ES term query is different for text fields e.g. fieldA -> fieldA.raw
     *
     * The inputString format is fieldName[:fieldNameForESQuery], such that for the example above the inputString value should be fieldA:fieldA.raw
     *
     * @param fieldMap
     * @param inputString
     */
    public static void addFieldsToMap(Map<String, String> fieldMap, String inputString) {
        if (!inputString.contains(":")) {
            fieldMap.put(inputString, inputString);
        } else {
            String[] split = inputString.split(":");
            fieldMap.put(split[0], split[1]);
        }
    }
}
