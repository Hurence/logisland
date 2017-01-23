/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.util.JsonPathExpressionValidator;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides common functionality used for processors interacting and manipulating JSON data via JsonPath.
 *
 * @see <a href="http://json.org">http://json.org</a>
 * @see <a href="https://github.com/jayway/JsonPath">https://github.com/jayway/JsonPath</a>
 */
public abstract class AbstractJsonPathProcessor extends AbstractProcessor {

    private static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();

    private static final JsonProvider JSON_PROVIDER = STRICT_PROVIDER_CONFIGURATION.jsonProvider();

    static final Map<String, String> NULL_REPRESENTATION_MAP = new HashMap<>();

    static final String EMPTY_STRING_OPTION = "empty string";
    static final String NULL_STRING_OPTION = "the string 'null'";

    static {
        NULL_REPRESENTATION_MAP.put(EMPTY_STRING_OPTION, "");
        NULL_REPRESENTATION_MAP.put(NULL_STRING_OPTION, "null");
    }

    public static final PropertyDescriptor NULL_VALUE_DEFAULT_REPRESENTATION = new PropertyDescriptor.Builder()
            .name("Null Value Representation")
            .description("Indicates the desired representation of JSON Path expressions resulting in a null value.")
            .required(true)
            .allowableValues(NULL_REPRESENTATION_MAP.keySet())
            .defaultValue(EMPTY_STRING_OPTION)
            .build();

    static DocumentContext validateAndEstablishJsonContext(String jsonContent) {
        // Parse the document once into an associated context to support multiple path evaluations if specified
        final AtomicReference<DocumentContext> contextHolder = new AtomicReference<>(null);

        DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(jsonContent);
        contextHolder.set(ctx);

        return contextHolder.get();
    }

    /**
     * Determines the context by which JsonSmartJsonProvider would treat the value. {@link Map} and {@link List} objects can be rendered as JSON elements, everything else is
     * treated as a scalar.
     *
     * @param obj item to be inspected if it is a scalar or a JSON element
     * @return false, if the object is a supported type; true otherwise
     */
    static boolean isJsonScalar(Object obj) {
        // For the default provider, JsonSmartJsonProvider, a Map or List is able to be handled as a JSON entity
        return !(obj instanceof Map || obj instanceof List);
    }

    static String getResultRepresentation(Object jsonPathResult, String defaultValue) {
        if (isJsonScalar(jsonPathResult)) {
            return Objects.toString(jsonPathResult, defaultValue);
        }
        return JSON_PROVIDER.toJson(jsonPathResult);
    }

    static FieldType getResultType(Object jsonPathResult) {
        if (jsonPathResult instanceof String) {
            return FieldType.STRING;
        } else if (jsonPathResult instanceof Float) {
            return FieldType.FLOAT;
        } else if (jsonPathResult instanceof Double) {
            return FieldType.DOUBLE;
        } else if (jsonPathResult instanceof Integer) {
            return FieldType.INT;
        } else if (jsonPathResult instanceof Long) {
            return FieldType.LONG;
        } else if (jsonPathResult instanceof Boolean) {
            return FieldType.BOOLEAN;
        } else {
            return FieldType.STRING;
        }

    }

    abstract static class JsonPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input) {
            String error = null;
            if (isStale(subject, input)) {
                if (JsonPathExpressionValidator.isValidExpression(input)) {
                    JsonPath compiledJsonPath = JsonPath.compile(input);
                    cacheComputedValue(subject, input, compiledJsonPath);
                } else {
                    error = "specified expression was not valid: " + input;
                }
            }
            return new ValidationResult.Builder().subject(subject).valid(error == null).explanation(error).build();
        }

        /**
         * An optional hook to act on the compute value
         */
        abstract void cacheComputedValue(String subject, String input, JsonPath computedJsonPath);

        /**
         * A hook for implementing classes to determine if a cached value is stale for a compiled JsonPath represented by either a validation
         */
        abstract boolean isStale(String subject, String input);
    }
}
