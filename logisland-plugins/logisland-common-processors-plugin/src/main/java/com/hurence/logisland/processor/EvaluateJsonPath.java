/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@Tags({"JSON", "evaluate", "JsonPath"})
@CapabilityDescription("Evaluates one or more JsonPath expressions against the content of a FlowFile. "
        + "The results of those expressions are assigned to Records Fields"
        + "depending on configuration of the Processor. "
        + "JsonPaths are entered by adding user-defined properties; the name of the property maps to the Field Name "
        + "into which the result will be placed. "
        + "The value of the property must be a valid JsonPath expression. "
        + "A Return Type of 'auto-detect' will make a determination based off the configured destination. "
        + "If the JsonPath evaluates to a JSON array or JSON object and the Return Type is set to 'scalar' the Record will be routed to error. "
        + "A Return Type of JSON can return scalar values if the provided JsonPath evaluates to the specified value. "
        + "If the expression matches nothing, Fields will be created with empty strings as the value ")
@DynamicProperty(name = "A Record field",
        value = "A JsonPath expression",
        description = "will be set to any JSON objects that match the JsonPath. ")
public class EvaluateJsonPath extends AbstractJsonPathProcessor {


    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_JSON = "json";
    public static final String RETURN_TYPE_SCALAR = "scalar";

    public static final String PATH_NOT_FOUND_IGNORE = "ignore";
    public static final String PATH_NOT_FOUND_WARN = "warn";


    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type").description("Indicates the desired return type of the JSON Path expressions.  " +
                    "Selecting 'auto-detect' will set the return type to 'json'  or 'scalar' ")
            .required(true)
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_JSON, RETURN_TYPE_SCALAR)
            .defaultValue(RETURN_TYPE_AUTO)
            .build();

    public static final PropertyDescriptor PATH_NOT_FOUND = new PropertyDescriptor.Builder()
            .name("Path Not Found Behavior")
            .description("Indicates how to handle missing JSON path expressions. Selecting 'warn' will "
                    + "generate a warning when a JSON path expression is not found.")
            .required(true)
            .allowableValues(PATH_NOT_FOUND_WARN, PATH_NOT_FOUND_IGNORE)
            .defaultValue(PATH_NOT_FOUND_IGNORE)
            .build();


    private List<PropertyDescriptor> properties;

    private final ConcurrentMap<String, JsonPath> cachedJsonPathMap = new ConcurrentHashMap<>();

    @Override
    public void init(final ProcessContext context) {

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RETURN_TYPE);
        properties.add(PATH_NOT_FOUND);
        properties.add(NULL_VALUE_DEFAULT_REPRESENTATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));


            int jsonPathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    jsonPathCount++;
                }
            }

            if (jsonPathCount != 1) {
                results.add(new ValidationResult.Builder().subject("JsonPaths").valid(false)
                        .explanation("Exactly one JsonPath must be set if using d").build());
            }


        return results;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(new JsonPathValidator() {

                    @Override
                    public ValidationResult validate(String subject, String input) {
                        return null;
                    }

                    @Override
                    public void cacheComputedValue(String subject, String input, JsonPath computedJsonPath) {
                        cachedJsonPathMap.put(input, computedJsonPath);
                    }

                    @Override
                    public boolean isStale(String subject, String input) {
                        return cachedJsonPathMap.get(input) == null;
                    }
                })
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.isDynamic()) {
            if (!StringUtils.equals(oldValue, newValue)) {
                if (oldValue != null) {
                    cachedJsonPathMap.remove(oldValue);
                }
            }
        }
    }

    /**
     * Provides cleanup of the map for any JsonPath values that may have been created. This will remove common values shared between multiple instances, but will be regenerated when the next
     * validation cycle occurs as a result of isStale()
     *
     * @param processContext context
     */
  /*  @OnRemoved
    public void onRemoved(ProcessContext processContext) {
        for (PropertyDescriptor propertyDescriptor : getPropertyDescriptors()) {
            if (propertyDescriptor.isDynamic()) {
                cachedJsonPathMap.remove(processContext.getProperty(propertyDescriptor).getValue());
            }
        }
    }*/
    @Override
    public Collection<Record> process(ProcessContext processContext, Collection<Record> records) throws ProcessException {


        String representationOption = processContext.getPropertyValue(NULL_VALUE_DEFAULT_REPRESENTATION).asString();
        final String nullDefaultValue = NULL_REPRESENTATION_MAP.get(representationOption);

        /* Build the JsonPath expressions from attributes */
        final Map<String, JsonPath> attributeToJsonPathMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : processContext.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final JsonPath jsonPath = JsonPath.compile(entry.getValue());
            attributeToJsonPathMap.put(entry.getKey().getName(), jsonPath);
        }

 /*       final String destination = processContext.getPropertyValue(DESTINATION).asString();
        String returnType = processContext.getPropertyValue(RETURN_TYPE).asString();
        if (returnType.equals(RETURN_TYPE_AUTO)) {
            returnType = destination.equals(DESTINATION_CONTENT) ? RETURN_TYPE_JSON : RETURN_TYPE_SCALAR;
        }

        DocumentContext documentContext = null;
        try {
            documentContext = validateAndEstablishJsonContext(processSession, flowFile);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile});
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> jsonPathResults = new HashMap<>();

        for (final Map.Entry<String, JsonPath> attributeJsonPathEntry : attributeToJsonPathMap.entrySet()) {

            final String jsonPathAttrKey = attributeJsonPathEntry.getKey();
            final JsonPath jsonPathExp = attributeJsonPathEntry.getValue();
            final String pathNotFound = processContext.getPropertyValue(PATH_NOT_FOUND).asString();

            final AtomicReference<Object> resultHolder = new AtomicReference<>(null);
            try {
                final Object result = documentContext.read(jsonPathExp);
                if (returnType.equals(RETURN_TYPE_SCALAR) && !isJsonScalar(result)) {
                    logger.error("Unable to return a scalar value for the expression {} for FlowFile {}. Evaluated value was {}. Transferring to {}.",
                            new Object[]{jsonPathExp.getPath(), flowFile.getId(), result.toString(), REL_FAILURE.getName()});
                    processSession.transfer(flowFile, REL_FAILURE);
                    return;
                }
                resultHolder.set(result);
            } catch (PathNotFoundException e) {

                if (pathNotFound.equals(PATH_NOT_FOUND_WARN)) {
                    logger.warn("FlowFile {} could not find path {} for attribute key {}.",
                            new Object[]{flowFile.getId(), jsonPathExp.getPath(), jsonPathAttrKey}, e);
                }

                if (destination.equals(DESTINATION_ATTRIBUTE)) {
                    jsonPathResults.put(jsonPathAttrKey, StringUtils.EMPTY);
                    continue;
                } else {
                    processSession.transfer(flowFile, REL_NO_MATCH);
                    return;
                }
            }

            final String resultRepresentation = getResultRepresentation(resultHolder.get(), nullDefaultValue);
            switch (destination) {
                case DESTINATION_ATTRIBUTE:
                    jsonPathResults.put(jsonPathAttrKey, resultRepresentation);
                    break;
                case DESTINATION_CONTENT:
                    flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(resultRepresentation.getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    });
                    processSession.getProvenanceReporter().modifyContent(flowFile, "Replaced content with result of expression " + jsonPathExp.getPath());
                    break;
            }
        }
        flowFile = processSession.putAllAttributes(flowFile, jsonPathResults);
        processSession.transfer(flowFile, REL_MATCH);*/
        return Collections.emptyList();
    }
}
