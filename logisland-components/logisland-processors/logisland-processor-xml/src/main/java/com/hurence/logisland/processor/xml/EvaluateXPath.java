/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Original source from Nifi:
 * https://github.com/apache/nifi/blob/master/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/EvaluateXPath.java
 */
package com.hurence.logisland.processor.xml;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.ValidationResult;
import com.hurence.logisland.validator.Validator;
import net.sf.saxon.lib.NamespaceConstant;
import net.sf.saxon.xpath.XPathEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static javax.xml.xpath.XPathConstants.STRING;


@Tags({"XML", "evaluate", "XPath"})
@CapabilityDescription("Evaluates one or more XPaths against the content of a record. The results of those XPaths are assigned to "
        + "new attributes in the records, depending on configuration of the "
        + "Processor. XPaths are entered by adding user-defined properties; the name of the property maps to the Attribute "
        + "Name into which the result will be placed. "
        + "The value of the property must be a valid XPath expression. If the expression matches nothing, no attributes is added. ")
@WritesAttribute(attribute = "user-defined", description = "This processor adds user-defined attributes.")
@DynamicProperty(name = "An attribute", value = "An XPath expression", description = " "
        + "the attribute is set to the result of the XPath Expression.")

public class EvaluateXPath extends AbstractProcessor {

    private static final String XPATH_FACTORY_IMPL = "net.sf.saxon.xpath.XPathFactoryImpl";
    private static Logger logger = LoggerFactory.getLogger(EvaluateXPath.class);

    protected HashMap<String, XPathExpression> xpathRules = new HashMap<String, XPathExpression>();

    public static final PropertyDescriptor SOURCE = new PropertyDescriptor.Builder()
            .name("source")
            .description("Indicates the attribute containing the xml data to evaluate xpath against.")
            .required(true)
            .build();

    public static final PropertyDescriptor VALIDATE_DTD = new PropertyDescriptor.Builder()
            .name("validate_dtd")
            .description("Specifies whether or not the XML content should be validated against the DTD.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();


    private static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    private static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");


    public static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();



    private List<PropertyDescriptor> properties;

    private final AtomicReference<XPathFactory> factoryRef = new AtomicReference<>();


    @Override
    public void init(ProcessContext context) {
        try {
            factoryRef.set(XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON,
                    XPATH_FACTORY_IMPL,
                    ClassLoader.getSystemClassLoader()));
        } catch (XPathFactoryConfigurationException e) {
            logger.warn(e.toString());
        }
        final XPathFactory factory = factoryRef.get();

        // loop over dynamic properties to add rules
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String name = entry.getKey().getName();
            final String rule = entry.getValue();

            final XPathEvaluator xpathEvaluator = (XPathEvaluator) factory.newXPath();
            XPathExpression xpathExpression;
            try {
                xpathExpression = xpathEvaluator.compile(rule);
                xpathRules.put(name, xpathExpression);
            } catch (XPathExpressionException e) {
                throw new ProcessException(e.getMessage());  // should not happen because we've already validated the XPath (in XPathValidator)
            }
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SOURCE);
        properties.add(VALIDATE_DTD);
        properties.add(CONFLICT_RESOLUTION_POLICY);
        return Collections.unmodifiableList(properties);
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(new XPathValidator())
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        if (records.isEmpty()) {
            return records;
        }

        final String sourceAttrName = context.getPropertyValue(SOURCE).asString();
        final String conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();
        final QName returnType = STRING;

        recordLoop:
        for (Record record : records) {
            final AtomicReference<Throwable> error = new AtomicReference<>(null);
            final AtomicReference<Source> sourceRef = new AtomicReference<>(null);

            // Grab the XML Document to evaluate xpath against
            String xmlDocToEvaluate = record.getField(sourceAttrName).getRawValue().toString();
            if (xmlDocToEvaluate == null || xmlDocToEvaluate.isEmpty()){
                continue recordLoop;
            }


            final Map<String, String> xpathResults = new HashMap<>();

            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            org.w3c.dom.Document xmlDoc = null;
            try {
                javax.xml.parsers.DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                StringBuilder xmlStringBuilder = new StringBuilder();
                xmlStringBuilder.append(xmlDocToEvaluate);
                ByteArrayInputStream input =  new ByteArrayInputStream(
                        xmlStringBuilder.toString().getBytes("UTF-8"));
                xmlDoc = docBuilder.parse(input);
            } catch (ParserConfigurationException e) {
                logger.warn(e.toString());
                continue recordLoop;
            } catch (UnsupportedEncodingException e) {
                logger.warn(e.toString());
                continue recordLoop;
            } catch (IOException e) {
                logger.warn(e.toString());
                continue recordLoop;
            } catch (SAXException e) {
                logger.warn(e.toString());
                continue recordLoop;
            }

            for (final Map.Entry<String, XPathExpression> entry : xpathRules.entrySet()) {
                String result = null;
                try {
                    XPathExpression xpe = entry.getValue();
                    result = (String) xpe.evaluate(xmlDoc, returnType);
                    if (result == null || result.isEmpty()) {
                        continue ;
                    }
                } catch (final XPathExpressionException e) {
                    logger.error("failed to evaluate XPath for {} for Property {} due to {}; routing to failure",
                            new Object[]{record, entry.getKey(), e});
                    continue recordLoop;
                }

                if (returnType == STRING) {
                    final String resultString = (String) result;
                    xpathResults.put(entry.getKey(), resultString);
                    String addedFieldName = entry.getKey();

                    // field is already here
                    if (record.hasField(addedFieldName)) {
                        if (conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                            overwriteObsoleteFieldValue(record, addedFieldName, resultString);
                        }
                    } else {
                        record.setStringField(addedFieldName, resultString);
                    }
                }
            }

            if (error.get() != null) {
                logger.error("Failed to write XPath result for {} due to {}; record left unchanged",
                        new Object[]{record, error.get()});
            }
        }
        return records;
    }

    private void overwriteObsoleteFieldValue(Record record, String fieldName, String newValue) {
        final Field fieldToUpdate = record.getField(fieldName);
        record.removeField(fieldName);
        record.setField(fieldName, fieldToUpdate.getType(), newValue);
    }

    private static class XPathValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input) {
            try {
                XPathFactory factory = XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON,
                        XPATH_FACTORY_IMPL,
                        ClassLoader.getSystemClassLoader());
                final XPathEvaluator evaluator = (XPathEvaluator) factory.newXPath();

                String error = null;
                try {
                    evaluator.compile(input);
                } catch (final Exception e) {
                    error = e.toString();
                }

                return new ValidationResult.Builder().input(input).subject(subject).valid(error == null).explanation(error).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Unable to initialize XPath engine due to " + e.toString()).build();
            }
        }
    }
}


