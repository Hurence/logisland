/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
 */
package com.hurence.logisland.documentation.json;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.hurence.logisland.annotation.behavior.DynamicProperties;
import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.documentation.DocumentationWriter;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.stream.RecordStream;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;


/**
 * Generates reStructuredText documentation for a ConfigurableComponent.
 * <p>
 * http://docutils.sourceforge.net/docs/ref/rst/restructuredtext.html
 * http://docutils.sourceforge.net/docs/ref/rst/directives.html
 */
public class JsonDocumentationWriter implements DocumentationWriter {

    /**
     * The filename where additional user specified information may be stored.
     */
    public static final String ADDITIONAL_DETAILS_RST = "additionalDetails.rst";

    @Override
    public void write(final ConfigurableComponent configurableComponent, final OutputStream streamToWriteTo) throws IOException {

        try {

            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator(streamToWriteTo, JsonEncoding.UTF8);


            generator.writeStartObject();

            writeDescription(configurableComponent, generator);
            writeTags(configurableComponent, generator);
            writeProperties(configurableComponent, generator);
            writeDynamicProperties(configurableComponent, generator);
            writeAdditionalBodyInfo(configurableComponent, generator);
            writeSeeAlso(configurableComponent, generator);

            generator.writeEndObject();

            generator.close();
        } catch (XMLStreamException | FactoryConfigurationError e) {
            throw new IOException("Unable to create XMLOutputStream", e);
        }


    }


    /**
     * Gets the class name of the component.
     *
     * @param configurableComponent the component to describe
     * @return the class name of the component
     */
    protected String getTitle(final ConfigurableComponent configurableComponent) {
        return configurableComponent.getClass().getSimpleName();
    }


    /**
     * Writes the list of components that may be linked from this component.
     *
     * @param configurableComponent the component to describe
     */
    private void writeSeeAlso(ConfigurableComponent configurableComponent, JsonGenerator generator) throws IOException {
        final SeeAlso seeAlso = configurableComponent.getClass().getAnnotation(SeeAlso.class);
        if (seeAlso != null) {
            generator.writeArrayFieldStart("seeAlso");

            int index = 0;
            for (final Class<? extends ConfigurableComponent> linkedComponent : seeAlso.value()) {
                generator.writeStartObject();
                generator.writeString(linkedComponent.getCanonicalName());
                generator.writeEndObject();
                ++index;
            }
            generator.writeEndArray();
        }
    }

    /**
     * This method may be overridden by sub classes to write additional
     * information to the body of the documentation.
     *
     * @param configurableComponent the component to describe
     * @throws XMLStreamException thrown if there was a problem writing to the
     *                            XML stream
     */
    protected void writeAdditionalBodyInfo(final ConfigurableComponent configurableComponent,
                                           final JsonGenerator generator) throws XMLStreamException {

    }

    private void writeTags(final ConfigurableComponent configurableComponent,
                           final JsonGenerator generator) throws XMLStreamException, IOException {
        final Tags tags = configurableComponent.getClass().getAnnotation(Tags.class);
        if (tags != null) {
            generator.writeArrayFieldStart("tags");
            for (String tag:tags.value()       ) {
                generator.writeString(tag);
            }
            generator.writeEndArray();
        }
    }


    /**
     * Writes a description of the configurable component.
     *
     */
    protected void writeDescription(final ConfigurableComponent configurableComponent,
                                    final JsonGenerator generator) throws IOException {

        generator.writeStringField("name", getTitle(configurableComponent));
        generator.writeStringField("description", getDescription(configurableComponent));
        generator.writeStringField("component", configurableComponent.getClass().getCanonicalName());
        if(Processor.class.isAssignableFrom(configurableComponent.getClass())){
            generator.writeStringField("type", "processor");
        }else if (ProcessingEngine.class.isAssignableFrom(configurableComponent.getClass())){
            generator.writeStringField("type", "engine");
        }else if (RecordStream.class.isAssignableFrom(configurableComponent.getClass())){
            generator.writeStringField("type", "stream");
        }
    }


    /**
     * Gets a description of the ConfigurableComponent using the
     * CapabilityDescription annotation.
     *
     * @param configurableComponent the component to describe
     * @return a description of the configurableComponent
     */
    protected String getDescription(final ConfigurableComponent configurableComponent) {
        final CapabilityDescription capabilityDescription = configurableComponent.getClass().getAnnotation(
                CapabilityDescription.class);

        final String description;
        if (capabilityDescription != null) {
            description = capabilityDescription.value();
        } else {
            description = "No description provided.";
        }

        return description;
    }

    /**
     * Writes the PropertyDescriptors out as a table.
     *
     * @param configurableComponent the component to describe

     */
    protected void writeProperties(final ConfigurableComponent configurableComponent,
                                   final JsonGenerator generator) throws IOException {

        final List<PropertyDescriptor> properties = configurableComponent.getPropertyDescriptors();


        if (properties.size() > 0) {
            generator.writeArrayFieldStart("properties");


            // write the individual properties
            for (PropertyDescriptor property : properties) {

                generator.writeStartObject();
                generator.writeStringField("name", property.getName());

               /* if (property.getValidators() != null && property.getValidators().size() >0 ){

                    generator.writeArrayFieldStart("validators");

                    property.getValidators().forEach(validator -> {
                        try {
                            generator.writeString(validator.getClass().);
                        } catch (IOException e) {
                           // do nothing
                        }
                    });

                    generator.writeEndArray();

                }*/

                generator.writeBooleanField("isRequired", property.isRequired());
                if (property.getDescription() != null && property.getDescription().trim().length() > 0) {
                    generator.writeStringField("description", property.getDescription());
                } else {
                    generator.writeStringField("description", "No Description Provided.");
                }

                writeValidValues(generator, property);
                generator.writeStringField("defaultValue", property.getDefaultValue());
                generator.writeBooleanField("isDynamic", property.isDynamic());
                generator.writeBooleanField("isSensitive", property.isSensitive());
                generator.writeBooleanField("isExpressionLanguageSupported", property.isExpressionLanguageSupported());
                generator.writeEndObject();
            }

            generator.writeEndArray();

        }
    }

    /**
     * Indicates whether or not the component contains at least one sensitive property.
     *
     * @param component the component to interogate
     * @return whether or not the component contains at least one sensitive property.
     */
    private boolean containsSensitiveProperties(final ConfigurableComponent component) {
        for (PropertyDescriptor descriptor : component.getPropertyDescriptors()) {
            if (descriptor.isSensitive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Indicates whether or not the component contains at least one property that supports Expression Language.
     *
     * @param component the component to interogate
     * @return whether or not the component contains at least one sensitive property.
     */
    private boolean containsExpressionLanguage(final ConfigurableComponent component) {
        for (PropertyDescriptor descriptor : component.getPropertyDescriptors()) {
            if (descriptor.isExpressionLanguageSupported()) {
                return true;
            }
        }
        return false;
    }

    private void writeDynamicProperties(final ConfigurableComponent configurableComponent,
                                        final JsonGenerator generator) throws IOException {

        final List<DynamicProperty> dynamicProperties = getDynamicProperties(configurableComponent);

        if (dynamicProperties != null && dynamicProperties.size() > 0) {

            generator.writeArrayFieldStart("dynamicProperties");

            for (final DynamicProperty dynamicProperty : dynamicProperties) {
                generator.writeStartObject();
                generator.writeStringField("name", dynamicProperty.name());
                generator.writeStringField("value", dynamicProperty.value());
                generator.writeStringField("description", dynamicProperty.description());
                generator.writeBooleanField("isExpressionLanguageSupported", dynamicProperty.supportsExpressionLanguage());
               generator.writeEndObject();
            }
            generator.writeEndArray();

        }
    }

    private List<DynamicProperty> getDynamicProperties(ConfigurableComponent configurableComponent) {
        final List<DynamicProperty> dynamicProperties = new ArrayList<>();
        final DynamicProperties dynProps = configurableComponent.getClass().getAnnotation(DynamicProperties.class);
        if (dynProps != null) {
            for (final DynamicProperty dynProp : dynProps.value()) {
                dynamicProperties.add(dynProp);
            }
        }

        final DynamicProperty dynProp = configurableComponent.getClass().getAnnotation(DynamicProperty.class);
        if (dynProp != null) {
            dynamicProperties.add(dynProp);
        }

        return dynamicProperties;
    }

    private void writeValidValueDescription(JsonGenerator generator, String description) throws IOException {

        generator.writeStringField("description", description);

        //   rstWriter.writeImage("_static/iconInfo.png", description, null, null, null, null);

    }

    /**
     * Interrogates a PropertyDescriptor to get a list of AllowableValues, if
     * there are none, nothing is written to the stream.
     *
     * @param property  the property to describe
     * @throws XMLStreamException thrown if there was a problem writing to the
     *                            XML Stream
     */
    protected void writeValidValues(JsonGenerator generator, PropertyDescriptor property) throws IOException {
        if (property.getAllowableValues() != null && property.getAllowableValues().size() > 0) {

            for (AllowableValue value : property.getAllowableValues()) {
                generator.writeStringField(value.getDisplayName(), value.getDescription());
            }
        }
    }



}
