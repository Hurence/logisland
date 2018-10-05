/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
package com.hurence.logisland.documentation.rst;

import com.hurence.logisland.annotation.behavior.DynamicProperties;
import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.classloading.PluginClassLoader;
import com.hurence.logisland.classloading.PluginClassloaderBuilder;
import com.hurence.logisland.classloading.PluginLoader;
import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.documentation.DocumentationWriter;

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
public class RstDocumentationWriter implements DocumentationWriter {

    /**
     * The filename where additional user specified information may be stored.
     */
    public static final String ADDITIONAL_DETAILS_RST = "additionalDetails.rst";

    @Override
    public void write(final ConfigurableComponent configurableComponent, final OutputStream streamToWriteTo) throws IOException {

        try {
            final RstPrintWriter rstWriter = new RstPrintWriter(streamToWriteTo, true);

            rstWriter.writeTransition();
            rstWriter.writeInternalReference(PluginProxy.unwrap(configurableComponent).getClass().getCanonicalName());
            writeDescription(configurableComponent, rstWriter);
            writeTags(configurableComponent, rstWriter);
            writeProperties(configurableComponent, rstWriter);
            writeDynamicProperties(configurableComponent, rstWriter);
            writeAdditionalBodyInfo(configurableComponent, rstWriter);
            writeSeeAlso(configurableComponent, rstWriter);

            rstWriter.close();
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
        return PluginProxy.unwrap(configurableComponent).getClass().getSimpleName();
    }


    /**
     * Writes the list of components that may be linked from this component.
     *
     * @param configurableComponent the component to describe
     * @param rstWriter             the stream writer to use
     */
    private void writeSeeAlso(ConfigurableComponent configurableComponent, RstPrintWriter rstWriter){
        final SeeAlso seeAlso = configurableComponent.getClass().getAnnotation(SeeAlso.class);
        if (seeAlso != null) {
            rstWriter.writeSectionTitle(3, "See Also:");
            int index = 0;
            for (final Class<? extends ConfigurableComponent> linkedComponent : seeAlso.value()) {
                if (index != 0) {
                    rstWriter.print(", ");
                }

                writeLinkForComponent(rstWriter, linkedComponent);

                ++index;
            }
            rstWriter.println();
        }
    }

    /**
     * This method may be overridden by sub classes to write additional
     * information to the body of the documentation.
     *
     * @param configurableComponent the component to describe
     * @param rstWriter             the stream writer
     * @throws XMLStreamException thrown if there was a problem writing to the
     *                            XML stream
     */
    protected void writeAdditionalBodyInfo(final ConfigurableComponent configurableComponent,
                                           final RstPrintWriter rstWriter) throws XMLStreamException {

    }

    private void writeTags(final ConfigurableComponent configurableComponent,
                           final RstPrintWriter rstWriter) throws XMLStreamException {
        final Tags tags = configurableComponent.getClass().getAnnotation(Tags.class);
        rstWriter.writeSectionTitle(3, "Tags");
        if (tags != null) {
            final String tagString = join(tags.value(), ", ");
            rstWriter.println(tagString);
        } else {
            rstWriter.println("None.");
        }
    }

    static String join(final String[] toJoin, final String delimiter) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < toJoin.length; i++) {
            sb.append(toJoin[i]);
            if (i < toJoin.length - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    /**
     * Writes a description of the configurable component.
     *
     * @param configurableComponent the component to describe
     * @param rstWriter             the stream writer
     */
    protected void writeDescription(final ConfigurableComponent configurableComponent,
                                    final RstPrintWriter rstWriter) {
        rstWriter.writeSectionTitle(2, getTitle(configurableComponent));
        rstWriter.println(getDescription(configurableComponent));

        PluginClassLoader cl = (PluginClassLoader)PluginLoader.getRegistry().get(PluginProxy.unwrap(configurableComponent).getClass().getCanonicalName());
        if (cl != null) {
            rstWriter.writeSectionTitle(3, "Module");
            rstWriter.println(cl.getModuleInfo().getArtifact());
        }

        rstWriter.writeSectionTitle(3, "Class");
        rstWriter.println(PluginProxy.unwrap(configurableComponent).getClass().getCanonicalName());
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
     * @param rstWriter             the stream writer
     */
    protected void writeProperties(final ConfigurableComponent configurableComponent,
                                   final RstPrintWriter rstWriter) {

        final List<PropertyDescriptor> properties = configurableComponent.getPropertyDescriptors();
        rstWriter.writeSectionTitle(3, "Properties");

        if (properties.size() > 0) {
            final boolean containsExpressionLanguage = containsExpressionLanguage(configurableComponent);
            final boolean containsSensitiveProperties = containsSensitiveProperties(configurableComponent);
            rstWriter.print("In the list below, the names of required properties appear in ");
            rstWriter.printStrong("bold");
            rstWriter.println(". Any other properties (not in bold) are considered optional. " +
                    "The table also indicates any default values");
            if (containsExpressionLanguage) {
                if (!containsSensitiveProperties) {
                    rstWriter.print(", and ");
                } else {
                    rstWriter.print(", ");
                }
                rstWriter.print("whether a property supports the ");
                rstWriter.writeLink("Expression Language", "expression-language.html");
            }
            if (containsSensitiveProperties) {
                rstWriter.print(", and whether a property is considered " + "\"sensitive\", meaning that its value will be encrypted. Before entering a "
                        + "value in a sensitive property, ensure that the ");

                rstWriter.printStrong("logisland.properties");
                rstWriter.print(" file has " + "an entry for the property ");
                rstWriter.printStrong("logisland.sensitive.props.key");
            }
            rstWriter.println(".");

            rstWriter.printCsvTable("allowable-values",
                    new String[]{"Name", "Description","Allowable Values" , "Default Value", "Sensitive", "EL"},
                    new int[]{20, 60, 30, 20, 10, 10});


            // write the individual properties
            for (PropertyDescriptor property : properties) {

                rstWriter.print("   \"");
                if (property.isRequired()) {
                    rstWriter.printStrong(property.getDisplayName());
                } else {
                    rstWriter.print(property.getDisplayName());
                }
                rstWriter.print("\", ");

                rstWriter.print("\"");
                if (property.getDescription() != null && property.getDescription().trim().length() > 0) {
                    rstWriter.print(property.getDescription());
                } else {
                    rstWriter.print("No Description Provided.");
                }
                rstWriter.print("\", ");

                rstWriter.print("\"");
                writeValidValues(rstWriter, property);
                rstWriter.print("\", ");



                rstWriter.print("\"");
                rstWriter.print(property.getDefaultValue());
                rstWriter.print("\", ");



                rstWriter.print("\"");
                if (property.isSensitive()) {
                    rstWriter.printStrong( "true");
                }else {
                    rstWriter.print("");
                }
                rstWriter.print("\", ");



                rstWriter.print("\"");
                if (property.isExpressionLanguageSupported()) {
                    rstWriter.printStrong( "true");
                }else {
                    rstWriter.print("");
                }


                rstWriter.println("\"");

            }


        } else {
            rstWriter.println("This component has no required or optional properties.");
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
                                        final RstPrintWriter rstWriter) {

        final List<DynamicProperty> dynamicProperties = getDynamicProperties(configurableComponent);

        if (dynamicProperties != null && dynamicProperties.size() > 0) {
            rstWriter.writeSectionTitle(3, "Dynamic Properties");
            rstWriter.println("Dynamic Properties allow the user to specify both the name and value of a property.");
            rstWriter.printCsvTable("dynamic-properties",
                    new String[]{"Name", "Value", "Description", "EL"},
                    new int[]{20, 20, 40, 10});

            for (final DynamicProperty dynamicProperty : dynamicProperties) {

                rstWriter.print("   \"");
                rstWriter.print(dynamicProperty.name());
                rstWriter.print("\", ");

                rstWriter.print("\"");
                rstWriter.print(dynamicProperty.value());
                rstWriter.print("\", ");

                rstWriter.print("\"");
                rstWriter.print(dynamicProperty.description());
                rstWriter.print("\", ");

                if (dynamicProperty.supportsExpressionLanguage()) {
                    rstWriter.printStrong("true");
                } else
                    rstWriter.print("\"\"");
                rstWriter.println();
            }

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

    private void writeValidValueDescription(RstPrintWriter rstWriter, String description) {

        rstWriter.print(description);

     //   rstWriter.writeImage("_static/iconInfo.png", description, null, null, null, null);

    }

    /**
     * Interrogates a PropertyDescriptor to get a list of AllowableValues, if
     * there are none, nothing is written to the stream.
     *
     * @param rstWriter the stream writer to use
     * @param property  the property to describe
     * @throws XMLStreamException thrown if there was a problem writing to the
     *                            XML Stream
     */
    protected void writeValidValues(RstPrintWriter rstWriter, PropertyDescriptor property){
        if (property.getAllowableValues() != null && property.getAllowableValues().size() > 0) {

            boolean first = true;
            for (AllowableValue value : property.getAllowableValues()) {
                if (!first) {
                    rstWriter.print(", ");
                } else {
                    first = false;
                }
                rstWriter.print(value.getDisplayName());
                if (value.getDescription() != null) {
                    writeValidValueDescription(rstWriter, " (" + value.getDescription() + ")");
                }
            }
        }
    }


    /**
     * Writes a link to another configurable component
     *
     * @param rstWriter the xml stream writer
     * @param clazz     the configurable component to link to
     */
    protected void writeLinkForComponent(final RstPrintWriter rstWriter, final Class<?> clazz) {
        rstWriter.writeInternalReferenceLink(clazz.getCanonicalName());
    }

}
