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
package com.hurence.logisland.documentation.example;

import java.util.*;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.behavior.ReadsAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttribute;
import com.hurence.logisland.annotation.behavior.WritesAttributes;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnRemoved;
import com.hurence.logisland.annotation.lifecycle.OnShutdown;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

@Tags({"one", "two", "three"})
@CapabilityDescription("This is a processor that is used to test documentation.\n" +
        "It contains several lines so we verify it works\n" +
        "Below a list :\n" +
        "* i am the first element\n" +
        "* i am the second element\n" +
        "\n" +
        "\tI should also support tabulation")
@WritesAttributes({
    @WritesAttribute(attribute = "first", description = "this is the first attribute i write"),
    @WritesAttribute(attribute = "second")})
@ReadsAttribute(attribute = "incoming", description = "this specifies the format of the thing")
@SeeAlso(value = {NakedProcessor.class}, classNames = {"com.hurence.logisland.processor.ExampleProcessor"})
@DynamicProperty(name = "Relationship Name", supportsExpressionLanguage = true, value = "some XPath", description = "Routes Records to relationships based on XPath")
public class FullyDocumentedProcessor extends AbstractProcessor {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder().name("Input Directory")
            .description("The input directory from which to pull files").required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(true).build();

    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder().name("Recurse Subdirectories")
            .description("Indicates whether or not to pull files from subdirectories").required(true)
            .allowableValues(
                    new AllowableValue("true", "true", "Should pull from sub directories"),
                    new AllowableValue("false", "false", "Should not pull from sub directories")
            ).defaultValue("true").build();

    public static final PropertyDescriptor OPTIONAL_PROPERTY = new PropertyDescriptor.Builder()
            .name("Optional Property").description("This is a property you can use or not").required(false).build();

    public static final PropertyDescriptor TYPE_PROPERTY = new PropertyDescriptor.Builder()
            .name("Type")
            .description("This is the type of something that you can choose.  It has several possible values")
            .allowableValues("yes", "no", "maybe", "possibly", "not likely", "longer option name")
            .required(true).build();


    private List<PropertyDescriptor> properties;

    private int onRemovedNoArgs = 0;
    private int onRemovedArgs = 0;

    private int onShutdownNoArgs = 0;
    private int onShutdownArgs = 0;

    @Override
    public void init(final ProcessContext context){
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DIRECTORY);
        properties.add(RECURSE);
        properties.add(OPTIONAL_PROPERTY);
        properties.add(TYPE_PROPERTY);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }



    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        return null;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName)
                .description("This is a property you can use or not").dynamic(true).build();
    }

    @OnRemoved
    public void onRemovedNoArgs() {
        onRemovedNoArgs++;
    }

    @OnRemoved
    public void onRemovedArgs(ProcessContext context) {
        onRemovedArgs++;
    }

    @OnShutdown
    public void onShutdownNoArgs() {
        onShutdownNoArgs++;
    }

    @OnShutdown
    public void onShutdownArgs(ProcessContext context) {
        onShutdownArgs++;
    }

    public int getOnRemovedNoArgs() {
        return onRemovedNoArgs;
    }

    public int getOnRemovedArgs() {
        return onRemovedArgs;
    }

    public int getOnShutdownNoArgs() {
        return onShutdownNoArgs;
    }

    public int getOnShutdownArgs() {
        return onShutdownArgs;
    }
}
