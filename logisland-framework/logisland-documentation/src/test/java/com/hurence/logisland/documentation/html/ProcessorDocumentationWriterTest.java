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
package com.hurence.logisland.documentation.html;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.documentation.DocumentationWriter;
import com.hurence.logisland.documentation.example.FullyDocumentedProcessor;
import com.hurence.logisland.documentation.example.NakedProcessor;
import com.hurence.logisland.documentation.init.ProcessorInitializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.hurence.logisland.documentation.html.XmlValidator.assertContains;
import static com.hurence.logisland.documentation.html.XmlValidator.assertNotContains;

public class ProcessorDocumentationWriterTest {

    @Test
    public void testFullyDocumentedProcessor() throws IOException {
        FullyDocumentedProcessor processor = new FullyDocumentedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer();
        initializer.initialize(processor);

        DocumentationWriter writer = new HtmlProcessorDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos);
        initializer.teardown(processor);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

        assertContains(results, FullyDocumentedProcessor.DIRECTORY.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.DIRECTORY.getDescription());
        assertContains(results, FullyDocumentedProcessor.OPTIONAL_PROPERTY.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.OPTIONAL_PROPERTY.getDescription());
        assertContains(results, FullyDocumentedProcessor.RECURSE.getDisplayName());
        assertContains(results, FullyDocumentedProcessor.RECURSE.getDescription());


        assertNotContains(results, "iconSecure.png");
        assertContains(results, FullyDocumentedProcessor.class.getAnnotation(CapabilityDescription.class)
                .value());
        assertNotContains(results, "This component has no required or optional properties.");
        assertNotContains(results, "No description provided.");
        assertNotContains(results, "No Tags provided.");
        assertNotContains(results, "Additional Details...");

        // verify the right OnRemoved and OnShutdown methods were called
        Assert.assertEquals(0, processor.getOnRemovedArgs());
        Assert.assertEquals(0, processor.getOnRemovedNoArgs());

        Assert.assertEquals(1, processor.getOnShutdownArgs());
        Assert.assertEquals(1, processor.getOnShutdownNoArgs());
    }

    @Test
    public void testNakedProcessor() throws IOException {
        NakedProcessor processor = new NakedProcessor();
        ProcessorInitializer initializer = new ProcessorInitializer();
        initializer.initialize(processor);

        DocumentationWriter writer = new HtmlProcessorDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos);
        initializer.teardown(processor);

        String results = new String(baos.toByteArray());
        XmlValidator.assertXmlValid(results);

        // no description
        assertContains(results, "No description provided.");

        // no tags
        assertContains(results, "None.");

        // properties
        assertContains(results, "This component has no required or optional properties.");


    }

}
