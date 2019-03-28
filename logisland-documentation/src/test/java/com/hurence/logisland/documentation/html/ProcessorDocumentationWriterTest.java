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
package com.hurence.logisland.documentation.html;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.documentation.DocumentationWriter;
import com.hurence.logisland.documentation.example.FullyDocumentedProcessor;
import com.hurence.logisland.documentation.example.NakedProcessor;
import com.hurence.logisland.documentation.rst.RstDocumentationWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProcessorDocumentationWriterTest {

    @Test
    public void testFullyDocumentedProcessor() throws IOException {
        FullyDocumentedProcessor processor = new FullyDocumentedProcessor();

        DocumentationWriter writer = new RstDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos);

        String results = new String(baos.toByteArray());

        FullyDocumentedProcessor.class.getAnnotation(CapabilityDescription.class);
    }

    @Test
    public void testNakedProcessor() throws IOException {
        NakedProcessor processor = new NakedProcessor();

        DocumentationWriter writer = new RstDocumentationWriter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        writer.write(processor, baos);

        String results = new String(baos.toByteArray());

        // no description
//        assertContains(results, "No description provided.");

        // no tags
//        assertContains(results, "None.");

        // properties
//        assertContains(results, "This component has no required or optional properties.");


    }

}
