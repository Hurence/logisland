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
package com.hurence.logisland.documentation;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class DocGeneratorTest {


    @Test
    @Ignore
    public void testProcessor() throws IOException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        /*ExtensionManager.discoverExtensions(NarClassLoaders.getInstance().getExtensionClassLoaders());*/

        DocGeneratorUtils.generate(new File("docs"), "components", "rst", false);

        File processorDirectory = new File(temporaryFolder.getRoot(), "com.hurence.logisland.processors.WriteResourceToStream");
        File indexHtml = new File(processorDirectory, "index.html");
        Assert.assertTrue(indexHtml + " should have been generated", indexHtml.exists());
        String generatedHtml = FileUtils.readFileToString(indexHtml);
        Assert.assertNotNull(generatedHtml);
        Assert.assertTrue(generatedHtml.contains("This example processor loads a resource from the nar and writes it to the Record content"));
        Assert.assertTrue(generatedHtml.contains("files that were successfully processed"));
        Assert.assertTrue(generatedHtml.contains("files that were not successfully processed"));
        Assert.assertTrue(generatedHtml.contains("resources"));
    }


}
