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
package com.hurence.connect.spooldir.modele;

import com.hurence.logisland.connect.spooldir.modele.DelayedFile;
import com.hurence.logisland.utils.LinkedHashSetBlockingQueue;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

public class DelayedFileTest {

    /**
     * test ordered by delai
     */
    @Test
    public void testOrderedbyDelay() {
        final DelayQueue<DelayedFile> errorFileQueue = new DelayQueue<>();
        DelayedFile a = new DelayedFile(new File("a"), 100, TimeUnit.MILLISECONDS);
        DelayedFile b = new DelayedFile(new File("b"), 200, TimeUnit.MILLISECONDS);
        DelayedFile c = new DelayedFile(new File("c"), 300, TimeUnit.MILLISECONDS);
        DelayedFile d = new DelayedFile(new File("c"), 400, TimeUnit.MILLISECONDS);
        errorFileQueue.add(a);
        errorFileQueue.add(c);
        errorFileQueue.add(b);
        errorFileQueue.add(d);

        Assert.assertNull(errorFileQueue.poll());
        Assert.assertNull(errorFileQueue.poll());
        while (!errorFileQueue.isEmpty()) {
            DelayedFile currentFile = errorFileQueue.poll();
            if (currentFile != null) {
                switch (errorFileQueue.size()) {
                    case 0:
                        Assert.assertEquals(currentFile, d);
                        break;
                    case 1:
                        Assert.assertEquals(currentFile, c);
                        break;
                    case 2:
                        Assert.assertEquals(currentFile, b);
                        break;
                    case 3:
                        Assert.assertEquals(currentFile, a);
                        break;
                    default:
                        Assert.fail("uncorrect size");
                }
            }
        }
    }
}
