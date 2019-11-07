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
 */
package com.hurence.logisland.cv.processor;


import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.hurence.logisland.cv.utils.CVUtils.toBI;
import static com.hurence.logisland.cv.utils.CVUtils.toBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class RunScriptTest {

    private static final Logger logger = LoggerFactory.getLogger(RunScriptTest.class);


    private static final String SCRIPT_FILE = "scripts/opencv.clj";
    public static final String LOGISLAND_NS = "com.hurence.logisland";

    enum ImageFile {

        CAT("img/cat/cat.jpg"),
        BLURED_CAT("img/cat/blured-cat.jpg"),
        DOUBLE_CAT("img/cat/double-cat.jpg"),
        CONTOUR_CAT("img/cat/contour-cat.jpg"),
        GRAY_CAT("img/cat/gray-cat.jpg"),
        HIGHLIGHT_CAT("img/cat/highlight-cat.jpg"),
        SEPIA_CAT("img/cat/sepia-cat.jpg"),
        THRESHOLD_CAT("img/cat/threshold-cat.jpg");

        private String path;
        private BufferedImage image;

        ImageFile(String path) {
            this.path = path;
            try {
                ClassLoader classLoader = RunScriptTest.class.getClassLoader();
                InputStream inputStream = classLoader.getResourceAsStream(path);
                if (inputStream != null) {
                    image = ImageIO.read(inputStream);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public String getPath() {
            return path;
        }

        public int getType() {
            return image.getType();
        }

        public int getHeight() {
            return image.getHeight();
        }

        public int getWidth() {
            return image.getWidth();
        }

        public BufferedImage getImage() {
            return image;
        }

        public byte[] getBytes() {
            try {
                return toBytes(image,"jpg");
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

    }

    private String getFileContentFromResources(String fileName) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        assert resource != null;
        return IOUtils.toString(resource, StandardCharsets.UTF_8);
    }

    private Record getImageRecord(ImageFile image) {
        return new StandardRecord(RecordDictionary.IMAGE)
                .setBytesField(FieldDictionary.RECORD_VALUE, image.getBytes())
                .setIntField(FieldDictionary.IMAGE_HEIGHT, image.getHeight())
                .setIntField(FieldDictionary.IMAGE_WIDTH, image.getWidth())
                .setIntField(FieldDictionary.IMAGE_TYPE, image.getType());
    }

    private Processor processor = new RunScript();

    @Test
    public void testBlur() throws IOException {

        Record inputImage = getImageRecord(ImageFile.CAT);

        TestRunner testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(RunScript.SCRIPT_NS, LOGISLAND_NS);
        testRunner.setProperty(RunScript.SCRIPT_FUNCTION, "ld_blur");
        testRunner.setProperty(RunScript.SCRIPT_CODE, getFileContentFromResources(SCRIPT_FILE));
        testRunner.assertValid();
        testRunner.enqueue(inputImage);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);

        out.assertFieldEquals(FieldDictionary.IMAGE_HEIGHT, ImageFile.BLURED_CAT.getHeight());
        out.assertFieldEquals(FieldDictionary.IMAGE_WIDTH, ImageFile.BLURED_CAT.getWidth());
        out.assertFieldEquals(FieldDictionary.IMAGE_TYPE, ImageFile.BLURED_CAT.getType());


        byte[] a1 = Arrays.copyOfRange(out.getField(FieldDictionary.RECORD_VALUE).asBytes(), 0, 600);
        byte[] a2 = Arrays.copyOfRange(ImageFile.BLURED_CAT.getBytes(), 0, 600);

        assertArrayEquals(a1, a2);
    }

    @Test
    public void testRotation() throws IOException {

        Record inputImage = getImageRecord(ImageFile.CAT);

        TestRunner testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(RunScript.SCRIPT_NS, LOGISLAND_NS);
        testRunner.setProperty(RunScript.SCRIPT_FUNCTION, "ld_rotate_concat");
        testRunner.setProperty(RunScript.SCRIPT_CODE, getFileContentFromResources(SCRIPT_FILE));
        testRunner.assertValid();
        testRunner.enqueue(inputImage);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);

        out.assertFieldEquals(FieldDictionary.IMAGE_HEIGHT, ImageFile.DOUBLE_CAT.getHeight());
        out.assertFieldEquals(FieldDictionary.IMAGE_WIDTH, ImageFile.DOUBLE_CAT.getWidth());
        out.assertFieldEquals(FieldDictionary.IMAGE_TYPE, ImageFile.DOUBLE_CAT.getType());


        byte[] a1 = Arrays.copyOfRange(out.getField(FieldDictionary.RECORD_VALUE).asBytes(), 0, 600);
        byte[] a2 = Arrays.copyOfRange(ImageFile.DOUBLE_CAT.getBytes(), 0, 600);

        assertArrayEquals(a1, a2);

    }

    @Test
    public void testOutputMode() throws IOException {

        Record inputImage = getImageRecord(ImageFile.CAT);

        TestRunner testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(RunScript.SCRIPT_NS, LOGISLAND_NS);
        testRunner.setProperty(RunScript.SCRIPT_FUNCTION, "ld_reduce_in_gray");
        testRunner.setProperty(RunScript.SCRIPT_CODE, getFileContentFromResources(SCRIPT_FILE));
        testRunner.setProperty(RunScript.OUTPUT_MODE, RunScript.OVERWRITE);
        testRunner.assertValid();
        testRunner.enqueue(inputImage);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(1);

        MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertRecordSizeEquals(4);

        out.assertFieldEquals(FieldDictionary.IMAGE_HEIGHT, ImageFile.GRAY_CAT.getHeight());
        out.assertFieldEquals(FieldDictionary.IMAGE_WIDTH, ImageFile.GRAY_CAT.getWidth());
        out.assertFieldEquals(FieldDictionary.IMAGE_TYPE, ImageFile.GRAY_CAT.getType());


        byte[] a1 = Arrays.copyOfRange(out.getField(FieldDictionary.RECORD_VALUE).asBytes(), 0, 600);
        byte[] a2 = Arrays.copyOfRange(ImageFile.GRAY_CAT.getBytes(), 0, 600);

        assertArrayEquals(a1, a2);


        testRunner.setProperty(RunScript.OUTPUT_MODE, RunScript.APPEND);
        testRunner.assertValid();
        testRunner.clearQueues();
        testRunner.enqueue(inputImage);
        testRunner.run();
        testRunner.assertAllInputRecordsProcessed();
        testRunner.assertOutputRecordsCount(2);


    }

}
