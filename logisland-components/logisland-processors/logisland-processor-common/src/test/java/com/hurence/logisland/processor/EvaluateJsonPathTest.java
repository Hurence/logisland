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
package com.hurence.logisland.processor;


import com.hurence.logisland.util.string.StringUtils;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import com.jayway.jsonpath.InvalidPathException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class EvaluateJsonPathTest {

    private static Logger logger = LoggerFactory.getLogger(EvaluateJsonPathTest.class);
    private static final String JSON_SNIPPET = "json/json-sample.json";
    private static final String XML_SNIPPET = "xml/xml-snippet.xml";


    /**
     * load logs from ressource as a string or return null if an error occurs
     */
    public static String loadFileContentAsString(String path, String encoding) {
        try {
            final URL url = EvaluateJsonPathTest.class.getClassLoader().getResource(path);
            assert url != null;
            byte[] encoded = Files.readAllBytes(Paths.get(new File(url.toURI()).getAbsolutePath()));
            return new String(encoded, encoding);
        } catch (Exception e) {
            logger.error(String.format("Could not load log file %s and convert to string", path), e);
            return null;
        }
    }


    @Test(expected = AssertionError.class)
    public void testInvalidJsonPath() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_SCALAR);
        testRunner.setProperty("invalid.jsonPath", "$..");

        Assert.fail("An improper JsonPath expression was not detected as being invalid.");
    }

    @Test
    public void testInvalidJsonDocument() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_SCALAR);

        testRunner.enqueue(null, loadFileContentAsString(XML_SNIPPET, "UTF-8"));
        testRunner.run();

        testRunner.assertOutputErrorCount(1);
    }

    @Test(expected = AssertionError.class)
    public void testInvalidConfiguration_destinationContent_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.assertValid();
        testRunner.setProperty("JsonPath1", "$[0]._id");
        testRunner.setProperty("JsonPath2", "$[0].name");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();

        Assert.fail("Processor incorrectly ran with an invalid configuration of multiple paths specified as attributes for a destination of content.");
    }

    @Test(expected = InvalidPathException.class)
    public void testInvalidConfiguration_invalidJsonPath_space() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());

        testRunner.setProperty("JsonPath1", "$[0]. _id");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();

        Assert.fail("Processor incorrectly ran with an invalid configuration of multiple paths specified as attributes for a destination of content.");
    }

    @Test
    public void testConfiguration_destinationAttributes_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty("JsonPath1", "$[0]._id");
        testRunner.setProperty("JsonPath2", "$[0].name");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();
    }

    @Test
    public void testExtractPath_destinationAttribute() throws Exception {
        String jsonPathAttrKey = "JsonPath";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(jsonPathAttrKey, "$[0]._id");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();

        testRunner.assertOutputErrorCount(0);

        final MockRecord out = testRunner.getOutputRecords().get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result", "54df94072d5dbf7dc6340cc5", out.getField(jsonPathAttrKey).asString());
    }

    @Test
    public void testExtractPath_destinationAttributes_twoPaths() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());

        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_JSON);

        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();


        testRunner.assertOutputErrorCount(0);
        final MockRecord out = testRunner.getOutputRecords().get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result for id attribute", "54df94072d5dbf7dc6340cc5", out.getField(jsonPathIdAttrKey).asString());
        Assert.assertEquals("Transferred flow file did not have the correct result for name attribute", "{\"first\":\"Shaffer\",\"last\":\"Pearson\"}", out.getField(jsonPathNameAttrKey).asString());
    }


    @Test
    public void testExtractPath_destinationAttributes_twoPaths_notFound() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());


        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id.nonexistent");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name.nonexistent");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();


        testRunner.assertOutputErrorCount(0);
        final MockRecord out = testRunner.getOutputRecords().get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result for id attribute", "", out.getField(jsonPathIdAttrKey).asString());
        Assert.assertEquals("Transferred flow file did not have the correct result for name attribute", "", out.getField(jsonPathNameAttrKey).asString());
    }

    @Test
    public void testExtractPath_destinationAttributes_twoPaths_oneFound() throws Exception {
        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());


        String jsonPathIdAttrKey = "evaluatejson.id";
        String jsonPathNameAttrKey = "evaluatejson.name";

        testRunner.setProperty(jsonPathIdAttrKey, "$[0]._id");
        testRunner.setProperty(jsonPathNameAttrKey, "$[0].name.nonexistent");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();


        testRunner.assertOutputErrorCount(0);
        final MockRecord out = testRunner.getOutputRecords().get(0);
        Assert.assertEquals("Transferred flow file did not have the correct result for id attribute", "54df94072d5dbf7dc6340cc5", out.getField(jsonPathIdAttrKey).asString());
        Assert.assertEquals("Transferred flow file did not have the correct result for name attribute", StringUtils.EMPTY, out.getField(jsonPathNameAttrKey).asString());
    }


    @Test
    public void testReturnScalar() throws Exception {
        String jsonPathAttrKey1 = "age";
        String jsonPathAttrKey2 = "latitude";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_SCALAR);
        testRunner.setProperty(jsonPathAttrKey1, "$[0].age");
        testRunner.setProperty(jsonPathAttrKey2, "$[0].latitude");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();


        testRunner.assertOutputErrorCount(0);
        final MockRecord out = testRunner.getOutputRecords().get(0);
        out.assertFieldTypeEquals(jsonPathAttrKey1, FieldType.INT);
        out.assertFieldEquals(jsonPathAttrKey1, 20);
        out.assertFieldTypeEquals(jsonPathAttrKey2, FieldType.DOUBLE);
        out.assertFieldEquals(jsonPathAttrKey2, -50.359159d);

    }


    @Test
    public void testRouteFailure_returnTypeScalar_resultArray() throws Exception {
        String jsonPathAttrKey = "friends.indefinite.id.list";

        final TestRunner testRunner = TestRunners.newTestRunner(new EvaluateJsonPath());
        testRunner.setProperty(EvaluateJsonPath.RETURN_TYPE, EvaluateJsonPath.RETURN_TYPE_SCALAR);
        testRunner.setProperty(jsonPathAttrKey, "$[0].friends[?(@.id < 3)].id");

        testRunner.enqueue(null, loadFileContentAsString(JSON_SNIPPET, "UTF-8"));
        testRunner.run();


        testRunner.assertOutputErrorCount(1);
        final MockRecord out = testRunner.getOutputRecords().get(0);

    }


}
