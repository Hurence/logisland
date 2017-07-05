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
package com.hurence.logisland.processor.binetflow;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;


public class TagBinetflowTest {

    private static Logger logger = LoggerFactory.getLogger(TagBinetflowTest.class);

    @Test
    public void TagBinetflowTest() throws IOException, InitializationException {

        final TestRunner runner = TestRunners.newTestRunner(TagBinetflow.class);
        runner.setProperty(TagBinetflow.MODEL_CLIENT_SERVICE, "modelClient");
        runner.setProperty(TagBinetflow.SCALER_MODEL_CLIENT_SERVICE, "scalerModelClient");

        runner.assertValid();

        //////////////////
        // KMEANS MODEL //
        //////////////////

        String modelLibrary = "mllib";
        String modelName = "kmeans";
        String modelFilePath = "src/test/resources/binetflow/savedModels_1499075424624";

        final MockModelClientService modelClient = new MockModelClientService(modelLibrary, modelName, modelFilePath);
        runner.addControllerService("modelClient", modelClient);
        runner.enableControllerService(modelClient);

        ////////////
        // SCALER //
        ////////////

        String scalerModelName = "standard_scaler";
        String scalerModelFilePath = "src/test/resources/binetflow/savedModels_scaler_1499075424624";

        final MockScalerModelClientService scalerModelClient = new MockScalerModelClientService(scalerModelName, scalerModelFilePath);
        runner.addControllerService("scalerModelClient", scalerModelClient);
        runner.enableControllerService(scalerModelClient);

        List<Record> records = new ArrayList<>();
        try {
            FileInputStream in = new FileInputStream("src/test/resources/binetflow/flowRecords");
            ObjectInputStream ois = new ObjectInputStream(in);
            records = (List<Record>) (ois.readObject());
            ois.close();
        } catch (Exception e) {
            System.out.println("Problem serializing: " + e);
        }

        runner.enqueue(records);
        runner.clearQueues();
        runner.run();
        runner.assertAllInputRecordsProcessed();
        runner.assertOutputRecordsCount(18);
        runner.assertOutputErrorCount(0);

        //MockRecord out = runner.getOutputRecords().get(0);

    }

}